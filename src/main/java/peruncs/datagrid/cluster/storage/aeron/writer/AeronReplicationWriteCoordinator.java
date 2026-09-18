package peruncs.datagrid.cluster.storage.aeron.writer;

import org.eclipse.serializer.persistence.binary.types.Binary;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;

/// Keeps local Store acceptance and Aeron publication in one ordered state
/// machine.
///
/// The coordinator owns the pending dictionary and prepared transaction for
/// its write path. It reports state changes to the checkpoint writer so
/// restart can distinguish a committed transaction from an uncertain one.
///
/// Lock order is strict: coordinator {@code writeLock} → publisher state
/// monitor → publisher {@code offerLock}. Slow Aeron offers, Archive waits, and
/// retention callbacks run after releasing {@code writeLock}; only the
/// bounded state transitions reacquire it. No callback may acquire the
/// coordinator lock while holding the publisher offer lock.
///
/// This is an Aeron-only write coordinator. Store integration must use
/// [AeronStorageBinaryReplicationTarget]; exposing this object as the
/// distributor would allow publication without local Store
/// acceptance and would bypass the durable fence.
public final class AeronReplicationWriteCoordinator implements AutoCloseable {
    private final AeronReplicationPublisher publisher;
    private final ReplicationDurabilityMode durabilityMode;
    private final AeronArchiveReplicationPublisher.CheckpointWriter listener;
    private final LongPredicate writeAdmission;
    /* Lease validity is checked before Archive capacity on every admission, so
     * a fenced writer fails with a distinct lease-lost error instead of a
     * misleading capacity-exhaustion message. Terminal-marker offers run
     * through the same gate under interprocess ownership (see WriterLeaseGate)
     * so a steal racing back pressure cannot slip a stale marker into Aeron. */
    private final WriterLeaseGate leaseGate;
    /* The single lock for all coordinator state below. It is reentrant: write
     * admission holds it across a whole Store transaction while the state
     * transitions nest inside. It is released only across the slow Archive
     * acknowledgement wait: commitOrMarkUncertain releases the caller's hold
     * after setting the commit guard, and commit() releases its own hold after
     * re-checking the writer identity. */
    private final ReentrantLock writeLock = new ReentrantLock();
    /* Writer identity pinned at claim time. The publisher seeds its sequence
     * from the checkpoint (recording ID + epoch) at construction; these values
     * refuse a publisher that was swapped or rewound underneath this
     * coordinator. Cross-process fencing stays with the checkpoint epoch and
     * Archive recording ownership enforced when the publisher is built. */
    private final long writerEpoch;
    private final long initialSequence;
    private byte[] pendingDictionary;
    private LocalEnqueue localAcceptanceFence;
    /* The coordinator is single-threaded. Reusing this channel-order scratch
     * array removes the ArrayList and temporary array from every write. A local
     * acceptance fence keeps the count beside the array until preparation ends. */
    private ByteBuffer[] bufferScratch = new ByteBuffer[8];
    private int bufferScratchCount;
    /* Set only after publisher.commit() has returned.  A checkpoint cleanup
     * failure after that point must not overwrite a durable COMMITTED record with
     * COMMITTING_UNCERTAIN. */
    private boolean commitMarkerPublished;
    private boolean commitInProgress;
    /* Signalled whenever commitInProgress is cleared. Shutdown and Archive
     * maintenance wait on it (bounded) instead of failing while a commit that
     * already released the write lock is still awaiting its Archive position. */
    private final Condition commitDone = this.writeLock.newCondition();

    AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher) {
        this(publisher, ReplicationDurabilityMode.ARCHIVE_FIRST,
                (state, sequence, length, chunks, crc, position) -> {
                }, ignored -> true);
    }

    AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher,
                                     final AeronArchiveReplicationPublisher.CheckpointWriter listener) {
        this(publisher, ReplicationDurabilityMode.ARCHIVE_FIRST, listener);
    }

    AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher,
                                     final ReplicationDurabilityMode durabilityMode,
                                     final AeronArchiveReplicationPublisher.CheckpointWriter listener) {
        this(publisher, durabilityMode, listener, ignored -> true);
    }

    AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher,
                                     final ReplicationDurabilityMode durabilityMode,
                                     final AeronArchiveReplicationPublisher.CheckpointWriter listener,
                                     final LongPredicate writeAdmission) {
        this(publisher, durabilityMode, listener, writeAdmission, () -> true);
    }

    AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher,
                                      final ReplicationDurabilityMode durabilityMode,
                                      final AeronArchiveReplicationPublisher.CheckpointWriter listener,
                                      final LongPredicate writeAdmission,
                                      final BooleanSupplier leaseValid) {
        this(publisher, durabilityMode, listener, writeAdmission, WriterLeaseGate.of(leaseValid));
    }

    AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher,
                                      final ReplicationDurabilityMode durabilityMode,
                                      final AeronArchiveReplicationPublisher.CheckpointWriter listener,
                                      final LongPredicate writeAdmission,
                                      final WriterLeaseGate leaseGate) {
        Objects.requireNonNull(publisher, "publisher");
        Objects.requireNonNull(durabilityMode, "durabilityMode");
        Objects.requireNonNull(listener, "listener");
        Objects.requireNonNull(writeAdmission, "writeAdmission");
        Objects.requireNonNull(leaseGate, "leaseGate");
        this.publisher = publisher;
        this.durabilityMode = durabilityMode;
        this.listener = listener;
        this.writeAdmission = writeAdmission;
        this.leaseGate = leaseGate;
        this.publisher.claimCoordinator(this, leaseGate);
        this.writerEpoch = publisher.epoch();
        this.initialSequence = publisher.nextSequence();
    }

    ReplicationDurabilityMode durabilityMode() {
        return this.durabilityMode;
    }

    long nextSequence() {
        return this.publisher.nextSequence();
    }

        /// Fails closed when the publisher no longer carries this writer's identity.
    ///
    /// Call with the write lock held.
    private void ensureWriterIdentity() {
        if (this.publisher.epoch() != this.writerEpoch ||
            this.publisher.nextSequence() < this.initialSequence) {
            this.publisher.failClosed();
            throw new IllegalStateException(
                    "Aeron publisher identity changed underneath its write coordinator");
        }
    }

        /// Saves a type dictionary for the next transaction.
    ///
    /// @param typeDictionaryData dictionary text, or `null` to clear it
    public void distributeTypeDictionary(final String typeDictionaryData) {
        this.writeLock.lock();
        try {
            this.ensureNotCommitting();
            this.ensureWriterIdentity();
            if (typeDictionaryData == null) {
                this.pendingDictionary = null;
            } else {
                final byte[] encoded = typeDictionaryData.getBytes(StandardCharsets.UTF_8);
                if (encoded.length > this.publisher.maxTransactionBytes()) {
                    throw new IllegalArgumentException("type dictionary exceeds maxTransactionBytes");
                }
                this.pendingDictionary = encoded;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

        /// Serializes one complete Store acceptance/publication transaction.
    void executeWriteAtomically(final WriteOperation operation) {
        Objects.requireNonNull(operation, "operation");
        this.writeLock.lock();
        try {
            this.ensureNotCommitting();
            this.ensureWriterIdentity();
            operation.run();
        } finally {
            this.writeLock.unlock();
        }
    }

        /// Publishes and commits one Store binary using the archive-first fence.
    ///
    /// This entry point is for the neutral distributor, which has no local
    /// persistence target to fence. Store writes should use
    /// [AeronStorageBinaryReplicationTarget] so local acceptance and
    /// publication remain one operation.
    ///
    /// @param data binary to publish
    void distributeData(final Binary data) {
        this.executeWriteAtomically(() ->
        {
            try (final AeronReplicationPublisher.PreparedTransaction prepared = this.prepare(data)) {
                this.commitOrMarkUncertain(prepared);
            }
        });
    }

        /// Commits a token. If the result is unclear, records that fact before
    /// rethrowing so restart cannot silently reuse the sequence.
    ///
    /// Must be called with the write lock held (the write-admission path
    /// does). Admission and the commit guard are set under that hold, then the
    /// lock is released for the commit and reacquired before this returns, so
    /// the caller's own unlock still balances exactly.
    void commitOrMarkUncertain(final AeronReplicationPublisher.PreparedTransaction prepared) {
        this.commitMarkerPublished = false;
        /* Set the guard while the caller's lock is still held. Releasing first
         * would let a second writer pass ensureNotCommitting() and prepare
         * concurrently with this commit, leaving two pending transactions
         * against one single-pending publisher. */
        this.ensureNotCommitting();
        this.ensureWriterIdentity();
        this.commitInProgress = true;
        this.writeLock.unlock();
        try {
            this.commit(prepared);
        } catch (final Error failure) {
            /* A fatal JVM error is not a recoverable publication failure.  Do not
             * attempt checkpoint I/O here: the existing PREPARING/ENQUEUED fence is
             * already the fail-closed recovery evidence, and allocating a second
             * marker can mask the original Error. */
            this.writeLock.lock();
            this.commitInProgress = false;
            this.commitDone.signalAll();
            this.publisher.failClosed();
            throw failure;
        } catch (final RuntimeException failure) {
            this.writeLock.lock();
            /* Clear the guard before marking uncertain: commit() clears it on
             * every path inside its own try, but a failure before that (for
             * example an identity change) leaves the guard set, and
             * markCommittingUncertain requires it clear. */
            this.commitInProgress = false;
            this.commitDone.signalAll();
            if (!this.commitMarkerPublished) {
                try {
                    this.markCommittingUncertain(prepared);
                } catch (final RuntimeException uncertainFailure) {
                    failure.addSuppressed(uncertainFailure);
                }
            } else {
                /* The Archive terminal marker is known durable. Keep the checkpoint
                 * state already written by notifyState(COMMITTED); a failed fence
                 * cleanup is a degraded shutdown, not an uncertain commit. */
                failure.addSuppressed(new IllegalStateException(
                        "Aeron commit is durable but its checkpoint cleanup failed"));
            }
            throw failure;
        } finally {
            /* Restore the caller's hold: commit() releases the lock for its slow
             * wait, and the catch paths above reacquire it before rethrowing. */
            if (!this.writeLock.isHeldByCurrentThread()) {
                this.writeLock.lock();
            }
            this.commitMarkerPublished = false;
        }
    }

        /// Publishes a transaction's prepare phase and reports PREPARING to the
    /// checkpoint listener. The writer first records a small PREPARING fence for
    /// archive-first writes, because a local Store can accept data before the
    /// publication has a terminal marker. Enqueue-first writes reuse their existing
    /// ENQUEUED fence. The returned token must be committed or closed; listener
    /// failure fails the publisher closed.
    AeronReplicationPublisher.PreparedTransaction prepare(final Binary data) {
        this.writeLock.lock();
        try {
            return this.prepareLocked(data);
        } finally {
            this.writeLock.unlock();
        }
    }

    private AeronReplicationPublisher.PreparedTransaction prepareLocked(final Binary data) {
        this.ensureNotCommitting();
        this.ensureWriterIdentity();
        Objects.requireNonNull(data, "data");
        if (this.publisher.hasPendingTransaction()) {
            throw new IllegalStateException("an Aeron prepared transaction is already pending");
        }
        final AeronReplicationPublisher.PreparedTransaction prepared;
        final LocalEnqueue local = this.localAcceptanceFence;
        /* PREPARING is reported before the publisher consumes the reusable buffer
         * array. A listener is allowed to run on this thread, so reject re-entrant
         * preparation before it can recollect into that array and corrupt the outer
         * transaction. */
        if (local == null && this.publisher.hasSequenceReservation()) {
            throw new IllegalStateException("cannot re-enter Aeron preparation while a sequence is reserved");
        }
        /* ENQUEUE_THEN_ARCHIVE already collected the channel-ordered buffer array while
         * fencing the local Store write. Reuse that view after the Store
         * restores the marked positions; collecting again would repeat the channel
         * walk and allocate another array for the same transaction. */
        final int bufferCount;
        final ByteBuffer[] buffers;
        if (local == null) {
            bufferCount = this.collectBuffers(data);
            buffers = this.bufferScratch;
        } else {
            if (local.source() != data) {
                throw new IllegalStateException(
                        "the pending Aeron local-acceptance fence belongs to a different Store transaction");
            }
            bufferCount = local.bufferCount();
            buffers = local.buffers();
        }
        final boolean archiveFirst = local == null;
        AeronReplicationPublisher.TransactionMetadata metadata = null;
        long sequence = -1L;
        /* ARCHIVE_FIRST used to publish before it left any durable local
         * evidence. Reserve the sequence and persist a PREPARING fence first so a
         * crash after local Store acceptance cannot silently disappear from the
         * next writer. The enqueue mode already owns an equivalent fence and must
         * not write it twice. */
        if (archiveFirst) {
            try {
                metadata = this.publisher.transactionMetadata(buffers, bufferCount);
            } catch (final RuntimeException | Error failure) {
                this.clearBufferScratch();
                this.publisher.failClosed();
                throw failure;
            }
            /* Capacity exhaustion is an admission result, not evidence that the
             * replication boundary is corrupt. Do not poison the publisher merely
             * because the local Archive is temporarily full. */
            try {
                this.ensureWriteAdmitted(metadata.dataLength());
            } catch (final RuntimeException | Error admissionFailure) {
                this.clearBufferScratch();
                throw admissionFailure;
            }
            try {
                sequence = this.publisher.reserveSequence();
                this.notifyState(AeronReplicationCheckpoint.State.PREPARING, sequence,
                        metadata.dataLength(), metadata.dataChunkCount(), metadata.crc32c(), -1);
            } catch (final RuntimeException | Error failure) {
                this.clearBufferScratch();
                this.publisher.failClosed();
                throw failure;
            }
        } else {
            this.ensureWriteAdmitted(local.metadata().dataLength());
        }
        try {
            prepared = archiveFirst
                    ? this.publisher.prepareTransaction(this.pendingDictionary, buffers, bufferCount, sequence, metadata)
                    : this.publisher.prepareTransaction(this.pendingDictionary, buffers,
                    bufferCount, local.sequence(), local.metadata());
        } catch (final RuntimeException | Error failure) {
            this.clearBufferScratch();
            if (archiveFirst) this.publisher.failClosed();
            throw failure;
        }
        this.clearBufferScratch();
        prepared.onAbort(abortPosition ->
        {
            try {
                /* A negative position means the ABORT marker was offered but its
                 * durable Archive position could not be established. Treat that path as
                 * uncertain; a restart must reseed rather than accept a rejection whose
                 * terminal evidence may still be in flight. */
                final AeronReplicationCheckpoint.State state = abortPosition < 0
                        ? AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN
                        : AeronReplicationCheckpoint.State.REJECTED;
                this.listener.onState(state, prepared.sequence(),
                        prepared.dataLength(), prepared.dataChunkCount(), prepared.dataCrc32c(), abortPosition);
            } catch (final RuntimeException | Error failure) {
                this.publisher.failClosed();
                throw failure;
            }
        });
        return prepared;
    }

    void markEnqueued(final AeronReplicationPublisher.PreparedTransaction prepared) {
        this.writeLock.lock();
        try {
            this.markEnqueuedLocked(prepared);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void markEnqueuedLocked(final AeronReplicationPublisher.PreparedTransaction prepared) {
        this.ensureNotCommitting();
        final LocalEnqueue local = this.localAcceptanceFence;
        if (local != null && local.sequence() == prepared.sequence()) {
            // ENQUEUE_THEN_ARCHIVE fenced the local write before preparation; do
            // not emit a second identical state transition after publication.
            return;
        }
        if (this.durabilityMode == ReplicationDurabilityMode.ARCHIVE_FIRST) {
            /* PREPARING is already a durable refusal fence for this mode.  Replacing
             * it with ENQUEUED adds another forced file+directory sync without adding
             * recovery information; COMMITTED/REJECTED is the next meaningful state. */
            return;
        }
        try {
            this.notifyState(AeronReplicationCheckpoint.State.ENQUEUED, prepared.sequence(),
                    prepared.dataLength(), prepared.dataChunkCount(), prepared.dataCrc32c(), -1);
        } catch (final RuntimeException | Error failure) {
            this.publisher.failClosed();
            throw failure;
        }
    }

        /// Records a local ENQUEUE before Aeron preparation begins. This closes the
    /// dual-write window where the Store can accept data and the process can die
    /// before the reserved sequence is durably recorded. The returned sequence is
    /// the one that preparation must later reuse.
    long markLocalEnqueue(final Binary data) {
        this.writeLock.lock();
        try {
            return this.markLocalEnqueueLocked(data);
        } finally {
            this.writeLock.unlock();
        }
    }

    private long markLocalEnqueueLocked(final Binary data) {
        this.ensureNotCommitting();
        Objects.requireNonNull(data, "data");
        if (this.localAcceptanceFence != null) {
            throw new IllegalStateException("an Aeron local acceptance fence is already pending");
        }
        if (this.publisher.hasSequenceReservation()) {
            throw new IllegalStateException("cannot start a local acceptance while a sequence is reserved");
        }
        final int bufferCount = this.collectBuffers(data);
        /* Keep the fence's view immutable until preparation consumes it. The
         * coordinator monitor currently serializes writes, but a re-entrant Store
         * callback must not be able to overwrite the reusable scratch array that a
         * pending fence references. This is a shallow copy: the off-heap buffers
         * themselves remain owned by the Store and are never copied here. */
        final ByteBuffer[] buffers = Arrays.copyOf(this.bufferScratch, bufferCount);
        final AeronReplicationPublisher.TransactionMetadata metadata;
        final long sequence;
        try {
            metadata = this.publisher.transactionMetadata(buffers, bufferCount);
        } catch (final RuntimeException | Error failure) {
            this.publisher.failClosed();
            this.clearBufferScratch();
            throw failure;
        }
        try {
            this.ensureWriteAdmitted(metadata.dataLength());
        } catch (final RuntimeException | Error admissionFailure) {
            this.clearBufferScratch();
            throw admissionFailure;
        }
        try {
            /* Reserve the sequence before writing the fence.  The ENQUEUE_THEN_ARCHIVE
             * preparation must reuse this exact reservation; reading nextSequence()
             * here would leave the fence one sequence behind the published data. */
            sequence = this.publisher.reserveSequence();
            /* No second collection is admitted while this fence exists, so the
             * writer-owned scratch remains stable until prepare consumes it. */
            this.localAcceptanceFence = new LocalEnqueue(sequence, metadata, data, buffers, bufferCount);
            this.notifyState(AeronReplicationCheckpoint.State.ENQUEUED, sequence,
                    metadata.dataLength(), metadata.dataChunkCount(), metadata.crc32c(), -1);
        } catch (final RuntimeException | Error failure) {
            /* A failed fence write must not leave a live reservation that a later
             * transaction could accidentally skip or reuse. The publisher is failed
             * closed because the durable boundary itself is no longer trustworthy. */
            this.publisher.failClosed();
            if (this.localAcceptanceFence != null) {
                try {
                    this.publisher.releaseReservedSequence(this.localAcceptanceFence.sequence());
                } catch (final RuntimeException releaseFailure) {
                    failure.addSuppressed(releaseFailure);
                }
            }
            this.localAcceptanceFence = null;
            this.clearBufferScratch();
            throw failure;
        }
        return sequence;
    }

    private void ensureWriteAdmitted(final int dataLength) {
        if (!this.leaseGate.isValid()) {
            this.publisher.failLeaseLost();
            throw new IllegalStateException(
                    "writer fencing lease lost, restart required; this writer is fenced");
        }
        final long dictionaryLength = this.pendingDictionary == null ? 0L : this.pendingDictionary.length;
        final long requiredBytes = Math.addExact(dataLength, dictionaryLength);
        if (requiredBytes > this.publisher.maxTransactionBytes()) {
            throw new IllegalArgumentException("Store transaction exceeds maxTransactionBytes");
        }
        if (!this.writeAdmission.test(requiredBytes)) {
            throw new IllegalStateException(
                    "Aeron Archive has insufficient free capacity for transaction bytes=%s".formatted(requiredBytes));
        }
    }

        /// Returns whether the publisher can still accept a transaction.
    boolean isWritable() {
        return !this.publisher.isFailed() && !this.publisher.isClosed();
    }

        /// Executes Archive maintenance while this coordinator excludes every Store
    /// write. The supplied operation is responsible for stopping and extending the
    /// recording; keeping this monitor held prevents an unrecorded publication gap.
    ///
    /// @param maintenance bounded Archive maintenance operation
    /// @return operation result
    public long withWritesPaused(final LongSupplier maintenance) {
        Objects.requireNonNull(maintenance, "maintenance");
        this.writeLock.lock();
        try {
            this.awaitNoCommit();
            if (this.localAcceptanceFence != null || this.publisher.hasPendingTransaction()) {
                throw new IllegalStateException("cannot run Archive maintenance while a transaction is pending");
            }
            return maintenance.getAsLong();
        } finally {
            this.writeLock.unlock();
        }
    }

        /// Clears the pre-enqueue fence when the Store rejected the write.
    void clearLocalEnqueue() {
        this.writeLock.lock();
        try {
            this.clearLocalEnqueueLocked();
        } finally {
            this.writeLock.unlock();
        }
    }

    private void clearLocalEnqueueLocked() {
        this.ensureNotCommitting();
        final LocalEnqueue local = this.localAcceptanceFence;
        if (local == null) return;
        try {
            /* The Store rejected the write. There is no Aeron transaction to
             * represent, so clear only the local acceptance fence. */
            this.listener.clearEnqueueFence();
        } catch (final Error failure) {
            /* The fence already makes recovery fail closed.  Never perform checkpoint
             * I/O while handling a fatal JVM error; preserving that Error is safer than
             * attempting to allocate or force another marker. */
            this.publisher.failClosed();
            throw failure;
        } catch (final RuntimeException failure) {
            this.publisher.failClosed();
            try {
                this.publisher.releaseReservedSequence(local.sequence());
            } catch (final RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
            this.localAcceptanceFence = null;
            this.clearBufferScratch();
            throw failure;
        }
        try {
            this.publisher.releaseReservedSequence(local.sequence());
        } catch (final RuntimeException | Error failure) {
            /* A reservation that cannot be released must never be reused by a later
             * write. Fail closed before propagating the cleanup error. */
            this.publisher.failClosed();
            throw failure;
        } finally {
            this.localAcceptanceFence = null;
            this.clearBufferScratch();
        }
    }

        /// Records that the local Store accepted an ENQUEUE_THEN_ARCHIVE write but
    /// publication preparation failed. The marker deliberately remains
    /// non-terminal so restart fails closed instead of assuming the Archive has
    /// the local transaction; recovery must reseed or explicitly repair the gap.
    void markEnqueueWithoutArchive() {
        this.writeLock.lock();
        try {
            this.markEnqueueWithoutArchiveLocked();
        } finally {
            this.writeLock.unlock();
        }
    }

    private void markEnqueueWithoutArchiveLocked() {
        this.ensureNotCommitting();
        final AeronReplicationPublisher.FailedPrepare failed = this.publisher.failedPrepare();
        final LocalEnqueue local = this.localAcceptanceFence;
        if (failed == null && local == null) {
            throw new IllegalStateException("no Aeron sequence was reserved by the failed prepare");
        }
        final long sequence = failed == null ? local.sequence() : failed.sequence();
        final int dataLength = failed == null ? local.metadata().dataLength() : failed.dataLength();
        final int dataChunkCount = failed == null ? local.metadata().dataChunkCount() : failed.dataChunkCount();
        final int crc32c = failed == null ? local.metadata().crc32c() : failed.crc32c();
        try {
            CrashHook.invoke("DURING_COMMITTING_UNCERTAIN_WRITE", sequence);
            this.notifyState(AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN, sequence,
                    dataLength, dataChunkCount, crc32c, -1);
            /* prepareReserved() clears its reservation on failure. The hook between
             * local acceptance and preparation does not create a FailedPrepare, so
             * consume that outstanding reservation after the uncertainty marker is
             * durable without rewinding the sequence. */
            if (failed == null) this.publisher.abandonReservedSequence(sequence);
        } catch (final RuntimeException | Error failure) {
            this.publisher.failClosed();
            if (failed == null && this.publisher.hasSequenceReservation()) {
                try {
                    this.publisher.abandonReservedSequence(sequence);
                } catch (final RuntimeException reservationFailure) {
                    failure.addSuppressed(reservationFailure);
                }
            }
            throw failure;
        }
        this.localAcceptanceFence = null;
        this.clearBufferScratch();
    }

        /// Publishes the commit marker and waits for the durability boundary.
    ///
    /// The caller must hold no write lock: the commit releases it for the
    /// Archive wait, and requiring a zero entry hold count turns a nested call
    /// into an immediate failure instead of a silent lock-accounting bug.
    /// Admission is granted by [commitOrMarkUncertain]; the commit guard is
    /// set here as well (idempotently) so a direct caller is still protected.
    void commit(final AeronReplicationPublisher.PreparedTransaction prepared) {
        if (this.writeLock.isHeldByCurrentThread()) {
            throw new IllegalStateException(
                    "Aeron commit must be called with the write lock released");
        }
        this.writeLock.lock();
        try {
            this.ensureWriterIdentity();
            this.commitInProgress = true;
            this.commitMarkerPublished = false;
        } finally {
            this.writeLock.unlock();
        }
        /* The marker offer runs under interprocess lease ownership while the
         * Archive acknowledgement wait runs outside it: the offer retries under
         * back pressure long enough for a successor to steal the lease, and a
         * marker offered after that steal can never be retracted. Do not hold
         * the write lock during either slow wait; commitInProgress keeps other
         * writers out until the terminal state below is recorded. */
        final long commitPosition;
        try {
            if (!this.leaseGate.isValid()) {
                this.publisher.failLeaseLost();
                throw new IllegalStateException("writer fencing lease lost before commit; restart required");
            }
            CrashHook.invoke("BEFORE_COMMIT_GATE", prepared.sequence());
            try {
                commitPosition = this.leaseGate.offerUnderOwnership(
                        stillOwner -> this.publisher.offerCommitMarker(prepared, stillOwner));
            } catch (final IllegalStateException fenced) {
                this.publisher.failLeaseLost();
                throw new IllegalStateException(
                        "writer fencing lease lost before commit; restart required", fenced);
            }
            final long position = this.publisher.awaitCommitPosition(prepared, commitPosition);
            this.writeLock.lock();
            try {
                if (!this.leaseGate.isValid()) {
                    this.publisher.failLeaseLost();
                    throw new IllegalStateException("writer fencing lease lost during commit; transaction is uncertain");
                }
                try {
                    CrashHook.invoke("AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT", prepared.sequence());
                    this.notifyState(AeronReplicationCheckpoint.State.COMMITTED, prepared.sequence(),
                            prepared.dataLength(), prepared.dataChunkCount(), prepared.dataCrc32c(), position);
                    /* Only a successful checkpoint callback proves that the
                     * durable COMMITTED record is visible. If the callback
                     * throws after a partial write, the outer failure path
                     * records COMMITTING_UNCERTAIN instead of guessing. */
                    this.commitMarkerPublished = true;
                } catch (final RuntimeException | Error failure) {
                    this.publisher.failClosed();
                    throw failure;
                }
                /* The dictionary is part of the durable transaction. Keep it available until
                 * the COMMITTED checkpoint has been written successfully; a checkpoint failure
                 * must never make a retry publish data whose type definitions were dropped. */
                this.pendingDictionary = null;
                this.localAcceptanceFence = null;
                this.clearBufferScratch();
                this.commitMarkerPublished = false;
            } finally {
                this.commitInProgress = false;
                this.commitDone.signalAll();
                this.writeLock.unlock();
            }
        } catch (final RuntimeException | Error failure) {
            /* The marker offer itself failed before any terminal state was
             * recorded. Clear the guard so commitOrMarkUncertain can mark the
             * transaction uncertain instead of wedging the coordinator. */
            this.writeLock.lock();
            try {
                this.commitInProgress = false;
                this.commitDone.signalAll();
            } finally {
                this.writeLock.unlock();
            }
            throw failure;
        }
    }

    void abort(final AeronReplicationPublisher.PreparedTransaction prepared) {
        this.writeLock.lock();
        try {
            this.ensureNotCommitting();
            /* Reserve the coordinator state, then release the lock before the
             * retrying Aeron offer and recorded-position wait. Other writers are
             * rejected by the existing terminal-operation guard, while health,
             * backup, and dispose paths are not blocked behind Archive progress. */
            this.commitInProgress = true;
        } finally {
            this.writeLock.unlock();
        }
        try {
            /* prepare() registers the rejection callback on the token. The publisher
             * invokes it for every successful abort path, including direct publisher
             * aborts and shutdown, so the checkpoint transition cannot be skipped or
             * emitted twice. */
            this.publisher.abort(prepared);
        } catch (final RuntimeException | Error failure) {
            this.publisher.failClosed();
            throw failure;
        } finally {
            this.writeLock.lock();
            try {
                this.localAcceptanceFence = null;
                this.clearBufferScratch();
                this.commitInProgress = false;
                this.commitDone.signalAll();
            } finally {
                this.writeLock.unlock();
            }
        }
    }

        /// Collects channel buffers into the reusable writer-owned array.
    private int collectBuffers(final Binary data) {
        Objects.requireNonNull(data, "data");
        this.bufferScratchCount = 0;
        data.iterateChannelChunks(channel ->
        {
            if (channel == null) throw new IllegalStateException("Serializer returned a null channel");
            for (final ByteBuffer buffer : channel.buffers()) {
                if (buffer == null) throw new IllegalStateException("Serializer returned a null channel buffer");
                if (this.bufferScratchCount == this.bufferScratch.length) {
                    this.bufferScratch = Arrays.copyOf(this.bufferScratch, this.bufferScratch.length * 2);
                }
                this.bufferScratch[this.bufferScratchCount++] = buffer;
            }
        });
        return this.bufferScratchCount;
    }

        /// Drops references to the last transaction's source buffers.
    private void clearBufferScratch() {
        Arrays.fill(this.bufferScratch, 0, this.bufferScratchCount, null);
        this.bufferScratchCount = 0;
    }

    void markCommittingUncertain(final AeronReplicationPublisher.PreparedTransaction prepared) {
        this.writeLock.lock();
        try {
            this.markCommittingUncertainLocked(prepared);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void markCommittingUncertainLocked(final AeronReplicationPublisher.PreparedTransaction prepared) {
        this.ensureNotCommitting();
        try {
            this.notifyState(AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN, prepared.sequence(),
                    prepared.dataLength(), prepared.dataChunkCount(), prepared.dataCrc32c(), -1);
        } catch (final RuntimeException | Error failure) {
            this.publisher.failClosed();
            throw failure;
        }
    }

    private void notifyState(final AeronReplicationCheckpoint.State state, final long sequence,
                             final int dataLength, final int dataChunkCount, final int dataCrc32c, final long position) {
        this.listener.onState(state, sequence, dataLength, dataChunkCount, dataCrc32c, position);
    }

    private void ensureNotCommitting() {
        if (this.commitInProgress) {
            throw new IllegalStateException("Aeron commit is in progress");
        }
    }

        /// Waits, bounded, for an in-flight commit to finish.
    ///
    /// Shutdown and Archive maintenance must not fail merely because a commit
    /// released the write lock and is still awaiting its Archive position; the
    /// wait is bounded by the recorded-position timeout so a stuck commit
    /// cannot hang shutdown forever. Call with the write lock held.
    private void awaitNoCommit() {
        if (!this.commitInProgress) {
            return;
        }
        final long deadline = System.nanoTime() + this.publisher.recordedPositionTimeoutNanos();
        while (this.commitInProgress) {
            final long remaining = deadline - System.nanoTime();
            if (remaining <= 0L) {
                throw new IllegalStateException(
                        "Aeron commit did not finish within the recorded-position timeout");
            }
            try {
                /* A signal only hints at progress and a timeout only means
                 * re-check: the loop guard plus the deadline above decide. */
                this.commitDone.await(remaining, TimeUnit.NANOSECONDS);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "interrupted while waiting for the Aeron commit to finish", interrupted);
            }
        }
    }

        /// Closes the publisher owned by this coordinator.
    @Override
    public void close() {
        this.dispose();
    }

        /// Releases the publisher owned by this coordinator.
    public void dispose() {
        this.writeLock.lock();
        try {
            this.disposeLocked();
        } finally {
            this.writeLock.unlock();
        }
    }

    private void disposeLocked() {
        this.awaitNoCommit();
        /* ENQUEUE_THEN_ARCHIVE may be interrupted after the Store accepted data
         * but before the Archive terminal marker was published.  Never turn that
         * local acceptance into an ABORT during shutdown: close the publication
         * without a terminal marker and keep the durable ENQUEUED fence so the next
         * process reseeds instead of silently discarding Store data. */
        if (this.localAcceptanceFence != null && this.publisher.hasPendingTransaction()) {
            this.publisher.closeWithoutAbort();
            this.publisher.releaseCoordinator(this);
            this.localAcceptanceFence = null;
            this.pendingDictionary = null;
            this.clearBufferScratch();
            return;
        }
        /* Keep the coordinator claim until publication shutdown has completed.  If an
         * abort/close offer is transiently unavailable, releasing first would allow a
         * second coordinator to claim the same publisher while this one still owns a
         * pending sequence. */
        this.publisher.close();
        this.publisher.releaseCoordinator(this);
        this.localAcceptanceFence = null;
        this.pendingDictionary = null;
        this.clearBufferScratch();
    }

    @FunctionalInterface
    interface WriteOperation {
        void run();
    }

        /// Pairs a reserved sequence with the transaction queued for publication.
    private record LocalEnqueue(long sequence, AeronReplicationPublisher.TransactionMetadata metadata,
                                Binary source, ByteBuffer[] buffers, int bufferCount) {
    }
}
