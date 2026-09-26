package peruncs.cluster.storage.aeron.writer;

import org.eclipse.serializer.persistence.binary.types.Binary;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.errors.WriterFencedException;
import peruncs.cluster.storage.ReplicationRetry;
import peruncs.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;

/// Keeps local Store acceptance and Aeron publication in one ordered state
/// machine.
///
/// A reserved write owns its dictionary, source views, and prepared transaction.
/// The coordinator reports state changes to the checkpoint writer so
/// restart can distinguish a committed transaction from an uncertain one.
///
/// Lock order is strict: coordinator {@code writeLock} → publisher state
/// monitor. The coordinator lock is a short
/// state-transition lock, never a wait lock: the slow marker offer and the
/// Archive acknowledgement wait run with no coordinator lock held, and the
/// in-progress guard is what keeps a second writer out. No callback may
/// acquire the coordinator lock while holding the publisher state monitor.
///
/// Every coordinator method either holds no lock or acquires {@code writeLock}
/// in a {@code try/finally} that releases it before returning or throwing, so
/// a failed method can never hand a leaked hold to its caller.
///
/// This is an Aeron-only write coordinator. Store integration must use
/// [AeronStorageBinaryReplicationTarget]; exposing this object as the
/// publisher would allow publication without local Store
/// acceptance and would bypass the durable fence.
public final class AeronReplicationWriteCoordinator implements AutoCloseable {
    private final AeronReplicationPublisher publisher;
    private final AeronArchiveReplicationPublisher.CheckpointWriter listener;
    private final LongPredicate writeAdmission;
    /* Lease validity is checked before Archive capacity on every admission, so
     * a fenced writer fails with a distinct lease-lost error instead of a
     * misleading capacity-exhaustion message. Terminal-marker offers run
     * through the same gate under interprocess ownership (see WriterLeaseGate)
     * so a steal racing back pressure cannot slip a stale marker into Aeron. */
    private final WriterLeaseGate leaseGate;
    /* The single lock for all coordinator state below. It is reentrant: write
     * admission holds it across the fast phase of a Store transaction
     * (marking, local acceptance, preparation) while the state transitions
     * nest inside. The slow phase of a commit never holds it: the in-progress
     * guard is set under a short hold and the wait itself runs unlocked, so
     * health, maintenance, and dispose paths never block behind Archive
     * progress. A standalone abort likewise releases before its bounded offer;
     * only the target's local-rejection path keeps its admission hold across
     * that bounded abort offer. */
    private final ReentrantLock writeLock = new ReentrantLock();
    /* Set before Archive maintenance waits for the current writer. New Store
     * writes fail immediately instead of queueing behind an operation whose
     * caller may already have timed out. */
    private final java.util.concurrent.atomic.AtomicBoolean maintenance =
            new java.util.concurrent.atomic.AtomicBoolean();
    /* Writer identity pinned at claim time. The publisher seeds its sequence
     * from the checkpoint (recording ID + epoch) at construction; these values
     * refuse a publisher that was swapped or rewound underneath this
     * coordinator. Cross-process fencing stays with the checkpoint epoch and
     * Archive recording ownership enforced when the publisher is built. */
    private final long writerEpoch;
    private final long initialSequence;
    /* Only populated while no write owns admission. Reservation transfers these
     * bytes to PreparedWrite; a rejected local write returns them for retry. */
    private byte[] retryDictionary;
    /* The coordinator is single-threaded. Reusing this channel-order scratch
     * array removes the ArrayList and temporary array from every write. A local
     * acceptance fence keeps the count beside the array until preparation ends. */
    private ByteBuffer[] bufferScratch = new ByteBuffer[8];
    private int bufferScratchCount;
    /* Set only after the Archive recorded position has been acknowledged and
     * the COMMITTED checkpoint has been written. A checkpoint cleanup failure
     * after that point must not overwrite a durable COMMITTED record with
     * COMMITTING_UNCERTAIN. */
    private PreparedWrite activeWrite;
    /* Signalled whenever the active write is cleared. Shutdown and Archive
     * maintenance wait on it (bounded) instead of failing while a commit that
     * holds no coordinator lock is still awaiting its Archive position. The
     * condition belongs to writeLock because it guards the same flag and the
     * waiters already hold writeLock while inspecting the transaction state. */
    private final Condition writeDone = this.writeLock.newCondition();

    /// One immutable snapshot owns admission until its terminal outcome is known.
    private record PreparedWrite(long sequence, byte[] dictionary, ByteBuffer[] buffers, int bufferCount,
                                 AeronReplicationPublisher.TransactionMetadata metadata, long fencingToken,
                                 AeronReplicationPublisher.PreparedTransaction transaction,
                                 State state, Thread ownerThread) {
        private enum State { RESERVED, PREPARED, COMMITTING, COMMITTED, REJECTING, REJECTED, UNCERTAIN }

        private PreparedWrite withTransaction(final AeronReplicationPublisher.PreparedTransaction prepared) {
            return new PreparedWrite(sequence, dictionary, buffers, bufferCount, metadata, fencingToken,
                    prepared, State.PREPARED, ownerThread);
        }

        private PreparedWrite withState(final State next, final Thread owner) {
            return new PreparedWrite(sequence, dictionary, buffers, bufferCount, metadata, fencingToken,
                    transaction, next, owner);
        }
    }

    AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher) {
        this(publisher, (state, sequence, length, chunks, crc, position) -> {
        }, ignored -> true, WriterLeaseGate.alwaysValid());
    }

    AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher,
                                     final AeronArchiveReplicationPublisher.CheckpointWriter listener) {
        this(publisher, listener, ignored -> true, WriterLeaseGate.alwaysValid());
    }

    AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher,
                                      final AeronArchiveReplicationPublisher.CheckpointWriter listener,
                                      final LongPredicate writeAdmission,
                                      final WriterLeaseGate leaseGate) {
        Objects.requireNonNull(publisher, "publisher");
        Objects.requireNonNull(listener, "listener");
        Objects.requireNonNull(writeAdmission, "writeAdmission");
        Objects.requireNonNull(leaseGate, "leaseGate");
        this.publisher = publisher;
        this.listener = listener;
        this.writeAdmission = writeAdmission;
        this.leaseGate = leaseGate;
        this.publisher.claimCoordinator(this, leaseGate);
        this.writerEpoch = publisher.epoch();
        this.initialSequence = publisher.nextSequence();
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
                this.retryDictionary = null;
            } else {
                final byte[] encoded = typeDictionaryData.getBytes(StandardCharsets.UTF_8);
                if (encoded.length > this.publisher.maxTransactionBytes()) {
                    throw new IllegalArgumentException("type dictionary exceeds maxTransactionBytes");
                }
                this.retryDictionary = encoded;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /// Runs the fast write phase and reserves its prepared transaction before
    /// releasing admission. This closes the gap in which another Store writer
    /// could replace the staged dictionary before commit established its guard.
    AeronReplicationPublisher.PreparedTransaction prepareWriteAtomically(
            final WriteOperation<AeronReplicationPublisher.PreparedTransaction> operation) {
        Objects.requireNonNull(operation, "operation");
        this.lockWriteAdmission();
        try {
            this.ensureNotCommitting();
            this.ensureWriterIdentity();
            return operation.run();
        } catch (final RuntimeException | Error failure) {
            this.clearActiveWrite();
            this.clearBufferScratch();
            throw failure;
        } finally {
            this.writeLock.unlock();
        }
    }

        /// Publishes and commits one Store binary using the archive-first fence.
    ///
    /// This entry point is for the neutral publisher, which has no local
    /// persistence target to fence. Store writes should use
    /// [AeronStorageBinaryReplicationTarget] so local acceptance and
    /// publication remain one operation.
    ///
    /// @param data binary to publish
    void distributeData(final Binary data) {
        final AeronReplicationPublisher.PreparedTransaction prepared =
                this.prepareWriteAtomically(() -> this.prepare(data));
        try (prepared) {
            this.commitOrMarkUncertain(prepared);
        }
    }

        /// Commits a token. If the result is unclear, records that fact before
    /// rethrowing so restart cannot silently reuse the sequence.
    ///
    /// Must be called with the write lock released — asserted up front, so a
    /// reentrant caller that still holds the lock fails fast instead of
    /// silently holding it across the slow offer and Archive wait below. The
    /// guard is set under a short lock hold, both slow waits (marker offer and
    /// Archive acknowledgement) run without any coordinator lock, and the
    /// terminal state is recorded under a second short hold. Every lock
    /// acquisition in this method is released before it returns or throws, so
    /// a failure can never leak a hold to the caller and no
    /// `isHeldByCurrentThread` balancing is needed.
    ///
    /// @param prepared transaction whose commit marker is offered now
    void commitOrMarkUncertain(final AeronReplicationPublisher.PreparedTransaction prepared) {
        if (this.writeLock.isHeldByCurrentThread()) {
            throw new IllegalStateException(
                    "commitOrMarkUncertain must be called with the write lock released; the slow offer path may not run under it");
        }
        this.beginCommit(prepared);
        RuntimeException failure = null;
        Error fatal = null;
        try {
            this.performCommit(prepared);
        } catch (final Error error) {
            fatal = error;
        } catch (final RuntimeException error) {
            failure = error;
        }
        this.finishCommit(prepared, failure, fatal);
        /* Throw only after every lock has been released; a fatal JVM error is
         * not a recoverable publication failure and must not trigger checkpoint
         * I/O, so it is recorded as failed closed but not as uncertain. */
        if (fatal != null) throw fatal;
        if (failure != null) throw failure;
    }

    /// Sets the commit guard and pins the writer identity.
    ///
    /// Called with no lock held; the method holds {@code writeLock} only for
    /// the state change. Setting the guard before any slow wait is what makes
    /// a concurrent writer fail fast with {@code "Aeron commit is in progress"}
    /// instead of preparing against the same publisher.
    private void beginCommit(final AeronReplicationPublisher.PreparedTransaction prepared) {
        this.writeLock.lock();
        try {
            if (this.activeWrite == null || this.activeWrite.transaction() != prepared) {
                throw new IllegalStateException("prepared transaction does not own Aeron write admission");
            }
            if (this.activeWrite.state() != PreparedWrite.State.PREPARED) {
                throw new IllegalStateException("Aeron write is not ready to commit: " + this.activeWrite.state());
            }
            try {
                this.ensureWriterIdentity();
                if (this.publisher.fencingToken() != this.activeWrite.fencingToken()) {
                    this.publisher.failClosed();
                    throw new WriterFencedException("prepared transaction lost its writer fencing token");
                }
            } catch (final RuntimeException | Error failure) {
                this.clearActiveWrite();
                this.clearBufferScratch();
                throw failure;
            }
            this.activeWrite = this.activeWrite.withState(PreparedWrite.State.COMMITTING, Thread.currentThread());
        } finally {
            this.writeLock.unlock();
        }
    }

    /// Clears the commit guard and records uncertainty when the commit failed.
    ///
    /// Called with no lock held after the slow phase. Fatal errors fail the
    /// publisher closed without a second checkpoint marker; a recoverable
    /// failure marks the transaction uncertain unless the durable COMMITTED
    /// record was already written.
    private void finishCommit(final AeronReplicationPublisher.PreparedTransaction prepared,
                              final RuntimeException failure, final Error fatal) {
        this.writeLock.lock();
        try {
            final PreparedWrite completed = this.activeWrite;
            if (fatal != null) {
                this.publisher.failClosed();
            } else if (failure != null) {
                if (completed == null || completed.state() != PreparedWrite.State.COMMITTED) {
                    try {
                        this.markCommittingUncertainLocked(prepared);
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
            }
        } finally {
            this.clearActiveWrite();
            this.clearBufferScratch();
            this.writeLock.unlock();
        }
    }

        /// Publishes a transaction's prepare phase after recording its PREPARING fence.
    AeronReplicationPublisher.PreparedTransaction prepare(final Binary data) {
        if (this.writeLock.isHeldByCurrentThread()) {
            return this.prepareLocked(data);
        }
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
        /* PREPARING is reported before the publisher consumes the reusable buffer
         * array. A listener is allowed to run on this thread, so reject re-entrant
         * preparation before it can recollect into that array and corrupt the outer
         * transaction. */
        if (this.publisher.hasSequenceReservation()) {
            throw new IllegalStateException("cannot re-enter Aeron preparation while a sequence is reserved");
        }
        final int bufferCount = this.collectBuffers(data);
        final ByteBuffer[] buffers = this.bufferScratch;
        final byte[] dictionary = this.retryDictionary;
        AeronReplicationPublisher.TransactionMetadata metadata;
        long sequence;
        /* Reserve the sequence and persist PREPARING before publication so a
         * crash after local Store acceptance remains visible to recovery. */
        try {
            metadata = this.publisher.transactionMetadata(buffers, bufferCount);
        } catch (final RuntimeException | Error failure) {
            this.clearBufferScratch();
            this.publisher.failClosed();
            throw failure;
        }
        try {
            this.ensureWriteAdmitted(metadata.dataLength(), dictionary);
        } catch (final RuntimeException | Error admissionFailure) {
            this.clearBufferScratch();
            throw admissionFailure;
        }
        try {
            sequence = this.publisher.reserveSequence();
            this.activeWrite = new PreparedWrite(sequence, dictionary, buffers, bufferCount,
                    metadata, this.publisher.fencingToken(), null, PreparedWrite.State.RESERVED,
                    Thread.currentThread());
            this.retryDictionary = null;
            this.notifyStateOutsideAdmission(AeronReplicationCheckpoint.State.PREPARING, sequence,
                    metadata.dataLength(), metadata.dataChunkCount(), metadata.crc32c(), -1);
        } catch (final RuntimeException | Error failure) {
            this.clearActiveWrite();
            this.clearBufferScratch();
            this.publisher.failClosed();
            throw failure;
        }
        try {
            prepared = this.publisher.prepareTransaction(
                    this.activeWrite.dictionary(), this.activeWrite.buffers(),
                    this.activeWrite.bufferCount(), this.activeWrite.sequence(), this.activeWrite.metadata());
        } catch (final RuntimeException | Error failure) {
            this.clearActiveWrite();
            this.clearBufferScratch();
            this.publisher.failClosed();
            throw failure;
        }
        try {
            this.activeWrite = this.activeWrite.withTransaction(prepared);
            if (prepared.sequence() != this.activeWrite.sequence() ||
                prepared.dataLength() != this.activeWrite.metadata().dataLength() ||
                prepared.dataCrc32c() != this.activeWrite.metadata().crc32c() ||
                this.publisher.fencingToken() != this.activeWrite.fencingToken()) {
                throw new IllegalStateException("prepared transaction changed its reserved identity");
            }
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
        } catch (final RuntimeException | Error failure) {
            this.clearActiveWrite();
            this.clearBufferScratch();
            prepared.abandonWithoutAbort();
            this.publisher.failClosed();
            throw failure;
        }
        return prepared;
    }

    private void ensureWriteAdmitted(final int dataLength, final byte[] dictionary) {
        if (!this.leaseGate.isValid()) {
            this.publisher.failLeaseLost();
            throw new WriterFencedException(
                    "writer fencing lease lost, restart required; this writer is fenced");
        }
        final long dictionaryLength = dictionary == null ? 0L : dictionary.length;
        final long requiredBytes = Math.addExact(dataLength, dictionaryLength);
        if (requiredBytes > this.publisher.maxTransactionBytes()) {
            throw new IllegalArgumentException("Store transaction exceeds maxTransactionBytes");
        }
        if (!this.writeAdmission.test(requiredBytes)) {
            throw new ReplicationUnavailableException(
                    "Aeron Archive has insufficient free capacity for transaction bytes=%s".formatted(requiredBytes));
        }
    }

    /// Fails closed when Archive maintenance cannot start.
    ///
    /// Maintenance shares the write-admission contract: a lost lease or a
    /// terminally failed driver must reject the operation with the recorded
    /// cause instead of failing later inside an Archive RPC.
    private void ensureMaintenanceAdmitted() {
        this.ensureWriteAdmitted(0, null);
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
        if (!this.maintenance.compareAndSet(false, true)) {
            throw new IllegalStateException("Aeron Archive maintenance is already in progress");
        }
        try {
            this.ensureMaintenanceAdmitted();
            /* Bounded, interruptible acquisition: a slow local Store write must
             * not park retention or dispose behind admission without a deadline. */
            final long deadline = ReplicationRetry.deadlineNanos(this.publisher.recordedPositionTimeoutNanos());
            try {
                if (!this.writeLock.tryLock(ReplicationRetry.remainingNanos(deadline), TimeUnit.NANOSECONDS)) {
                    throw new ReplicationUnavailableException("timed out waiting for Aeron write admission");
                }
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new ReplicationUnavailableException(
                        "interrupted while waiting for Aeron write admission", interrupted);
            }
            try {
                this.awaitNoCommit();
                if (this.publisher.hasPendingTransaction()) {
                    throw new IllegalStateException("cannot run Archive maintenance while a transaction is pending");
                }
                return maintenance.getAsLong();
            } finally {
                this.writeLock.unlock();
            }
        } finally {
            this.maintenance.set(false);
        }
    }

    private void lockWriteAdmission() {
        if (this.maintenance.get()) {
            throw new IllegalStateException("Aeron Archive maintenance is in progress; write admission is closed");
        }
        final long deadline = ReplicationRetry.deadlineNanos(this.publisher.admissionTimeoutNanos());
        try {
            if (!this.writeLock.tryLock(ReplicationRetry.remainingNanos(deadline), TimeUnit.NANOSECONDS)) {
                throw new ReplicationUnavailableException(
                        "timed out waiting for Aeron write admission", null);
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new ReplicationUnavailableException(
                    "interrupted while waiting for Aeron write admission", interrupted);
        }
        boolean admitted = false;
        try {
            while (this.activeWrite != null) {
                if (this.activeWrite.ownerThread() == Thread.currentThread()) {
                    throw new IllegalStateException("cannot re-enter an active Aeron write");
                }
                if (this.maintenance.get()) {
                    throw new IllegalStateException("Aeron Archive maintenance is in progress; write admission is closed");
                }
                final long remaining = ReplicationRetry.remainingNanos(deadline);
                if (remaining == 0L) {
                    throw new ReplicationUnavailableException("timed out waiting for Aeron write admission", null);
                }
                try {
                    this.writeDone.await(remaining, TimeUnit.NANOSECONDS);
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new ReplicationUnavailableException(
                            "interrupted while waiting for Aeron write admission", interrupted);
                }
            }
            if (this.maintenance.get()) {
                throw new IllegalStateException("Aeron Archive maintenance is in progress; write admission is closed");
            }
            admitted = true;
        } finally {
            if (!admitted) this.writeLock.unlock();
        }
    }

        /// Offers the commit marker under lease ownership and waits for the
    /// durability boundary.
    ///
    /// Called only by [commitOrMarkUncertain] with no coordinator lock held.
    /// The marker offer runs under interprocess lease ownership while the
    /// Archive acknowledgement wait runs outside it: the offer retries under
    /// back pressure long enough for a successor to steal the lease, and a
    /// marker offered after that steal can never be retracted.
    private void performCommit(final AeronReplicationPublisher.PreparedTransaction prepared) {
        if (!this.leaseGate.isValid()) {
            this.publisher.failLeaseLost();
            throw new WriterFencedException("writer fencing lease lost before commit; restart required");
        }
        CrashHook.invoke("BEFORE_COMMIT_GATE", prepared.sequence());
        final long commitPosition;
        try {
            commitPosition = this.publisher.offerCommitMarker(prepared);
        } catch (final WriterFencedException fenced) {
            /* Genuine fencing loss only: the lease gate and the ownership check
             * inside the offer retry loop throw this type exclusively. Every
             * other failure (back-pressure timeout, closed publication,
             * interrupt) keeps its own category and message. */
            this.publisher.failLeaseLost();
            throw fenced;
        }
        final long position = this.publisher.awaitCommitPosition(prepared, commitPosition);
        this.writeLock.lock();
        try {
            if (!this.leaseGate.isValid()) {
                this.publisher.failLeaseLost();
                throw new WriterFencedException(
                        "writer fencing lease lost during commit; transaction is uncertain");
            }
            CrashHook.invoke("AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT", prepared.sequence());
            this.notifyStateOutsideAdmission(AeronReplicationCheckpoint.State.COMMITTED, prepared.sequence(),
                    prepared.dataLength(), prepared.dataChunkCount(), prepared.dataCrc32c(), position);
            /* Only a successful checkpoint callback proves that the durable
             * COMMITTED record is visible. If the callback throws after a
             * partial write, the caller records COMMITTING_UNCERTAIN instead
             * of guessing. */
            this.activeWrite = this.activeWrite.withState(PreparedWrite.State.COMMITTED, Thread.currentThread());
        } catch (final RuntimeException | Error failure) {
            this.publisher.failClosed();
            throw failure;
        } finally {
            this.writeLock.unlock();
        }
    }

    void abort(final AeronReplicationPublisher.PreparedTransaction prepared) {
        this.writeLock.lock();
        try {
            if (this.activeWrite != null && this.activeWrite.state() == PreparedWrite.State.COMMITTING) {
                throw new IllegalStateException("Aeron commit is in progress");
            }
            if (this.activeWrite == null || this.activeWrite.transaction() != prepared) {
                throw new IllegalStateException("prepared transaction does not own Aeron abort admission");
            }
            if (this.activeWrite.state() != PreparedWrite.State.PREPARED) {
                throw new IllegalStateException("Aeron write is not ready to abort: " + this.activeWrite.state());
            }
            this.activeWrite = this.activeWrite.withState(PreparedWrite.State.REJECTING, Thread.currentThread());
            /* Reserve the coordinator state, then release the lock before the
             * retrying Aeron offer and recorded-position wait. Other writers are
             * rejected by the existing terminal-operation guard, while health,
             * backup, and dispose paths are not blocked behind Archive progress. */
        } finally {
            this.writeLock.unlock();
        }
        boolean rejected = false;
        try {
            /* prepare() registers the rejection callback on the token. The publisher
             * invokes it for every successful abort path, including direct publisher
             * aborts and shutdown, so the checkpoint transition cannot be skipped or
             * emitted twice. */
            this.publisher.abort(prepared);
            rejected = true;
        } catch (final RuntimeException | Error failure) {
            this.publisher.failClosed();
            throw failure;
        } finally {
            this.writeLock.lock();
            try {
                if (this.activeWrite != null) {
                    if (rejected && this.activeWrite.dictionary() != null) {
                        this.retryDictionary = this.activeWrite.dictionary();
                    }
                    this.activeWrite = this.activeWrite.withState(
                            rejected ? PreparedWrite.State.REJECTED : PreparedWrite.State.UNCERTAIN,
                            Thread.currentThread());
                }
                this.clearActiveWrite();
                this.clearBufferScratch();
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

    /// Marks the prepared transaction uncertain and releases admission.
    ///
    /// Called from the Store write path while it still holds the single
    /// admission hold, so the checkpoint journal runs at exactly one hold
    /// and the caller's finally keeps balancing its own lock.
    void markCommittingUncertain(final AeronReplicationPublisher.PreparedTransaction prepared) {
        if (!this.writeLock.isHeldByCurrentThread()) {
            throw new IllegalStateException(
                    "markCommittingUncertain requires the caller's Aeron write-admission hold");
        }
        boolean owned = false;
        try {
            if (this.activeWrite == null || this.activeWrite.transaction() != prepared) {
                throw new IllegalStateException("prepared transaction does not own Aeron write admission");
            }
            if (this.activeWrite.state() != PreparedWrite.State.PREPARED) {
                throw new IllegalStateException("Aeron write is not ready to mark uncertain: " + this.activeWrite.state());
            }
            owned = true;
            this.markCommittingUncertainLocked(prepared);
        } finally {
            if (owned) this.clearActiveWrite();
            if (owned) this.clearBufferScratch();
        }
    }

    private void markCommittingUncertainLocked(final AeronReplicationPublisher.PreparedTransaction prepared) {
        try {
            this.notifyStateOutsideAdmission(AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN, prepared.sequence(),
                    prepared.dataLength(), prepared.dataChunkCount(), prepared.dataCrc32c(), -1);
            this.activeWrite = this.activeWrite.withState(PreparedWrite.State.UNCERTAIN, Thread.currentThread());
        } catch (final RuntimeException | Error failure) {
            this.publisher.failClosed();
            throw failure;
        }
    }

    private void notifyState(final AeronReplicationCheckpoint.State state, final long sequence,
                             final int dataLength, final int dataChunkCount, final int dataCrc32c, final long position) {
        this.listener.onState(state, sequence, dataLength, dataChunkCount, dataCrc32c, position);
    }

    /// Runs durable checkpoint I/O without holding write admission.
    ///
    /// Must be called with exactly one {@code writeLock} hold: the caller
    /// reserves the active write first, so a concurrent writer is excluded by
    /// the admission loop and Archive maintenance parks in [awaitNoCommit]
    /// while this runs. The hold count is asserted so a future nested call
    /// cannot journal with a divergent lock stack, and the re-acquire is
    /// bounded and interruptible so a wedged holder fails this writer instead
    /// of parking it without a deadline.
    private void notifyStateOutsideAdmission(final AeronReplicationCheckpoint.State state, final long sequence,
                                             final int dataLength, final int dataChunkCount,
                                             final int dataCrc32c, final long position) {
        if (!this.writeLock.isHeldByCurrentThread() || this.writeLock.getHoldCount() != 1) {
            throw new IllegalStateException(
                    "checkpoint journal write requires exactly one Aeron write-admission hold");
        }
        this.writeLock.unlock();
        Throwable failure = null;
        try {
            this.notifyState(state, sequence, dataLength, dataChunkCount, dataCrc32c, position);
        } catch (final RuntimeException | Error thrown) {
            failure = thrown;
        } finally {
            final long deadline = ReplicationRetry.deadlineNanos(this.publisher.admissionTimeoutNanos());
            boolean relocked = false;
            try {
                relocked = this.writeLock.tryLock(
                        ReplicationRetry.remainingNanos(deadline), TimeUnit.NANOSECONDS);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            if (!relocked) {
                /* Returning without the single hold would let every caller's
                 * finally unlock a stack it does not own (or run cleanup
                 * unguarded). Fail the publisher closed and regain ownership
                 * — newer admissions already refuse the failed writer, so the
                 * in-flight holder finishes and releases; lock() then
                 * preserves the caller's contract and the original failure. */
                this.publisher.failClosed();
                try {
                    this.writeLock.lockInterruptibly();
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    final ReplicationUnavailableException unavailable = new ReplicationUnavailableException(
                            "timed out or interrupted re-acquiring Aeron write admission after a checkpoint write");
                    if (failure != null) unavailable.addSuppressed(failure);
                    throw unavailable;
                }
                final ReplicationUnavailableException unavailable = new ReplicationUnavailableException(
                        "timed out or interrupted re-acquiring Aeron write admission after a checkpoint write");
                if (failure != null) unavailable.addSuppressed(failure);
                throw unavailable;
            }
        }
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException runtime) throw runtime;
    }

    private void clearActiveWrite() {
        if (this.activeWrite != null) {
            this.activeWrite = null;
            this.writeDone.signalAll();
        }
    }

    private void ensureNotCommitting() {
        if (this.activeWrite != null) {
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
        if (this.activeWrite == null) {
            return;
        }
        final long deadline = ReplicationRetry.deadlineNanos(this.publisher.recordedPositionTimeoutNanos());
        while (this.activeWrite != null) {
            final long remaining = ReplicationRetry.remainingNanos(deadline);
            if (remaining == 0L) {
                throw new ReplicationUnavailableException(
                        "Aeron commit did not finish within the recorded-position timeout");
            }
            try {
                /* A signal only hints at progress and a timeout only means
                 * re-check: the loop guard plus the deadline above decide. */
                this.writeDone.await(remaining, TimeUnit.NANOSECONDS);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new ReplicationUnavailableException(
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
        /* Keep the coordinator claim until publication shutdown has completed.  If an
         * abort/close offer is transiently unavailable, releasing first would allow a
         * second coordinator to claim the same publisher while this one still owns a
         * pending sequence. */
        this.publisher.close();
        this.publisher.releaseCoordinator(this);
        this.retryDictionary = null;
        this.clearBufferScratch();
    }

    @FunctionalInterface
    interface WriteOperation<T> {
        T run();
    }

}
