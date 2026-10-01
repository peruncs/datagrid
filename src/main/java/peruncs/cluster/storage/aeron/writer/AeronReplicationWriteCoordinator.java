package peruncs.cluster.storage.aeron.writer;

import org.eclipse.serializer.persistence.binary.types.Binary;
import peruncs.cluster.errors.ReplicationPendingException;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.errors.WriteRejectedException;
import peruncs.cluster.storage.ReplicationRetry;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;
import peruncs.cluster.storage.io.FaultInjection;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/// Keeps local Store acceptance and Aeron publication in one ordered state
/// machine.
///
/// A reserved write owns its dictionary, source views, and prepared transaction.
/// The coordinator advances the in-memory replication boundary after each
/// recorded terminal marker. The Store mark is the restart authority.
///
/// Lock order is strict: coordinator {@code writeLock} → publisher state
/// monitor. Write admission holds the lock across the whole prepare phase:
/// index validation, the chunk offers, the wait for the Archive to record them
/// (up to the recorded-position timeout) and the local Store write. Only the
/// COMMIT offer runs without the lock, and it does not wait for recording. Every
/// other acquisition is bounded by the same timeout. No callback may acquire
/// the coordinator lock while holding the publisher state monitor.
///
/// Every coordinator method either holds no lock or acquires {@code writeLock}
/// in a {@code try/finally} that releases it before returning or throwing, so
/// a failed method can never hand a leaked hold to its caller.
///
/// This is an Aeron-only write coordinator. Store integration must use
/// [AeronStorageBinaryReplicationTarget]; exposing this object as the
/// publisher would allow publication without local Store
/// acceptance and would bypass the Store-mark reservation.
public final class AeronReplicationWriteCoordinator implements AutoCloseable {
    private final AeronReplicationPublisher publisher;
    private final LongConsumer terminalRecorded;
    private final LongPredicate writeAdmission;
    /* The single lock for all coordinator state below. It is reentrant: write
     * admission holds it across the fast phase of a Store transaction
     * (marking, preparation, Archive recording wait, local acceptance) while
     * the state transitions nest inside. The COMMIT offer runs unlocked. Other
     * paths acquire it with a deadline (see lockBounded), so health,
     * maintenance, and dispose fail fast behind a stuck prepare instead of
     * waiting forever. */
    private final ReentrantLock writeLock = new ReentrantLock();
    /* Set before Archive maintenance waits for the current writer. New Store
     * writes fail immediately instead of queueing behind an operation whose
     * caller may already have timed out. */
    private final AtomicBoolean maintenance = new AtomicBoolean();
    /* Writer identity pinned at claim time. These values refuse a publisher
     * that was swapped or rewound underneath this coordinator. */
    private final long writerEpoch;
    private final long initialSequence;
    /* Only populated while no write owns admission. Reservation transfers these
     * bytes to PreparedWrite; a rejected local write returns them for retry. */
    private byte[] retryDictionary;
    /* The coordinator is single-threaded. Reusing this channel-order scratch
     * array removes the ArrayList and temporary array from every write. The
     * count stays beside the array until preparation ends. */
    private ByteBuffer[] bufferScratch = new ByteBuffer[8];
    private int bufferScratchCount;
    /* Set while a Store write owns admission, including while it waits for the
     * Archive terminal position. */
    private PreparedWrite activeWrite;
    private volatile RuntimeException failure;
    private MarkReservation markReservation;
    /* Signalled whenever the active write is cleared. Shutdown and Archive
     * maintenance wait on it (bounded) instead of failing while a commit that
     * holds no coordinator lock is still awaiting its Archive position. The
     * condition belongs to writeLock because it guards the same flag and the
     * waiters already hold writeLock while inspecting the transaction state. */
    private final Condition writeDone = this.writeLock.newCondition();

    private record MarkReservation(long sequence, Thread ownerThread) {
    }

    /// One immutable snapshot owns admission until its terminal outcome is known.
    private record PreparedWrite(long sequence, byte[] dictionary, ByteBuffer[] buffers, int bufferCount,
                                 int dataLength, long fencingToken,
                                 PreparedTransaction transaction,
                                 State state, Thread ownerThread) {
        private enum State { RESERVED, PREPARED, COMMITTING, REPLICATION_SUSPENDED, REJECTING }

        private PreparedWrite withTransaction(final PreparedTransaction prepared) {
            return new PreparedWrite(sequence, dictionary, buffers, bufferCount, dataLength, fencingToken,
                    prepared, State.PREPARED, ownerThread);
        }

        private PreparedWrite withState(final State next, final Thread owner) {
            return new PreparedWrite(sequence, dictionary, buffers, bufferCount, dataLength, fencingToken,
                    transaction, next, owner);
        }

        private PreparedWrite suspended() {
            return new PreparedWrite(sequence, null, null, 0, dataLength, fencingToken,
                    transaction, State.REPLICATION_SUSPENDED, null);
        }
    }

    AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher) {
        this(publisher, ignored -> { }, ignored -> true);
    }

    AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher,
                                     final LongConsumer terminalRecorded) {
        this(publisher, terminalRecorded, ignored -> true);
    }

    AeronReplicationWriteCoordinator(final AeronReplicationPublisher publisher,
                                      final LongConsumer terminalRecorded,
                                      final LongPredicate writeAdmission) {
        Objects.requireNonNull(publisher, "publisher");
        Objects.requireNonNull(terminalRecorded, "terminalRecorded");
        Objects.requireNonNull(writeAdmission, "writeAdmission");
        this.publisher = publisher;
        this.terminalRecorded = terminalRecorded;
        this.writeAdmission = writeAdmission;
        this.publisher.claimCoordinator(this);
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
        this.lockBounded();
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
    PreparedTransaction prepareWriteAtomically(
            final Supplier<PreparedTransaction> operation) {
        Objects.requireNonNull(operation, "operation");
        this.lockWriteAdmission();
        try {
            this.ensureNotCommitting();
            this.ensureWriterIdentity();
            return operation.get();
        } catch (final RuntimeException | Error failure) {
            this.clearActiveWrite();
            this.cancelMarkReservationLocked();
            this.clearBufferScratch();
            throw failure;
        } finally {
            this.writeLock.unlock();
        }
    }

    /// Reserves the sequence and Archive position that the Store serializes
    /// into its mark. Maintenance waits for the Store commit to consume or
    /// cancel this reservation.
    public void reserveStoreCommit(final ReplicationMark mark,
                                   final AeronArchiveReplicationPublisher archivePublisher) {
        Objects.requireNonNull(mark, "mark");
        Objects.requireNonNull(archivePublisher, "archivePublisher");
        this.lockWriteAdmission();
        try {
            this.ensureNotCommitting();
            this.ensureWriterIdentity();
            if (mark.clusterId() == null || !mark.clusterId().equals(this.publisher.clusterId()) ||
                mark.epoch() != this.writerEpoch) {
                throw new IllegalArgumentException("replication mark identity does not match the writer");
            }
            if (this.publisher.hasSequenceReservation()) {
                throw new IllegalStateException("a Store replication mark is already reserved");
            }
            final long recordingId = archivePublisher.recordingId();
            final long startPosition = archivePublisher.currentPosition();
            if (recordingId < 0L || startPosition < 0L) {
                throw new ReplicationUnavailableException("writer recording position is unavailable");
            }
            final long sequence = this.publisher.reserveSequence();
            this.markReservation = new MarkReservation(sequence, Thread.currentThread());
            mark.reserve(recordingId, this.publisher.fencingToken(), sequence, startPosition);
        } finally {
            this.writeLock.unlock();
        }
    }

    /// Releases a sequence reserved before Serializer ran when that Store
    /// commit never reached the target.
    public void cancelStoreCommit(final ReplicationMark mark) {
        Objects.requireNonNull(mark, "mark");
        this.lockBounded();
        try {
            if (this.markReservation == null ||
                this.markReservation.ownerThread() != Thread.currentThread() ||
                this.markReservation.sequence() != mark.sequence()) return;
            this.cancelMarkReservationLocked();
        } finally {
            this.writeLock.unlock();
        }
    }

    /// Publishes and commits one Store binary.
    ///
    /// This entry point is for the neutral publisher, which has no local
    /// persistence target. Store writes should use
    /// [AeronStorageBinaryReplicationTarget] so local acceptance and
    /// publication remain one operation.
    ///
    /// @param data binary to publish
    void distributeData(final Binary data) {
        final PreparedTransaction prepared =
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
    void commitOrMarkUncertain(final PreparedTransaction prepared) {
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
        this.finishCommit(failure, fatal);
        /* Throw only after every lock has been released. */
        if (fatal != null) throw fatal;
        if (failure != null) throw failure;
    }

    /// Completes a locally accepted Store commit without waiting for Archive recording.
    /// A bounded COMMIT offer failure keeps the graph valid and suspends admission.
    void commitAcceptedStore(final PreparedTransaction prepared) {
        if (this.writeLock.isHeldByCurrentThread()) {
            throw new IllegalStateException("Store COMMIT offer may not run under the coordinator lock");
        }
        this.beginCommit(prepared);
        final long position;
        try {
            position = this.publisher.offerStoreCommitMarker(prepared);
        } catch (final ReplicationUnavailableException unavailable) {
            final ReplicationPendingException pending =
                    new ReplicationPendingException(prepared.sequence(), unavailable);
            this.suspendCommit(pending);
            throw pending;
        } catch (final RuntimeException | Error failed) {
            this.finishCommit(failed instanceof RuntimeException runtime ? runtime : null,
                    failed instanceof Error error ? error : null);
            throw failed;
        }
        try {
            this.terminalRecorded.accept(position);
            prepared.invokeCommitAction();
        } catch (final RuntimeException | Error failed) {
            this.publisher.failClosed();
            this.finishCommit(failed instanceof RuntimeException runtime ? runtime : null,
                    failed instanceof Error error ? error : null);
            throw failed;
        }
        this.finishCommit(null, null);
    }

    /// Retries one suspended Store COMMIT offer from the node maintenance task.
    public void retryPendingCommit() {
        final PreparedTransaction prepared;
        this.lockBounded();
        try {
            if (this.activeWrite == null ||
                this.activeWrite.state() != PreparedWrite.State.REPLICATION_SUSPENDED) return;
            prepared = this.activeWrite.transaction();
            this.activeWrite = this.activeWrite.withState(PreparedWrite.State.COMMITTING, Thread.currentThread());
        } finally {
            this.writeLock.unlock();
        }

        final long position;
        try {
            position = this.publisher.offerStoreCommitMarker(prepared);
        } catch (final ReplicationUnavailableException unavailable) {
            this.suspendCommit((ReplicationPendingException) this.failure);
            return;
        } catch (final RuntimeException | Error failed) {
            this.finishCommit(failed instanceof RuntimeException runtime ? runtime : null,
                    failed instanceof Error error ? error : null);
            throw failed;
        }
        try {
            this.terminalRecorded.accept(position);
            prepared.invokeCommitAction();
        } catch (final RuntimeException | Error failed) {
            this.publisher.failClosed();
            this.finishCommit(failed instanceof RuntimeException runtime ? runtime : null,
                    failed instanceof Error error ? error : null);
            throw failed;
        }
        this.finishCommit(null, null);
    }

    private void suspendCommit(final ReplicationPendingException pending) {
        this.lockBounded();
        try {
            this.failure = pending;
            if (this.activeWrite != null) this.activeWrite = this.activeWrite.suspended();
            this.clearBufferScratch();
        } finally {
            this.writeLock.unlock();
        }
    }

    public RuntimeException failure() {
        return this.failure;
    }

    /// Sets the commit guard and pins the writer identity.
    ///
    /// Called with no lock held; the method holds {@code writeLock} only for
    /// the state change. Setting the guard before any slow wait is what makes
    /// a concurrent writer fail fast with {@code "Aeron commit is in progress"}
    /// instead of preparing against the same publisher.
    private void beginCommit(final PreparedTransaction prepared) {
        this.lockBounded();
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
                    throw new IllegalStateException("prepared transaction lost its writer fencing token");
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

    /// Clears the commit guard after terminal publication succeeds or fails.
    ///
    /// Called with no lock held after the slow phase. Fatal errors fail the
    /// publisher closed on any ambiguity; restart resolves the Store mark
    /// against the Archive tail.
    private void finishCommit(final RuntimeException failure, final Error fatal) {
        this.lockBounded();
        try {
            if (fatal != null || failure != null) this.publisher.failClosed();
            this.failure = failure;
            if (fatal != null) {
                this.failure = new ReplicationUnavailableException("Aeron commit failed with a fatal error", fatal);
            }
        } finally {
            this.clearActiveWrite();
            this.clearBufferScratch();
            this.writeLock.unlock();
        }
    }

    /// Publishes a transaction's prepare frames.
    PreparedTransaction prepare(final Binary data) {
        if (this.writeLock.isHeldByCurrentThread()) {
            return this.prepareLocked(data);
        }
        this.lockBounded();
        try {
            return this.prepareLocked(data);
        } finally {
            this.writeLock.unlock();
        }
    }

    private PreparedTransaction prepareLocked(final Binary data) {
        this.ensureNotCommitting();
        this.ensureWriterIdentity();
        Objects.requireNonNull(data, "data");
        if (this.publisher.hasPendingTransaction()) {
            throw new IllegalStateException("an Aeron prepared transaction is already pending");
        }
        final PreparedTransaction prepared;
        /* Reject re-entry before recollecting into the reusable array. */
        final MarkReservation reservedMark = this.markReservation;
        if (this.publisher.hasSequenceReservation() &&
            (reservedMark == null || reservedMark.ownerThread() != Thread.currentThread())) {
            throw new IllegalStateException("cannot re-enter Aeron preparation while a sequence is reserved");
        }
        final int bufferCount = this.collectBuffers(data);
        final ByteBuffer[] buffers = this.bufferScratch;
        final byte[] dictionary = this.retryDictionary;
        final int dataLength;
        long sequence;
        /* Reserve the sequence before publication so recovery can reconcile the
         * Archive tail against the Store mark. */
        try {
            dataLength = this.publisher.dataLength(buffers, bufferCount);
        } catch (final RuntimeException | Error failure) {
            this.clearBufferScratch();
            this.failClosedUnlessRejected(failure);
            throw failure;
        }
        try {
            this.ensureWriteAdmitted(dataLength, dictionary);
        } catch (final RuntimeException | Error admissionFailure) {
            this.clearBufferScratch();
            throw admissionFailure;
        }
        try {
            sequence = reservedMark == null ? this.publisher.reserveSequence() : reservedMark.sequence();
            this.activeWrite = new PreparedWrite(sequence, dictionary, buffers, bufferCount,
                    dataLength, this.publisher.fencingToken(), null, PreparedWrite.State.RESERVED,
                    Thread.currentThread());
            if (reservedMark != null) this.markReservation = null;
            this.retryDictionary = null;
        } catch (final RuntimeException | Error failure) {
            this.clearActiveWrite();
            this.clearBufferScratch();
            this.publisher.failClosed();
            throw failure;
        }
        try {
            prepared = this.publisher.prepareTransaction(
                    this.activeWrite.dictionary(), this.activeWrite.buffers(),
                    this.activeWrite.bufferCount(), this.activeWrite.sequence());
        } catch (final RuntimeException | Error failure) {
            final PreparedWrite rejected = this.activeWrite;
            if (failure instanceof WriteRejectedException rejection && rejected != null) {
                try {
                    if (rejection.recordedAbortPosition() >= 0L) {
                        this.terminalRecorded.accept(rejection.recordedAbortPosition());
                    }
                } catch (final RuntimeException | Error boundaryFailure) {
                    this.publisher.failClosed();
                    boundaryFailure.addSuppressed(failure);
                    this.clearActiveWrite();
                    this.clearBufferScratch();
                    throw boundaryFailure;
                }
                this.retryDictionary = rejected.dictionary();
            }
            this.clearActiveWrite();
            this.clearBufferScratch();
            this.failClosedUnlessRejected(failure);
            throw failure;
        }
        try {
            this.activeWrite = this.activeWrite.withTransaction(prepared);
            if (prepared.sequence() != this.activeWrite.sequence() ||
                prepared.dataLength() != this.activeWrite.dataLength() ||
                this.publisher.fencingToken() != this.activeWrite.fencingToken()) {
                throw new IllegalStateException("prepared transaction changed its reserved identity");
            }
            prepared.onAbort(abortPosition ->
            {
                try {
                    if (abortPosition >= 0L) this.terminalRecorded.accept(abortPosition);
                    else this.publisher.failClosed();
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
        final long dictionaryLength = dictionary == null ? 0L : dictionary.length;
        final long requiredBytes;
        try {
            requiredBytes = Math.addExact(dataLength, dictionaryLength);
        } catch (final ArithmeticException overflow) {
            throw new WriteRejectedException("Store transaction exceeds maxTransactionBytes", overflow);
        }
        if (requiredBytes > this.publisher.maxTransactionBytes()) {
            throw new WriteRejectedException("Store transaction exceeds maxTransactionBytes");
        }
        if (!this.writeAdmission.test(requiredBytes)) {
            throw new WriteRejectedException(
                    "Aeron Archive has insufficient free capacity for transaction bytes=%s".formatted(requiredBytes));
        }
    }

    /// Applies the write admission checks before Archive maintenance.
    private void ensureMaintenanceAdmitted() {
        this.ensureWriteAdmitted(0, null);
    }

    /// Returns whether the publisher can still accept a transaction.
    boolean isWritable() {
        return this.failure == null && !this.publisher.isFailed() && !this.publisher.isClosed();
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
            throw new ReplicationUnavailableException("Aeron Archive maintenance is already in progress");
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

    /// Takes the coordinator lock with the recorded-position deadline instead of waiting forever.
    ///
    /// A prepare holds this lock while it waits for the Archive, so an unbounded wait here would
    /// park dispose, cancellation and retry behind a slow or dead Archive.
    private void lockBounded() {
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
    }

    private void lockWriteAdmission() {
        if (this.maintenance.get()) {
            throw new WriteRejectedException("Aeron Archive maintenance is in progress; write admission is closed");
        }
        final long deadline = ReplicationRetry.deadlineNanos(this.publisher.admissionTimeoutNanos());
        try {
            if (!this.writeLock.tryLock(ReplicationRetry.remainingNanos(deadline), TimeUnit.NANOSECONDS)) {
                throw new WriteRejectedException(
                        "timed out waiting for Aeron write admission", null);
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new WriteRejectedException(
                    "interrupted while waiting for Aeron write admission", interrupted);
        }
        boolean admitted = false;
        try {
            while (this.activeWrite != null || this.markReservation != null) {
                if (this.activeWrite != null &&
                    this.activeWrite.state() == PreparedWrite.State.REPLICATION_SUSPENDED) {
                    throw this.suspendedRejection();
                }
                if (this.activeWrite == null && this.markReservation != null &&
                    this.markReservation.ownerThread() == Thread.currentThread()) {
                    admitted = true;
                    break;
                }
                if (this.activeWrite != null && this.activeWrite.ownerThread() == Thread.currentThread()) {
                    throw new IllegalStateException("cannot re-enter an active Aeron write");
                }
                if (this.maintenance.get()) {
                    throw new WriteRejectedException("Aeron Archive maintenance is in progress; write admission is closed");
                }
                final long remaining = ReplicationRetry.remainingNanos(deadline);
                if (remaining == 0L) {
                    throw new WriteRejectedException("timed out waiting for Aeron write admission", null);
                }
                try {
                    this.writeDone.await(remaining, TimeUnit.NANOSECONDS);
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new WriteRejectedException(
                            "interrupted while waiting for Aeron write admission", interrupted);
                }
            }
            if (this.maintenance.get()) {
                throw new WriteRejectedException("Aeron Archive maintenance is in progress; write admission is closed");
            }
            admitted = true;
        } finally {
            if (!admitted) this.writeLock.unlock();
        }
    }

    /// Offers the commit marker and waits for the durability boundary.
    ///
    /// Called only by [commitOrMarkUncertain] with no coordinator lock held.
    private void performCommit(final PreparedTransaction prepared) {
        FaultInjection.invoke(FaultInjection.Point.BEFORE_COMMIT_GATE, prepared.sequence());
        final long commitPosition = this.publisher.offerCommitMarker(prepared);
        final long position = this.publisher.awaitCommitPosition(prepared, commitPosition);
        this.lockBounded();
        try {
            FaultInjection.invoke(FaultInjection.Point.AFTER_COMMIT_RECORDED_BEFORE_BOUNDARY_UPDATE, prepared.sequence());
            this.terminalRecorded.accept(position);
        } catch (final RuntimeException | Error failure) {
            this.publisher.failClosed();
            throw failure;
        } finally {
            this.writeLock.unlock();
        }
    }

    void abort(final PreparedTransaction prepared) {
        this.lockBounded();
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
            /* prepare() registers the terminal callback on the token so committed
             * aborts advance the same in-memory boundary as commits. */
            this.publisher.abort(prepared);
            rejected = true;
        } catch (final RuntimeException | Error failure) {
            this.publisher.failClosed();
            throw failure;
        } finally {
            this.lockBounded();
            try {
                if (this.activeWrite != null) {
                    if (rejected && this.activeWrite.dictionary() != null) {
                        this.retryDictionary = this.activeWrite.dictionary();
                    }
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

    private void failClosedUnlessRejected(final Throwable failure) {
        if (!(failure instanceof WriteRejectedException)) this.publisher.failClosed();
    }

    /// Fails closed when local Store acceptance made the transaction uncertain.
    ///
    /// Called from the Store write path while it still holds the single
    /// admission hold and balances that hold in its own finally block.
    void markStoreOutcomeUncertain(final PreparedTransaction prepared) {
        if (!this.writeLock.isHeldByCurrentThread()) {
            throw new IllegalStateException(
                    "markStoreOutcomeUncertain requires the caller's Aeron write-admission hold");
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
            this.publisher.failClosed();
        } finally {
            if (owned) this.clearActiveWrite();
            if (owned) this.clearBufferScratch();
        }
    }

    private void clearActiveWrite() {
        if (this.activeWrite != null) {
            this.activeWrite = null;
            this.writeDone.signalAll();
        }
    }

    private void ensureNotCommitting() {
        if (this.activeWrite != null ||
            (this.markReservation != null && this.markReservation.ownerThread() != Thread.currentThread())) {
            if (this.activeWrite != null &&
                this.activeWrite.state() == PreparedWrite.State.REPLICATION_SUSPENDED) {
                throw this.suspendedRejection();
            }
            throw new IllegalStateException("Aeron commit is in progress");
        }
    }

    private WriteRejectedException suspendedRejection() {
        final RuntimeException pending = this.failure;
        return new WriteRejectedException(pending == null
                ? "replication suspended: a local Store COMMIT is pending"
                : "replication suspended: %s".formatted(pending.getMessage()));
    }

    private void cancelMarkReservationLocked() {
        final MarkReservation reservation = this.markReservation;
        if (reservation == null) return;
        if (reservation.ownerThread() != Thread.currentThread()) {
            throw new IllegalStateException("only the Store commit owner may cancel its replication mark");
        }
        if (this.publisher.hasSequenceReservation()) {
            this.publisher.releaseReservedSequence(reservation.sequence());
        }
        this.markReservation = null;
        this.writeDone.signalAll();
    }

    /// Waits, bounded, for an in-flight commit to finish.
    ///
    /// Shutdown and Archive maintenance must not fail merely because a commit
    /// released the write lock and is still awaiting its Archive position; the
    /// wait is bounded by the recorded-position timeout so a stuck commit
    /// cannot hang shutdown forever. Call with the write lock held.
    private void awaitNoCommit() {
        if (this.activeWrite == null && this.markReservation == null) {
            return;
        }
        final long deadline = ReplicationRetry.deadlineNanos(this.publisher.recordedPositionTimeoutNanos());
        while (this.activeWrite != null || this.markReservation != null) {
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
        this.lockBounded();
        try {
            this.disposeLocked();
        } finally {
            this.writeLock.unlock();
        }
    }

    private void disposeLocked() {
        if (this.activeWrite != null &&
            this.activeWrite.state() == PreparedWrite.State.REPLICATION_SUSPENDED) {
            /* Maintenance is stopped before transport teardown. Leave the Store
             * mark as recovery evidence and close without emitting a contradictory
             * ABORT; startup will append the missing COMMIT if needed. */
            this.clearActiveWrite();
            this.clearBufferScratch();
        } else {
            this.awaitNoCommit();
        }
        /* Keep the coordinator claim until publication shutdown has completed.  If an
         * abort/close offer is transiently unavailable, releasing first would allow a
         * second coordinator to claim the same publisher while this one still owns a
         * pending sequence. */
        this.publisher.close();
        this.publisher.releaseCoordinator(this);
        this.retryDictionary = null;
        this.clearBufferScratch();
    }
}
