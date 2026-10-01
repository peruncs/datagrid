package peruncs.cluster.storage.aeron.writer;

import io.aeron.Aeron;
import io.aeron.DirectBufferVector;
import io.aeron.ExclusivePublication;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.errors.WriteRejectedException;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.cluster.storage.binary.NativeMemory;
import peruncs.cluster.storage.io.FaultInjection;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/// Publishes one ordered transaction at a time.
///
/// Data chunks are prepared first. A commit marker makes the complete
/// transaction visible; an abort marker closes a rejected or abandoned one.
/// Small payloads use one reusable direct staging buffer; full chunks can be offered
/// as vectors without a staging copy. Publication ownership prevents overlapping writes.
final class AeronReplicationPublisher implements AutoCloseable {
    /// Waits for the Archive to record a publication position.
    @FunctionalInterface
    interface PositionAwaiter {
        /// Blocks until the position is recorded.
        ///
        /// @param position     publication position to wait for
        /// @param timeoutNanos longest wait
        /// @return the recorded position, at least `position`
        long await(long position, long timeoutNanos);
    }

    /// Everything one publisher is created from.
    record Configuration(AeronOfferRetryer.Offerer offerer, int maxMessageLength,
                                 AeronReplicationConfiguration replication, UUID clusterId,
                                 long epoch, long initialSequence, AutoCloseable closeAction,
                                 PositionAwaiter commitPositionAwaiter, long wireNonce) {
        Configuration {
            Objects.requireNonNull(offerer, "offerer");
            Objects.requireNonNull(replication, "replication");
            Objects.requireNonNull(clusterId, "clusterId");
            Objects.requireNonNull(commitPositionAwaiter, "commitPositionAwaiter");
            if (initialSequence < 0 || initialSequence == Long.MAX_VALUE || maxMessageLength <= 0 ||
                maxMessageLength > replication.maxMessageLength() ||
                (long) replication.chunkSize() + AeronReplicationEnvelope.HEADER_LENGTH > maxMessageLength ||
                wireNonce == 0L) {
                throw new IllegalArgumentException("invalid publisher configuration");
            }
        }
    }

    private volatile AeronOfferRetryer offerer;
    private volatile int maxMessageLength;
    private final AeronReplicationConfiguration configuration;
    private final UUID clusterId;
    private final long wireNonce;
    private final long epoch;
    /* Fencing token selected from the Store mark before recovery starts. */
    private volatile long fencingToken = 1L;
    private boolean fencingTokenClaimed;
    private volatile AutoCloseable closeAction;
    private volatile PositionAwaiter commitPositionAwaiter;
    private final NativeMemory.Allocation framingAllocation;
    private final ByteBuffer framingStorage;
    private final EnvelopeFramer.GatherScratch gatherScratch = new EnvelopeFramer.GatherScratch();
    private final EnvelopeFramer.Configuration framerConfiguration;
    /* transactionMetadata is synchronized, so its CRC accumulator is confined
     * to one publisher call at a time and can be reset for each transaction. */
    /* All sequence operations are protected by this publisher's monitor. Keeping
     * the value primitive avoids an allocation and makes the ownership rule
     * explicit instead of implying lock-free access. */
    private long nextSequence;
    /* A reservation is an ownership token, not merely a value derived from
     * nextSequence. Keeping it explicitly prevents an unrelated caller from
     * bypassing the Store-mark reservation by reusing the incremented value. */
    private long reservedSequence = -1L;
     PreparedTransaction pendingTransaction;
    private Object coordinatorOwner;
     volatile boolean failed;
    /// Where the publisher is in its life.
    enum Lifecycle {
        /// Accepting transactions.
        OPEN,
        /// A close is running.
        CLOSING,
        /// A close ended without completing; no transaction is accepted, and a later close may retry.
        CLOSE_INTERRUPTED,
        /// Closed for good.
        CLOSED
    }

    /// The one operation that may run at a time.
    enum Operation {
        /// Nothing is running.
        IDLE,
        /// A transaction is being prepared: its frames are being offered.
        PREPARING,
        /// A prepared transaction is being committed or aborted.
        TERMINAL
    }

    Lifecycle lifecycle = Lifecycle.OPEN;
    private Operation operation = Operation.IDLE;

    /// Creates a publisher for a production Aeron publication.
    ///
    /// @param publication          exclusive publication owned by the publisher
    /// @param configuration        framing, retry, and timeout limits
    /// @param clusterId            replication cluster identity
    /// @param epoch                writer epoch bound to the Store mark
    /// @param initialSequence      first sequence to publish
    /// @param commitPositionAwaiter waits for the Archive to record a position
    /// @param wireNonce            public cluster-id-derived framing value, not a credential
    /// @return publisher that closes the publication on close
    static AeronReplicationPublisher onPublication(final ExclusivePublication publication,
                                                   final AeronReplicationConfiguration configuration, final UUID clusterId,
                                                   final long epoch, final long initialSequence,
                                                   final PositionAwaiter commitPositionAwaiter, final long wireNonce) {
        return new AeronReplicationPublisher(new Configuration(
                offerer(publication), actualMaxMessageLength(publication, configuration),
                configuration, clusterId, epoch, initialSequence, publication, commitPositionAwaiter, wireNonce));
    }

    AeronReplicationPublisher(final Configuration settings) {
        this.offerer = new AeronOfferRetryer(settings.offerer(), settings.replication());
        this.maxMessageLength = settings.maxMessageLength();
        this.configuration = settings.replication();
        this.clusterId = settings.clusterId();
        this.wireNonce = settings.wireNonce();
        this.epoch = settings.epoch();
        this.nextSequence = settings.initialSequence();
        this.closeAction = settings.closeAction();
        this.commitPositionAwaiter = settings.commitPositionAwaiter();
        this.framingAllocation = NativeMemory.allocate(
                settings.replication().chunkSize() + AeronReplicationEnvelope.HEADER_LENGTH);
        this.framingStorage = this.framingAllocation.buffer();
        this.framerConfiguration = new EnvelopeFramer.Configuration(
                settings.clusterId(), settings.epoch(), settings.wireNonce(),
                settings.replication().chunkSize(), this.framingStorage, this.gatherScratch);
    }

    private static AeronOfferRetryer.Offerer offerer(final ExclusivePublication publication) {
        return new AeronOfferRetryer.Offerer() {
            @Override
            public long offer(final DirectBuffer buffer, final int offset, final int length) {
                return publication.offer(buffer, offset, length);
            }

            @Override
            public boolean isConnected() {
                return publication.isConnected();
            }

            @Override
            public long offer(final DirectBufferVector[] vectors) {
                return publication.offer(vectors);
            }
        };
    }

    /// Rebinds an idle publisher after its Archive recording is extended.
    synchronized void rebindPublication(final ExclusivePublication publication,
                                        final PositionAwaiter commitPositionAwaiter) {
        Objects.requireNonNull(publication, "publication");
        Objects.requireNonNull(commitPositionAwaiter, "commitPositionAwaiter");
        this.ensureOpen();
        if (this.pendingTransaction != null || this.operation != Operation.IDLE) {
            throw new IllegalStateException("cannot replace Aeron publication while a transaction is active");
        }
        this.maxMessageLength = actualMaxMessageLength(publication, this.configuration);
        this.offerer = new AeronOfferRetryer(offerer(publication), this.configuration);
        this.closeAction = publication;
        this.commitPositionAwaiter = commitPositionAwaiter;
    }

    private static int actualMaxMessageLength(final ExclusivePublication publication,
                                              final AeronReplicationConfiguration configuration) {
        Objects.requireNonNull(publication, "publication");
        Objects.requireNonNull(configuration, "configuration");
        final int actual = publication.maxMessageLength();
        if (actual <= 0) {
            throw new IllegalArgumentException("Aeron publication has no usable message capacity");
        }
        return Math.min(actual, configuration.maxMessageLength());
    }

    private static long totalRemaining(final ByteBuffer[] buffers, final int count) {
        Objects.requireNonNull(buffers, "dataBuffers");
        if (count < 0 || count > buffers.length) throw new IllegalArgumentException("invalid data buffer count");
        long length = 0;
        for (int index = 0; index < count; index++) {
            final ByteBuffer buffer = buffers[index];
            Objects.requireNonNull(buffer, "dataBuffers contains null");
            length = Math.addExact(length, buffer.remaining());
        }
        return length;
    }

    /// Publishes one transaction and its terminal commit marker.
    long publishTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers) {
        this.ensureNoSequenceReservation();
        return this.commit(this.prepareTransaction(dictionary, dataBuffers, dataBuffers == null ? 0 : dataBuffers.length));
    }

    /// Appends a terminal marker discovered by Store-mark recovery.
    ///
    /// Recovery markers keep the Store mark's token so another restart can
    /// recognize them if recovery is interrupted before the next Store commit.
    long appendRecoveryMarker(final long sequence, final AeronReplicationEnvelope.Kind kind,
                              final int payloadLength, final int dataChunkCount,
                              final int dataCrc32c, final long markToken) {
        if (kind != AeronReplicationEnvelope.Kind.COMMIT && kind != AeronReplicationEnvelope.Kind.ABORT) {
            throw new IllegalArgumentException("recovery may append only a terminal marker");
        }
        synchronized (this) {
            this.ensureOpen();
            if (this.pendingTransaction != null || this.operation != Operation.IDLE ||
                this.reservedSequence >= 0L || this.coordinatorOwner != null) {
                throw new IllegalStateException("recovery marker requires an idle publisher");
            }
        }
        if (markToken <= 0L) throw new IllegalArgumentException("recovery marker requires a positive mark token");
        final EnvelopeFramer framer = new EnvelopeFramer(
                sequence, this.framerConfiguration, markToken, this.maxMessageLength, this.offerer);
        try {
            final long offered = framer.offerMarker(kind, payloadLength, dataChunkCount, dataCrc32c);
            return this.commitPositionAwaiter.await(offered,
                    this.configuration.recordedPositionTimeoutNanos());
        } catch (final RuntimeException | Error failure) {
            this.failClosed();
            throw failure;
        } finally {
            framer.close();
        }
    }

    /// Publishes dictionary and Store-data chunks and returns a token whose commit
    /// marker is pending. The caller must commit or close the token. If publishing
    /// fails after a sequence is reserved, the publisher attempts an abort and
    /// then fails closed so a later write cannot skip the damaged sequence.
    /// This low-level overload is reserved for direct publisher users; coordinator
    /// writes must use the explicit-sequence overload so their Store-mark reservation and
    /// publication cannot diverge.
    ///
    /// @param dictionary  optional type dictionary bytes
    /// @param dataBuffers Store binary buffers; positions are not changed
    /// @return a token that must be committed or closed
    PreparedTransaction prepareTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers) {
        return this.prepareTransaction(dictionary, dataBuffers, dataBuffers == null ? 0 : dataBuffers.length);
    }

    /// Prepares a transaction using only the populated prefix of a reusable buffer array.
    private PreparedTransaction prepareTransaction(final byte[] dictionary,
                                                   final ByteBuffer[] dataBuffers, final int bufferCount) {
        final long sequence;
        final int dataLength;
        final int dataChunks;
        synchronized (this) {
            this.ensureOpen();
            this.ensureNoSequenceReservation();
            if (this.coordinatorOwner != null) {
                throw new IllegalStateException(
                        "coordinator-owned publisher requires the explicit reserved-sequence preparation path");
            }
            this.ensureNoPendingTransaction();
            FaultInjection.invoke(FaultInjection.Point.BEFORE_PREPARE, this.nextSequence);
            final int dictionaryLength = dictionary == null ? 0 : dictionary.length;
            final long totalDataLength = totalRemaining(dataBuffers, bufferCount);
            if (totalDataLength > (long) this.configuration.maxTransactionBytes() - dictionaryLength) {
                throw new WriteRejectedException("Store transaction exceeds maxTransactionBytes");
            }
            dataLength = (int) totalDataLength;
            sequence = this.nextSequence;
            if (sequence < 0 || sequence == Long.MAX_VALUE) {
                throw new IllegalStateException("Aeron replication sequence space is exhausted");
            }
            this.nextSequence = sequence + 1;
            dataChunks = chunkCount(dataLength);
            this.operation = Operation.PREPARING;
        }
        return this.prepareWithRecovery(dictionary, dataBuffers, bufferCount, sequence, dataLength, dataChunks);
    }

    /// Completes preparation for a sequence reserved before the Store serialized its mark.
    /// The explicit sequence prevents a crash between the mark write and publication
    /// from leaving two different sequence numbers in the log. This explicit reservation
    /// is part of the Store-mark contract and must not be replaced with an independent
    /// sequence allocation.
    ///
    /// @param dictionary       optional type dictionary bytes
    /// @param dataBuffers      Store binary buffers whose positions are not changed
    /// @param bufferCount      number of populated buffers
    /// @param reservedSequence sequence returned by [#reserveSequence()]
    /// @return a token that must be committed or closed
    /// @throws IllegalStateException if the reservation is no longer current
    PreparedTransaction prepareTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers,
                                           final int bufferCount, final long reservedSequence) {
        final int dataLength;
        synchronized (this) {
            this.ensureOpen();
            this.ensureNoPendingTransaction();
            if (reservedSequence < 0 || reservedSequence == Long.MAX_VALUE ||
                this.reservedSequence != reservedSequence || this.nextSequence != reservedSequence + 1) {
                throw new IllegalStateException("reserved replication sequence is no longer current");
            }
            final int dictionaryLength = dictionary == null ? 0 : dictionary.length;
            final long totalDataLength = totalRemaining(dataBuffers, bufferCount);
            if (totalDataLength > (long) this.configuration.maxTransactionBytes() - dictionaryLength) {
                throw new WriteRejectedException("Store transaction exceeds maxTransactionBytes");
            }
            dataLength = (int) totalDataLength;
            FaultInjection.invoke(FaultInjection.Point.BEFORE_PREPARE, reservedSequence);
            this.operation = Operation.PREPARING;
        }
        return this.prepareWithRecovery(dictionary, dataBuffers, bufferCount, reservedSequence,
                dataLength, this.chunkCount(dataLength));
    }

    private PreparedTransaction prepareWithRecovery(final byte[] dictionary, final ByteBuffer[] dataBuffers,
                                                     final int bufferCount, final long sequence, final int dataLength,
                                                     final int dataChunks) {
        EnvelopeFramer framer = null;
        boolean handedOff = false;
        try {
            framer = new EnvelopeFramer(
                    sequence, this.framerConfiguration, this.fencingToken,
                    this.maxMessageLength, this.offerer);
            final PreparedTransaction prepared = this.prepareReserved(dictionary, dataBuffers, bufferCount, sequence,
                    dataLength, dataChunks, framer);
            synchronized (this) {
                this.operation = Operation.IDLE;
            }
            handedOff = true;
            return prepared;
        } catch (final Error failure) {
            /* Do not allocate, compute a checksum, or offer a compensating marker
             * while the JVM is already in a fatal Error path.  The Store mark (when
             * one exists) remains unresolved and therefore forces fail-closed recovery. */
            final PreparedTransaction pending;
            synchronized (this) {
                pending = this.pendingTransaction;
            }
            this.failPendingTransaction(pending, sequence);
            throw failure;
        } catch (final RuntimeException failure) {
            /* AFTER_PREPARE can throw after the token has become the pending
             * transaction. The recovery abort below is the terminal decision for
             * that token; clear the owner reference even when the abort offer fails
             * so close() cannot emit a second terminal marker. */
            final PreparedTransaction pending;
            synchronized (this) {
                pending = this.pendingTransaction;
            }
            long abortPosition = -1L;
            boolean abortRecorded = false;
            try {
                // Clear any transaction prefix that reached the log. If the publication
                // itself is gone this fails as well, and recovery must retain the tail.
                if (framer != null) {
                    final long offeredPosition = framer.offerMarker(
                            AeronReplicationEnvelope.Kind.ABORT, dataLength, dataChunks, 0);
                    abortPosition = this.commitPositionAwaiter.await(offeredPosition,
                            Math.min(this.configuration.abortRecordedPositionTimeoutNanos(),
                                    this.configuration.recordedPositionTimeoutNanos()));
                    abortRecorded = true;
                }
            } catch (final RuntimeException abortFailure) {
                failure.addSuppressed(abortFailure);
            } catch (final Error abortFailure) {
                this.failPendingTransaction(pending, sequence);
                abortFailure.addSuppressed(failure);
                throw abortFailure;
            }
            if (abortRecorded) {
                try {
                    FaultInjection.invoke(FaultInjection.Point.AFTER_PREPARE_FAILURE_ABORT_OFFERED, sequence);
                    if (pending != null) pending.invokeAbortAction(abortPosition);
                } catch (final RuntimeException | Error cleanupFailure) {
                    this.failPendingTransaction(pending, sequence);
                    cleanupFailure.addSuppressed(failure);
                    throw cleanupFailure;
                }
                this.detachPending(pending, sequence, false);
                throw WriteRejectedException.afterRecordedAbort(
                        "Aeron prepare failed and its ABORT was recorded", failure, abortPosition);
            }
            try {
                FaultInjection.invoke(FaultInjection.Point.AFTER_PREPARE_FAILURE_ABORT_OFFERED, sequence);
            } catch (final RuntimeException | Error hookFailure) {
                failure.addSuppressed(hookFailure);
            }
            this.failPendingTransaction(pending, sequence);
            throw failure;
        } finally {
            if (!handedOff && framer != null) framer.close();
        }
    }

    /// Terminates a failed preparation: the pending transaction (when one
    /// exists) is marked terminal so close() cannot emit a second terminal
    /// marker, the reserved sequence is released, and the publisher fails
    /// closed for the rest of the process.
    private void failPendingTransaction(final PreparedTransaction pending, final long sequence) {
        this.detachPending(pending, sequence, true);
    }

    /// Detaches a finished preparation and optionally latches the publisher.
    private void detachPending(
            final PreparedTransaction pending, final long sequence, final boolean failClosed) {
        synchronized (this) {
            if (pending != null) {
                pending.terminal = true;
                this.pendingTransaction = null;
            }
            if (this.reservedSequence == sequence) this.reservedSequence = -1L;
            this.operation = Operation.IDLE;
            if (failClosed) this.failed = true;
        }
    }

    private PreparedTransaction prepareReserved(final byte[] dictionary, final ByteBuffer[] dataBuffers,
                                                final int bufferCount, final long sequence, final int dataLength,
                                                final int dataChunks, final EnvelopeFramer framer) {
        if (dictionary != null && dictionary.length != 0) {
            framer.offerDictionaryChunks(new UnsafeBuffer(dictionary), dictionary.length);
            FaultInjection.invoke(FaultInjection.Point.AFTER_DICTIONARY_CHUNKS, sequence);
        }
        final int dataCrc32c = framer.offerDataChunks(dataBuffers, bufferCount, dataLength);
        /* The local Store write may start only after every prepare frame is
         * durably recorded. The Store mark then lets restart recovery finish a
         * missing COMMIT without ever having to guess whether its payload exists. */
        this.commitPositionAwaiter.await(framer.lastOfferPosition(),
                    this.configuration.recordedPositionTimeoutNanos());
        FaultInjection.invoke(FaultInjection.Point.AFTER_DATA_CHUNKS, sequence);
        final PreparedTransaction prepared = new PreparedTransaction(this, sequence, dataLength, dataChunks,
                dataCrc32c, framer);
        synchronized (this) {
            this.pendingTransaction = prepared;
            if (this.reservedSequence == sequence) this.reservedSequence = -1L;
        }
        FaultInjection.invoke(FaultInjection.Point.AFTER_PREPARE, sequence);
        return prepared;
    }

    /// Returns the next unreserved sequence for diagnostics and recovery checks.
    /// This method does not reserve the value; use [#reserveSequence()] when
    /// a Store-mark reservation must carry the same sequence as a later publication.
    synchronized long nextSequence() {
        return this.nextSequence;
    }

    UUID clusterId() {
        return this.clusterId;
    }

    /// Returns the writer epoch bound to this publisher at construction.
    long epoch() {
        return this.epoch;
    }

    /// Installs the next token derived from the persisted Store mark.
    ///
    /// Every envelope offered afterwards carries this token; readers reject
    /// frames from a deposed writer whose token is lower. The claim happens
    /// before the write coordinator admits its first transaction. The claim is
    /// single-shot so a publication cannot mix token series.
    ///
    /// @param fencingToken positive token
    /// @throws IllegalArgumentException when the token is not positive
    /// @throws IllegalStateException when a different token was already claimed
    void claimFencingToken(final long fencingToken) {
        if (fencingToken <= 0) {
            throw new IllegalArgumentException("writer fencing token must be positive");
        }
        synchronized (this) {
            if (this.fencingTokenClaimed) {
                if (this.fencingToken != fencingToken) {
                    this.failed = true;
                    throw new IllegalStateException(
                            "writer fencing token %s was already claimed; cannot replace it with %s".formatted(
                                    this.fencingToken, fencingToken));
                }
                return;
            }
            this.fencingToken = fencingToken;
            this.fencingTokenClaimed = true;
        }
    }

    /// Returns the fencing token carried by offered envelopes.
    long fencingToken() {
        return this.fencingToken;
    }

    /// Reserves the next sequence so a Store-mark reservation and its later publication
    /// share one sequence number. The reservation must either be used by the
    /// explicit-sequence preparation method or released after a local rejection.
    /// A caller must not publish another transaction while this reservation is
    /// outstanding.
    ///
    /// @return the sequence reserved for the next explicit-sequence preparation
    synchronized long reserveSequence() {
        this.ensureOpen();
        this.ensureNoPendingTransaction();
        if (this.reservedSequence != -1L) {
            throw new IllegalStateException("an Aeron sequence reservation is already outstanding");
        }
        final long sequence = this.nextSequence;
        if (sequence < 0 || sequence == Long.MAX_VALUE) {
            throw new IllegalStateException("Aeron replication sequence space is exhausted");
        }
        this.nextSequence = sequence + 1;
        this.reservedSequence = sequence;
        return sequence;
    }

    /// Releases a reservation when the local Store rejects the reserved write.
    synchronized void releaseReservedSequence(final long sequence) {
        if (sequence < 0 || sequence == Long.MAX_VALUE || this.reservedSequence != sequence ||
            this.nextSequence != sequence + 1) {
            throw new IllegalStateException("replication sequence reservation is no longer current");
        }
        this.nextSequence = sequence;
        this.reservedSequence = -1L;
    }

    /// Returns whether a Store-mark reservation currently owns the next sequence.
    synchronized boolean hasSequenceReservation() {
        return this.reservedSequence != -1L;
    }

    /// Measures the populated prefix of a reusable buffer array.
    ///
    /// @param dataBuffers Store binary buffers
    /// @param bufferCount number of populated buffers
    /// @return total remaining bytes
    /// @throws WriteRejectedException when the data alone exceeds `maxTransactionBytes`
    synchronized int dataLength(final ByteBuffer[] dataBuffers, final int bufferCount) {
        final long length = totalRemaining(dataBuffers, bufferCount);
        if (length > this.configuration.maxTransactionBytes()) {
            throw new WriteRejectedException("Store transaction exceeds maxTransactionBytes");
        }
        return (int) length;
    }

    /// Returns the maximum combined dictionary and Store-binary size.
    synchronized int maxTransactionBytes() {
        return this.configuration.maxTransactionBytes();
    }

    long admissionTimeoutNanos() {
        return this.configuration.offerTimeoutNanos();
    }

    /// Returns the bounded wait for one recorded-position acknowledgement.
    ///
    /// Coordinator shutdown paths use it to bound their wait for an in-flight
    /// commit instead of failing fast while a commit is still waiting.
    long recordedPositionTimeoutNanos() {
        return this.configuration.recordedPositionTimeoutNanos();
    }

    /// Marks one transaction terminal and releases the publisher for the next one.
    ///
    /// @param transaction terminal transaction
    /// @param failed whether the publisher itself is now untrustworthy
    private void finishTerminal(final PreparedTransaction transaction, final boolean failed) {
        synchronized (this) {
            transaction.terminal = true;
            this.pendingTransaction = null;
            this.operation = Operation.IDLE;
            if (failed) this.failed = true;
        }
        transaction.framer.close();
    }

    /// Enters the single terminal-marker section for one prepared transaction.
    ///
    /// @param transaction prepared transaction
    private void beginTerminal(final PreparedTransaction transaction) {
        synchronized (this) {
            this.ensureOpen();
            this.validate(transaction);
            if (transaction.terminal) throw new IllegalStateException("prepared transaction is already terminal");
            if (this.operation == Operation.TERMINAL) throw new IllegalStateException("publisher terminal operation is already running");
            this.operation = Operation.TERMINAL;
        }
    }

    /// Publishes the commit marker and waits for the configured durability boundary.
    ///
    /// Once the marker is offered it may still be delivered or recorded, so
    /// the acknowledgement wait fails closed instead of publishing an abort
    /// that would create two terminal markers for one sequence.
    long commit(final PreparedTransaction transaction) {
        return this.awaitCommitPosition(transaction, this.offerCommitMarker(transaction));
    }

    /// Offers the commit marker without waiting for durability.
    ///
    /// Back-pressure waits and Archive acknowledgement hold no publisher lock.
    ///
    /// @param transaction prepared transaction
    /// @return Aeron publication position of the offered marker
    long offerCommitMarker(final PreparedTransaction transaction) {
        return this.offerCommitMarker(transaction, false);
    }

    /// Offers a Store-backed COMMIT without waiting for Archive recording.
    /// A bounded offer failure leaves the prepared token open for retry.
    long offerStoreCommitMarker(final PreparedTransaction transaction) {
        return this.offerCommitMarker(transaction, true);
    }

    private long offerCommitMarker(final PreparedTransaction transaction, final boolean retryable) {
        this.beginTerminal(transaction);
        /* No publisher lock is held across back-pressure retries. The
         * prepared transaction owns its frame buffer until it is terminal. */
        try {
            FaultInjection.invoke(FaultInjection.Point.BEFORE_COMMIT_OFFER, transaction.sequence);
            final long position = transaction.framer.offerMarker(AeronReplicationEnvelope.Kind.COMMIT,
                    transaction.dataLength, transaction.dataChunkCount, transaction.crc32c);
            if (retryable) this.finishTerminal(transaction, false);
            return position;
        } catch (final ReplicationUnavailableException unavailable) {
            if (retryable) {
                synchronized (this) {
                    transaction.commitPending = true;
                    this.operation = Operation.IDLE;
                }
            } else {
                this.finishTerminal(transaction, true);
            }
            throw unavailable;
        } catch (final RuntimeException | Error failure) {
            this.finishTerminal(transaction, true);
            throw failure;
        }
    }

    /// Waits for durability of a previously offered commit marker.
    ///
    /// @param transaction    prepared transaction whose marker was offered
    /// @param commitPosition Aeron position returned by [#offerCommitMarker]
    /// @return recorded Archive position
    long awaitCommitPosition(final PreparedTransaction transaction, final long commitPosition) {
        try {
            FaultInjection.invoke(FaultInjection.Point.AFTER_COMMIT_OFFER, transaction.sequence);
            final long recordedPosition = this.commitPositionAwaiter.await(commitPosition,
                    this.configuration.recordedPositionTimeoutNanos());
            FaultInjection.invoke(FaultInjection.Point.AFTER_COMMIT_RECORDED, transaction.sequence);
            this.finishTerminal(transaction, false);
            return recordedPosition;
        } catch (final RuntimeException | Error failure) {
            this.finishTerminal(transaction, true);
            throw failure;
        }
    }

    /// Publishes an abort marker after the local Store rejects the transaction.
    long abort(final PreparedTransaction transaction) {
        final long offeredPosition;
        this.beginTerminal(transaction);
        /* As with the commit marker, run the retrying offer outside the state
         * monitor so close() and monitoring do not stall behind back pressure. */
        try {
            offeredPosition = transaction.framer.offerMarker(AeronReplicationEnvelope.Kind.ABORT,
                    transaction.dataLength, transaction.dataChunkCount, 0);
            transaction.abortAttempted = true;
        } catch (final RuntimeException | Error failure) {
            this.finishTerminal(transaction, true);
            throw failure;
        }
        try {
            final long position = this.commitPositionAwaiter.await(offeredPosition,
                    this.configuration.recordedPositionTimeoutNanos());
            synchronized (this) {
                transaction.abortPosition = position;
                transaction.terminal = true;
                this.pendingTransaction = null;
                this.operation = Operation.IDLE;
            }
            transaction.framer.close();
            FaultInjection.invoke(FaultInjection.Point.AFTER_ABORT_OFFERED, transaction.sequence);
            /* Notify outside the publisher monitor. Boundary callbacks may acquire the
             * provider lock, and invoking them while holding this lock would create a
             * publisher/provider lock-order cycle during shutdown. */
            transaction.invokeAbortAction(position);
            return position;
        } catch (final RuntimeException | Error failure) {
            /* The marker may already have been offered when the Archive acknowledgement
             * failed. Surface that attempted abort with its known-or-unknown position so
             * the coordinator can persist an uncertain state instead of losing evidence. */
            this.finishTerminal(transaction, true);
            this.invokeAbortActionAfterFailure(transaction, failure);
            throw failure;
        }
    }

    private int chunkCount(final int length) {
        return EnvelopeFramer.chunkCount(length, this.configuration.chunkSize());
    }

    /// Replays an abort callback after a publication failure, retaining callback failures.
    private void invokeAbortActionAfterFailure(final PreparedTransaction transaction, final Throwable failure) {
        final long abortPosition;
        synchronized (this) {
            abortPosition = transaction.abortPosition;
        }
        try {
            transaction.invokeAbortAction(abortPosition);
        } catch (final RuntimeException | Error callbackFailure) {
            failure.addSuppressed(callbackFailure);
        }
    }

    private void validate(final PreparedTransaction transaction) {
        if (transaction == null || transaction.owner != this) throw new IllegalArgumentException("unknown prepared transaction");
    }

    private void ensureOpen() {
        if (this.lifecycle != Lifecycle.OPEN) throw new IllegalStateException("Aeron publisher is closed or closing");
        if (this.failed) throw new IllegalStateException("Aeron publisher is failed closed");
    }

    private void ensureNoPendingTransaction() {
        if (this.operation == Operation.PREPARING) {
            throw new IllegalStateException("an Aeron transaction is being prepared");
        }
        if (this.pendingTransaction != null && !this.pendingTransaction.terminal) {
            throw new IllegalStateException("an Aeron prepared transaction is already pending");
        }
    }

    private void ensureNoSequenceReservation() {
        if (this.reservedSequence != -1L) {
            throw new IllegalStateException("an Aeron sequence reservation is already outstanding");
        }
    }

    /// Returns whether an uncommitted prepared transaction owns this publisher.
    synchronized boolean hasPendingTransaction() {
        return this.pendingTransaction != null && !this.pendingTransaction.terminal;
    }

    /// Returns whether publication resources have completed their terminal close.
    synchronized boolean isClosed() {
        return this.lifecycle == Lifecycle.CLOSED;
    }

    /// Claims the publisher for its single write coordinator.
    synchronized void claimCoordinator(final AeronReplicationWriteCoordinator coordinator) {
        this.ensureOpen();
        if (this.coordinatorOwner != null && this.coordinatorOwner != coordinator) {
            throw new IllegalStateException("an Aeron publisher already has a write coordinator");
        }
        this.coordinatorOwner = coordinator;
    }

    /// Releases the coordinator claim during owner disposal.
    synchronized void releaseCoordinator(final AeronReplicationWriteCoordinator coordinator) {
        if (this.coordinatorOwner == coordinator) this.coordinatorOwner = null;
    }

    /// Prevents further writes after an external durability or boundary failure.
    synchronized void failClosed() {
        this.failed = true;
    }

    /// Returns whether a publication failure made this writer fail closed.
    synchronized boolean isFailed() {
        return this.failed;
    }

    /// Aborts a pending transaction when possible and releases the publication.
    @Override
    public void close() {
        this.closeInternal();
    }

    private void closeInternal() {
        final PreparedTransaction pending;
        final boolean abortPending;
        synchronized (this) {
            if (this.lifecycle == Lifecycle.CLOSED) return;
            if (this.operation == Operation.PREPARING) {
                throw new IllegalStateException("Aeron publisher preparation is still in progress");
            }
            if (this.operation == Operation.TERMINAL) {
                throw new IllegalStateException("Aeron publisher terminal operation is still in progress");
            }
            if (this.lifecycle == Lifecycle.CLOSING) {
                throw new IllegalStateException("Aeron publisher close is already in progress");
            }
            pending = this.pendingTransaction;
            abortPending = pending != null && !pending.terminal && !pending.commitPending;
            this.lifecycle = Lifecycle.CLOSING;
            if (this.reservedSequence != -1L) {
                /* A reservation can exist before preparation creates a token.  Keep the
                 * sequence consumed for fail-closed recovery, but drop the in-memory
                 * reservation so close retry cannot reuse it. */
                this.reservedSequence = -1L;
                this.failed = true;
            }
        }
        RuntimeException failure = null;
        Error fatalFailure = null;
        boolean reopen = false;
        boolean closedForGood = false;
        boolean abortCompleted = !abortPending;
        if (!abortPending && pending != null) {
            /* A locally accepted Store commit must never be contradicted by an
             * ABORT during shutdown. Its persisted mark lets restart recovery
             * append the missing COMMIT if this offer did not reach Aeron. */
            synchronized (this) {
                if (this.pendingTransaction == pending) {
                    pending.terminal = true;
                    this.pendingTransaction = null;
                }
            }
        }
        if (abortPending) {
            boolean abortMarkerOffered = false;
            try {
                /* The retrying abort offer runs outside the state monitor; only
                 * the token's flag update needs it. */
                final long offeredPosition = pending.framer.offerMarker(
                        AeronReplicationEnvelope.Kind.ABORT, pending.dataLength, pending.dataChunkCount, 0);
                synchronized (this) {
                    pending.abortAttempted = true;
                }
                abortMarkerOffered = true;
                final long position = this.commitPositionAwaiter.await(offeredPosition,
                    this.configuration.recordedPositionTimeoutNanos());
                synchronized (this) {
                    pending.abortPosition = position;
                }
                abortCompleted = true;
                pending.invokeAbortAction(position);
            } catch (final RuntimeException abortFailure) {
                /* A failure before Aeron accepted the marker is retryable: keep the
                 * pending token and publication alive so a transient NOT_CONNECTED or
                 * back-pressure condition can be retried. Once the marker was accepted,
                 * its durability is ambiguous and the writer must fail closed. */
                if (abortMarkerOffered) {
                    this.failed = true;
                    /* The marker was accepted but its durable position is unknown. A
                     * coordinator callback fails the writer closed, and restart recovery
                     * resolves the open transaction from the Archive tail. */
                    this.invokeAbortActionAfterFailure(pending, abortFailure);
                    /* Aeron accepted the marker, so a second close must never emit a
                     * contradictory terminal marker.  Its recorded position is unknown,
                     * therefore the boundary callback is intentionally not invoked and
                     * recovery remains fail-closed. */
                    abortCompleted = true;
                } else {
                    reopen = true;
                }
                failure = abortFailure;
            } catch (final Error abortFailure) {
                this.failed = true;
                /* Fatal errors are not retryable.  Release the publication even when
                 * the marker was not accepted; retaining it would leak the native
                 * envelope storage during shutdown. */
                abortCompleted = true;
                /* Preserve fatal JVM errors for the caller.  They are still followed by
                 * best-effort publication cleanup below, but wrapping an OOME or
                 * StackOverflowError as an ordinary state failure obscures the cause. */
                fatalFailure = abortFailure;
            } finally {
                if (abortCompleted) {
                    synchronized (this) {
                        if (this.pendingTransaction == pending) {
                            pending.terminal = true;
                            this.pendingTransaction = null;
                        }
                    }
                }
            }
        }
        if (abortCompleted) {
            try {
                if (this.closeAction != null) this.closeAction.close();
            } catch (final Exception closeFailure) {
                final RuntimeException wrapped = new IllegalStateException("failed to close Aeron publication", closeFailure);
                if (failure == null) failure = wrapped;
                else failure.addSuppressed(wrapped);
            } catch (final Error closeFailure) {
                if (fatalFailure == null) fatalFailure = closeFailure;
                else fatalFailure.addSuppressed(closeFailure);
            } finally {
                this.releaseFramingStorage();
                if (pending != null) pending.framer.close();
            }
        }
        try {
            if (fatalFailure != null) {
                if (failure != null) fatalFailure.addSuppressed(failure);
                throw fatalFailure;
            }
            if (failure != null) {
                throw failure;
            }
            closedForGood = true;
        } finally {
            synchronized (this) {
                this.lifecycle = closedForGood ? Lifecycle.CLOSED
                        : reopen ? Lifecycle.OPEN : Lifecycle.CLOSE_INTERRUPTED;
            }
        }
    }

    private void releaseFramingStorage() {
        this.framingAllocation.close();
    }

}
