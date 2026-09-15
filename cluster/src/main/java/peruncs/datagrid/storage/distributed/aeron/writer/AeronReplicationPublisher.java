package peruncs.datagrid.storage.distributed.aeron.writer;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import peruncs.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongUnaryOperator;
import java.util.zip.CRC32C;

/**
 * Publishes one ordered transaction at a time.
 *
 * <p>Data chunks are prepared first. A commit marker makes the complete
 * transaction visible; an abort marker closes a rejected or abandoned one.
 * The publisher stages frames in one direct buffer so a large Store binary is
 * not flattened into a second heap copy.</p>
 */
final class AeronReplicationPublisher implements AutoCloseable {
    private static final Cleaner CLEANER = Cleaner.create();
    private static final UnsafeBuffer EMPTY_BUFFER = new UnsafeBuffer();
    private final AeronOfferRetryer offerer;
    private final int maxMessageLength;
    private final AeronReplicationConfiguration configuration;
    private final UUID clusterId;
    private final long epoch;
    private final AutoCloseable closeAction;
    private final LongUnaryOperator commitPositionAwaiter;
    private final ByteBuffer envelopeStorage;
    private final UnsafeBuffer envelopeBuffer;
    private final DirectBufferCleanup directBufferCleanup;
    private final Cleaner.Cleanable cleanable;
    private final byte[] crcScratch = new byte[16 * 1024];
    /* Publisher methods are synchronized, so one reusable CRC instance is enough
     * and avoids retaining a ThreadLocal value on every caller thread. */
    private final CRC32C dataCrc = new CRC32C();
    private final CRC32C chunkCrc = new CRC32C();
    /* All sequence operations are protected by this publisher's monitor. Keeping
     * the value primitive avoids an allocation and makes the ownership rule
     * explicit instead of implying lock-free access. */
    private long nextSequence;
    /* A reservation is an ownership token, not merely a value derived from
     * nextSequence. Keeping it explicitly prevents an unrelated caller from
     * bypassing the durable fence by reusing the incremented value. */
    private long reservedSequence = -1L;
    private FailedPrepare failedPrepare;
    private PreparedTransaction pendingTransaction;
    private Object coordinatorOwner;
    private volatile boolean failed;
    private boolean closeRequested;
    private boolean closeInProgress;
    private boolean terminalOperation;
    private boolean envelopeFreed;
    private boolean closed;

    AeronReplicationPublisher(final ExclusivePublication publication,
                              final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
                              final long initialSequence) {
        this(offerer(publication), actualMaxMessageLength(publication, configuration), configuration, clusterId, epoch, initialSequence,
                null, LongUnaryOperator.identity());
    }


    AeronReplicationPublisher(final ExclusivePublication publication,
                              final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
                              final long initialSequence, final LongUnaryOperator commitPositionAwaiter) {
        this(offerer(publication), actualMaxMessageLength(publication, configuration), configuration, clusterId, epoch, initialSequence,
                publication, commitPositionAwaiter);
    }

    AeronReplicationPublisher(final AeronOfferRetryer.Offerer offerer, final int maxMessageLength,
                              final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
                              final long initialSequence) {
        this(offerer, maxMessageLength, configuration, clusterId, epoch, initialSequence, (AutoCloseable) null);
    }

    AeronReplicationPublisher(final AeronOfferRetryer.Offerer offerer, final int maxMessageLength,
                              final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
                              final long initialSequence, final LongUnaryOperator commitPositionAwaiter) {
        this(offerer, maxMessageLength, configuration, clusterId, epoch, initialSequence, null,
                commitPositionAwaiter);
    }

    private AeronReplicationPublisher(final AeronOfferRetryer.Offerer offerer, final int maxMessageLength,
                                      final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
                                      final long initialSequence, final AutoCloseable closeAction) {
        this(offerer, maxMessageLength, configuration, clusterId, epoch, initialSequence, closeAction,
                LongUnaryOperator.identity());
    }

    private AeronReplicationPublisher(final AeronOfferRetryer.Offerer offerer, final int maxMessageLength,
                                      final AeronReplicationConfiguration configuration, final UUID clusterId, final long epoch,
                                      final long initialSequence, final AutoCloseable closeAction, final LongUnaryOperator commitPositionAwaiter) {
        if (offerer == null || configuration == null || clusterId == null || commitPositionAwaiter == null ||
            initialSequence < 0 || initialSequence == Long.MAX_VALUE || maxMessageLength <= 0 ||
            maxMessageLength > configuration.maxMessageLength() ||
            (long) configuration.chunkSize() + AeronReplicationEnvelope.HEADER_LENGTH > maxMessageLength) {
            throw new IllegalArgumentException("invalid publisher configuration");
        }
        this.offerer = new AeronOfferRetryer(offerer, configuration);
        this.maxMessageLength = maxMessageLength;
        this.configuration = configuration;
        this.clusterId = clusterId;
        this.epoch = epoch;
        this.nextSequence = initialSequence;
        this.closeAction = closeAction;
        this.commitPositionAwaiter = commitPositionAwaiter;
        this.envelopeStorage = ByteBuffer.allocateDirect(maxMessageLength);
        this.envelopeBuffer = new UnsafeBuffer(this.envelopeStorage);
        this.directBufferCleanup = new DirectBufferCleanup(this.envelopeStorage);
        this.cleanable = CLEANER.register(this, this.directBufferCleanup);
    }

    private static AeronOfferRetryer.Offerer offerer(final ExclusivePublication publication) {
        return new AeronOfferRetryer.Offerer() {
            @Override
            public long offer(final org.agrona.DirectBuffer buffer, final int offset, final int length) {
                return publication.offer(buffer, offset, length);
            }

            @Override
            public boolean isConnected() {
                return publication.isConnected();
            }
        };
    }

    private static int actualMaxMessageLength(final ExclusivePublication publication,
                                              final AeronReplicationConfiguration configuration) {
        if (publication == null) throw new NullPointerException("publication");
        if (configuration == null) throw new NullPointerException("configuration");
        final int actual = publication.maxMessageLength();
        if (actual <= 0) {
            throw new IllegalArgumentException("Aeron publication has no usable message capacity");
        }
        return Math.min(actual, configuration.maxMessageLength());
    }

    private static long totalRemaining(final ByteBuffer[] buffers, final int count) {
        if (buffers == null) throw new NullPointerException("dataBuffers");
        if (count < 0 || count > buffers.length) throw new IllegalArgumentException("invalid data buffer count");
        long length = 0;
        for (int index = 0; index < count; index++) {
            final ByteBuffer buffer = buffers[index];
            if (buffer == null) throw new NullPointerException("dataBuffers contains null");
            length = Math.addExact(length, buffer.remaining());
        }
        return length;
    }

    /** Publishes one transaction and its terminal commit marker. */
    synchronized long publishTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers) {
        this.ensureNoSequenceReservation();
        return this.commit(this.prepareTransaction(dictionary, dataBuffers, dataBuffers == null ? 0 : dataBuffers.length));
    }

    /**
     * Publishes dictionary and Store-data chunks and returns a token whose commit
     * marker is pending. The caller must commit or close the token. If publishing
     * fails after a sequence is reserved, the publisher attempts an abort and
     * then fails closed so a later write cannot skip the damaged sequence.
     * This low-level overload is reserved for direct publisher users; coordinator
     * writes must use the explicit-sequence overload so their durable fence and
     * publication cannot diverge.
     *
     * @param dictionary  optional type dictionary bytes
     * @param dataBuffers Store binary buffers; positions are not changed
     * @return a token that must be committed or closed
     */
    synchronized PreparedTransaction prepareTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers) {
        return this.prepareTransaction(dictionary, dataBuffers, dataBuffers == null ? 0 : dataBuffers.length);
    }

    /** Prepares a transaction using only the populated prefix of a reusable buffer array. */
    private synchronized PreparedTransaction prepareTransaction(final byte[] dictionary,
                                                                final ByteBuffer[] dataBuffers, final int bufferCount) {
        this.ensureOpen();
        this.ensureNoSequenceReservation();
        if (this.coordinatorOwner != null) {
            throw new IllegalStateException(
                    "coordinator-owned publisher requires the explicit reserved-sequence preparation path");
        }
        this.failedPrepare = null;
        this.ensureNoPendingTransaction();
        CrashHook.invoke("BEFORE_PREPARE", this.nextSequence);
        final int dictionaryLength = dictionary == null ? 0 : dictionary.length;
        final long totalDataLength = totalRemaining(dataBuffers, bufferCount);
        if (totalDataLength > (long) this.configuration.maxTransactionBytes() - dictionaryLength) {
            throw new IllegalArgumentException("Store transaction exceeds maxTransactionBytes");
        }
        final int dataLength = (int) totalDataLength;
        final long sequence = this.nextSequence;
        if (sequence < 0 || sequence == Long.MAX_VALUE) {
            throw new IllegalStateException("Aeron replication sequence space is exhausted");
        }
        this.nextSequence = sequence + 1;
        final int dataChunks = chunkCount(dataLength);
        return this.prepareReserved(dictionary, dataBuffers, bufferCount, sequence, dataLength, dataChunks, -1, false);
    }

    /**
     * Completes preparation for a sequence reserved before a durable fence was
     * written. The explicit sequence prevents a crash between the fence write
     * and publication from leaving two different sequence numbers in the log.
     * The metadata must describe the same buffers; it is checked again before
     * publication so a caller cannot reuse a fence for different data.
     * This explicit reservation is part of the durable-fence contract and must
     * not be replaced with an independent sequence allocation.
     *
     * @param dictionary       optional type dictionary bytes
     * @param dataBuffers      Store binary buffers whose positions are not changed
     * @param reservedSequence sequence returned by {@link #reserveSequence()}
     * @param metadata         length, chunk count, and CRC captured for the fence
     * @return a token that must be committed or closed
     * @throws IllegalStateException    if the reservation is no longer current
     * @throws IllegalArgumentException if the metadata does not match the buffers
     */
    synchronized PreparedTransaction prepareTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers,
                                                        final long reservedSequence, final TransactionMetadata metadata) {
        return this.prepareTransaction(dictionary, dataBuffers, dataBuffers == null ? 0 : dataBuffers.length,
                reservedSequence, metadata);
    }

    /** Completes preparation with an explicit sequence and populated buffer count. */
    synchronized PreparedTransaction prepareTransaction(final byte[] dictionary, final ByteBuffer[] dataBuffers,
                                                        final int bufferCount, final long reservedSequence, final TransactionMetadata metadata) {
        this.ensureOpen();
        this.failedPrepare = null;
        this.ensureNoPendingTransaction();
        if (metadata == null || reservedSequence < 0 || reservedSequence == Long.MAX_VALUE ||
            this.reservedSequence != reservedSequence || this.nextSequence != reservedSequence + 1) {
            throw new IllegalStateException("reserved replication sequence is no longer current");
        }
        final int dictionaryLength = dictionary == null ? 0 : dictionary.length;
        if (metadata.dataLength() != totalRemaining(dataBuffers, bufferCount) ||
            metadata.dataLength() < 0 || metadata.dataChunkCount() != this.chunkCount(metadata.dataLength()) ||
            (long) metadata.dataLength() > (long) this.configuration.maxTransactionBytes() - dictionaryLength) {
            throw new IllegalArgumentException("transaction metadata does not match Store data");
        }
        CrashHook.invoke("BEFORE_PREPARE", reservedSequence);
        return this.prepareReserved(dictionary, dataBuffers, bufferCount, reservedSequence, metadata.dataLength(),
                metadata.dataChunkCount(), metadata.crc32c(), true);
    }

    private PreparedTransaction prepareReserved(final byte[] dictionary, final ByteBuffer[] dataBuffers,
                                                final int bufferCount, final long sequence, final int dataLength, final int dataChunks, final int expectedCrc32c,
                                                final boolean verifyExpectedCrc) {
        try {
            if (dictionary != null && dictionary.length != 0) {
                this.publishDictionaryChunks(sequence, new UnsafeBuffer(dictionary), dictionary.length);
                CrashHook.invoke("AFTER_DICTIONARY_CHUNKS", sequence);
            }
            final int dataCrc32c = this.publishDataChunks(sequence, dataBuffers, bufferCount, dataLength);
            if (verifyExpectedCrc && dataCrc32c != expectedCrc32c) {
                throw new IllegalArgumentException("transaction data changed after durable fence");
            }
            CrashHook.invoke("AFTER_DATA_CHUNKS", sequence);
            final PreparedTransaction prepared = new PreparedTransaction(this, sequence, dataLength, dataChunks,
                    dataCrc32c);
            this.pendingTransaction = prepared;
            if (this.reservedSequence == sequence) this.reservedSequence = -1L;
            CrashHook.invoke("AFTER_PREPARE", sequence);
            return prepared;
        } catch (final Error failure) {
            /* Do not allocate, compute a checksum, or offer a compensating marker
             * while the JVM is already in a fatal Error path.  The durable fence (when
             * one exists) remains unresolved and therefore forces fail-closed recovery. */
            final PreparedTransaction pending = this.pendingTransaction;
            if (pending != null) {
                pending.terminal = true;
                this.pendingTransaction = null;
            }
            if (this.reservedSequence == sequence) this.reservedSequence = -1L;
            this.failed = true;
            throw failure;
        } catch (final RuntimeException failure) {
            /* AFTER_PREPARE can throw after the token has become the pending
             * transaction. The recovery abort below is the terminal decision for
             * that token; clear the owner reference even when the abort offer fails
             * so close() cannot emit a second terminal marker. */
            final PreparedTransaction pending = this.pendingTransaction;
            int failedCrc32c = 0;
            try {
                failedCrc32c = this.computeDataCrc(dataBuffers, bufferCount, dataLength);
            } catch (final Error crcFailure) {
                throw crcFailure;
            } catch (final RuntimeException crcFailure) {
                failure.addSuppressed(crcFailure);
            }
            this.failedPrepare = new FailedPrepare(sequence, dataLength, dataChunks, failedCrc32c);
            try {
                // Clear any transaction prefix that reached the log. If the publication
                // itself is gone this fails as well, and recovery must retain the tail.
                this.offerMarker(sequence, AeronReplicationEnvelope.Kind.ABORT, dataLength, dataChunks, 0);
                this.failed = true;
                CrashHook.invoke("AFTER_PREPARE_FAILURE_ABORT_OFFERED", sequence);
            } catch (final Error abortFailure) {
                throw abortFailure;
            } catch (final RuntimeException abortFailure) {
                failure.addSuppressed(abortFailure);
            }
            if (pending != null) {
                pending.terminal = true;
                this.pendingTransaction = null;
            }
            if (this.reservedSequence == sequence) this.reservedSequence = -1L;
            this.failed = true;
            throw failure;
        }
    }

    /** Returns metadata for the most recent failed prepare, if any. */
    synchronized FailedPrepare failedPrepare() {
        return this.failedPrepare;
    }

    /**
     * Returns the next unreserved sequence for diagnostics and recovery checks.
     * This method does not reserve the value; use {@link #reserveSequence()} when
     * a durable fence must carry the same sequence as a later publication.
     */
    synchronized long nextSequence() {
        return this.nextSequence;
    }

    /**
     * Reserves the next sequence so a durable fence and its later publication
     * share one sequence number. The reservation must either be used by the
     * explicit-sequence preparation method or released after a local rejection.
     * A caller must not publish another transaction while this reservation is
     * outstanding.
     *
     * @return the sequence reserved for the next explicit-sequence preparation
     */
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

    /** Releases a reservation when the local Store rejects the fenced write. */
    synchronized void releaseReservedSequence(final long sequence) {
        if (sequence < 0 || sequence == Long.MAX_VALUE || this.reservedSequence != sequence ||
            this.nextSequence != sequence + 1) {
            throw new IllegalStateException("replication sequence reservation is no longer current");
        }
        this.nextSequence = sequence;
        this.reservedSequence = -1L;
    }

    /**
     * Abandons a reservation after local Store acceptance when publication cannot
     * even be prepared. The sequence remains consumed and the caller must have
     * persisted an in-flight uncertainty marker; rewinding it would let a later
     * write reuse a sequence whose local bytes already exist.
     */
    synchronized void abandonReservedSequence(final long sequence) {
        if (sequence < 0 || sequence == Long.MAX_VALUE || this.reservedSequence != sequence ||
            this.nextSequence != sequence + 1) {
            throw new IllegalStateException("replication sequence reservation is no longer current");
        }
        this.reservedSequence = -1L;
        this.failed = true;
    }

    /** Returns whether a durable fence currently owns the next sequence. */
    synchronized boolean hasSequenceReservation() {
        return this.reservedSequence != -1L;
    }

    /** Computes transaction metadata for the populated prefix of a reusable buffer array. */
    synchronized TransactionMetadata transactionMetadata(final ByteBuffer[] dataBuffers, final int bufferCount) {
        final long length = totalRemaining(dataBuffers, bufferCount);
        if (length > this.configuration.maxTransactionBytes()) {
            throw new IllegalArgumentException("Store transaction exceeds maxTransactionBytes");
        }
        final int dataLength = (int) length;
        return new TransactionMetadata(dataLength, this.chunkCount(dataLength),
                this.computeDataCrc(dataBuffers, bufferCount, dataLength));
    }

    /**
     * Publishes Store data directly from the caller's buffer sequence.
     *
     * <p>The returned value is the CRC32C of the complete logical Store binary.
     * The coordinator uses it in the commit marker. The earlier fence CRC remains
     * a separate pass because it is the recovery evidence written before local
     * Store acceptance.</p>
     */
    private int publishDataChunks(final long sequence, final ByteBuffer[] sources, final int sourceCount, final int length) {
        final CRC32C crc = this.dataCrc;
        crc.reset();
        if (length == 0) {
            this.offerEncoded(sequence, AeronReplicationEnvelope.Kind.STORE_BINARY,
                    0, 0, 1, 0, 0, EMPTY_BUFFER, 0, 0);
            return 0;
        }

        final int count = this.chunkCount(length);
        int sourceIndex = 0;
        ByteBuffer source = sources[sourceIndex];
        int sourcePosition = source.position();
        int logicalOffset = 0;
        for (int chunkIndex = 0; chunkIndex < count; chunkIndex++) {
            final int chunkLength = Math.min(this.configuration.chunkSize(), length - logicalOffset);
            this.chunkCrc.reset();
            int copied = 0;
            while (copied < chunkLength) {
                while (sourcePosition >= source.limit()) {
                    if (++sourceIndex >= sourceCount) throw new IllegalArgumentException("data buffer length changed");
                    source = sources[sourceIndex];
                    sourcePosition = source.position();
                }
                final int amount = Math.min(this.crcScratch.length,
                        Math.min(source.limit() - sourcePosition, chunkLength - copied));
                source.get(sourcePosition, this.crcScratch, 0, amount);
                crc.update(this.crcScratch, 0, amount);
                this.chunkCrc.update(this.crcScratch, 0, amount);
                this.envelopeBuffer.putBytes(AeronReplicationEnvelope.HEADER_LENGTH + copied,
                        this.crcScratch, 0, amount);
                sourcePosition += amount;
                copied += amount;
            }
            this.offerDataChunk(sequence, length, chunkIndex, count, logicalOffset,
                    chunkLength, (int) this.chunkCrc.getValue());
            logicalOffset += chunkLength;
        }
        return (int) crc.getValue();
    }

    private int computeDataCrc(final ByteBuffer[] sources, final int sourceCount, final int length) {
        final CRC32C crc = this.dataCrc;
        crc.reset();
        int remaining = length;
        for (int sourceIndex = 0; sourceIndex < sourceCount; sourceIndex++) {
            final ByteBuffer sourceBuffer = sources[sourceIndex];
            final int amount = Math.min(remaining, sourceBuffer.remaining());
            if (amount > 0) {
                updateCrc(crc, sourceBuffer, sourceBuffer.position(), amount);
                remaining -= amount;
            }
            if (remaining == 0) break;
        }
        if (remaining != 0) throw new IllegalArgumentException("data buffer length changed");
        return (int) crc.getValue();
    }

    /** Updates CRC32C using absolute reads without mutating caller state. */
    private void updateCrc(final CRC32C crc, final ByteBuffer source,
                           final int offset, final int length) {
        for (int copied = 0; copied < length; ) {
            final int amount = Math.min(this.crcScratch.length, length - copied);
            source.get(offset + copied, this.crcScratch, 0, amount);
            crc.update(this.crcScratch, 0, amount);
            copied += amount;
        }
    }

    /** Publishes the commit marker and waits for the configured durability boundary. */
    long commit(final PreparedTransaction transaction) {
        final long commitPosition;
        synchronized (this) {
            this.ensureOpen();
            this.validate(transaction);
            if (transaction.terminal) throw new IllegalStateException("prepared transaction is already terminal");
            if (this.terminalOperation) throw new IllegalStateException("publisher terminal operation is already running");
            this.terminalOperation = true;
            try {
                CrashHook.invoke("BEFORE_COMMIT_OFFER", transaction.sequence);
                commitPosition = this.offerMarker(transaction.sequence, AeronReplicationEnvelope.Kind.COMMIT,
                        transaction.dataLength, transaction.dataChunkCount, transaction.crc32c);
            } catch (final RuntimeException | Error failure) {
                transaction.terminal = true;
                this.pendingTransaction = null;
                this.terminalOperation = false;
                this.failed = true;
                throw failure;
            }
        }
        try {
            CrashHook.invoke("AFTER_COMMIT_OFFER", transaction.sequence);
            final long recordedPosition = this.commitPositionAwaiter.applyAsLong(commitPosition);
            CrashHook.invoke("AFTER_COMMIT_RECORDED", transaction.sequence);
            synchronized (this) {
                transaction.terminal = true;
                this.pendingTransaction = null;
                this.terminalOperation = false;
            }
            return recordedPosition;
        } catch (final RuntimeException | Error failure) {
            /* Once the commit marker was offered it may still be delivered or
             * recorded. Publishing an abort after an acknowledgement timeout would
             * create two terminal markers for one sequence. Fail closed instead. */
            synchronized (this) {
                transaction.terminal = true;
                this.pendingTransaction = null;
                this.terminalOperation = false;
                this.failed = true;
            }
            throw failure;
        }
    }

    /** Publishes an abort marker after the local Store rejects the transaction. */
    long abort(final PreparedTransaction transaction) {
        final long offeredPosition;
        synchronized (this) {
            this.ensureOpen();
            this.validate(transaction);
            if (transaction.terminal) throw new IllegalStateException("prepared transaction is already terminal");
            if (this.terminalOperation) throw new IllegalStateException("publisher terminal operation is already running");
            this.terminalOperation = true;
            try {
                offeredPosition = this.offerMarker(transaction.sequence, AeronReplicationEnvelope.Kind.ABORT,
                        transaction.dataLength, transaction.dataChunkCount, 0);
                transaction.abortAttempted = true;
            } catch (final RuntimeException | Error failure) {
                transaction.terminal = true;
                this.pendingTransaction = null;
                this.terminalOperation = false;
                this.failed = true;
                throw failure;
            }
        }
        try {
            final long position = this.commitPositionAwaiter.applyAsLong(offeredPosition);
            synchronized (this) {
                transaction.abortPosition = position;
                transaction.terminal = true;
                this.pendingTransaction = null;
                this.terminalOperation = false;
            }
            CrashHook.invoke("AFTER_ABORT_OFFERED", transaction.sequence);
            /* Notify outside the publisher monitor. Checkpoint listeners may acquire the
             * provider lock, and invoking them while holding this lock would create a
             * publisher/provider lock-order cycle during shutdown. */
            transaction.invokeAbortAction(position);
            return position;
        } catch (final RuntimeException | Error failure) {
            /* The marker may already have been offered when the Archive acknowledgement
             * failed. Surface that attempted abort with its known-or-unknown position so
             * the coordinator can persist an uncertain state instead of losing evidence. */
            synchronized (this) {
                transaction.terminal = true;
                this.pendingTransaction = null;
                this.terminalOperation = false;
                this.failed = true;
            }
            this.invokeAbortActionAfterFailure(transaction, failure);
            throw failure;
        }
    }

    private void publishDictionaryChunks(final long sequence, final org.agrona.DirectBuffer bytes, final int length) {
        if (length == 0) {
            return;
        }
        final int count = this.chunkCount(length);
        for (int index = 0, offset = 0; offset < length; index++) {
            final int chunkLength = Math.min(this.configuration.chunkSize(), length - offset);
            this.offerEncoded(sequence, AeronReplicationEnvelope.Kind.TYPE_DICTIONARY, length, index, count,
                    offset, 0, bytes, offset, chunkLength);
            offset += chunkLength;
        }
    }

    private int chunkCount(final int length) {
        return Math.max(1, (int) ((length + (long) this.configuration.chunkSize() - 1L) /
                                  this.configuration.chunkSize()));
    }

    private long offerEncoded(final long sequence, final AeronReplicationEnvelope.Kind kind,
                              final int payloadLength, final int chunkIndex, final int chunkCount, final int chunkOffset,
                              final int commitCrc32c, final org.agrona.DirectBuffer payload, final int payloadOffset,
                              final int payloadChunkLength) {
        final int encodedLength = AeronReplicationEnvelope.encode(this.envelopeBuffer, 0, this.clusterId,
                this.epoch, sequence, kind, payloadLength, chunkIndex, chunkCount, chunkOffset, commitCrc32c,
                payload == null ? EMPTY_BUFFER : payload, payloadOffset, payloadChunkLength);
        if (encodedLength > this.maxMessageLength) {
            throw new IllegalArgumentException("replication chunk exceeds Aeron max message length");
        }
        return this.offerer.offer(this.envelopeBuffer, encodedLength);
    }

    private void offerDataChunk(final long sequence, final int payloadLength, final int chunkIndex,
                                final int chunkCount, final int chunkOffset, final int payloadChunkLength, final int payloadCrc32c) {
        final int encodedLength = AeronReplicationEnvelope.encodeWithPayloadCrc(this.envelopeBuffer, 0,
                this.clusterId, this.epoch, sequence, AeronReplicationEnvelope.Kind.STORE_BINARY, payloadLength,
                chunkIndex, chunkCount, chunkOffset, 0, this.envelopeBuffer,
                AeronReplicationEnvelope.HEADER_LENGTH, payloadChunkLength,
                payloadCrc32c);
        if (encodedLength > this.maxMessageLength) {
            throw new IllegalArgumentException("replication chunk exceeds Aeron max message length");
        }
        this.offerer.offer(this.envelopeBuffer, encodedLength);
    }

    /** Replays an abort callback after a publication failure, retaining callback failures. */
    private void invokeAbortActionAfterFailure(final PreparedTransaction transaction, final Throwable failure) {
        final long abortPosition;
        synchronized (this) {
            abortPosition = transaction.abortPosition;
        }
        try {
            transaction.invokeAbortAction(abortPosition);
        } catch (final RuntimeException callbackFailure) {
            failure.addSuppressed(callbackFailure);
        }
    }

    private long offerMarker(final long sequence, final AeronReplicationEnvelope.Kind kind,
                             final int payloadLength, final int chunkCount, final int commitCrc32c) {
        return this.offerEncoded(sequence, kind, payloadLength, 0, Math.max(1, chunkCount), 0,
                commitCrc32c, EMPTY_BUFFER, 0, 0);
    }

    /** Advances the next sequence when an external cursor or promotion supplies a newer index. */
    synchronized void synchronizeNextSequence(final long next) {
        if (next < 0) throw new IllegalArgumentException("next sequence must be non-negative");
        this.ensureNoPendingTransaction();
        this.ensureNoSequenceReservation();
        if (next > this.nextSequence) this.nextSequence = next;
    }

    private void validate(final PreparedTransaction transaction) {
        if (transaction == null || transaction.owner != this) throw new IllegalArgumentException("unknown prepared transaction");
    }

    private void ensureOpen() {
        if (this.closed || this.closeRequested) throw new IllegalStateException("Aeron publisher is closed or closing");
        if (this.failed) throw new IllegalStateException("Aeron publisher is failed closed");
    }

    private void ensureNoPendingTransaction() {
        if (this.pendingTransaction != null && !this.pendingTransaction.terminal) {
            throw new IllegalStateException("an Aeron prepared transaction is already pending");
        }
    }

    private void ensureNoSequenceReservation() {
        if (this.reservedSequence != -1L) {
            throw new IllegalStateException("an Aeron sequence reservation is already outstanding");
        }
    }

    /** Returns whether an uncommitted prepared transaction owns this publisher. */
    synchronized boolean hasPendingTransaction() {
        return this.pendingTransaction != null && !this.pendingTransaction.terminal;
    }

    /** Returns whether publication resources have completed their terminal close. */
    synchronized boolean isClosed() {
        return this.closed;
    }

    /** Claims the publisher for its single write coordinator. */
    synchronized void claimCoordinator(final AeronReplicationWriteCoordinator coordinator) {
        this.ensureOpen();
        if (this.coordinatorOwner != null && this.coordinatorOwner != coordinator) {
            throw new IllegalStateException("an Aeron publisher already has a write coordinator");
        }
        this.coordinatorOwner = coordinator;
    }

    /** Releases the coordinator claim during owner disposal. */
    synchronized void releaseCoordinator(final AeronReplicationWriteCoordinator coordinator) {
        if (this.coordinatorOwner == coordinator) this.coordinatorOwner = null;
    }

    /** Prevents further writes after an external durability or checkpoint failure. */
    synchronized void failClosed() {
        this.failed = true;
    }

    /** Returns whether a publication failure made this writer fail closed. */
    synchronized boolean isFailed() {
        return this.failed;
    }

    /** Aborts a pending transaction when possible and releases the publication. */
    @Override
    public void close() {
        this.closeInternal(true);
    }

    /**
     * Releases the publication without manufacturing an ABORT marker. This is
     * used only when the Store has already accepted the transaction: the durable
     * in-flight fence must remain unresolved so restart fails closed instead of
     * pretending that a local acceptance was rejected.
     */
    void closeWithoutAbort() {
        this.closeInternal(false);
    }

    private void closeInternal(final boolean abortPendingOnClose) {
        final PreparedTransaction pending;
        final boolean abortPending;
        synchronized (this) {
            if (this.closed) return;
            if (this.terminalOperation) {
                throw new IllegalStateException("Aeron publisher terminal operation is still in progress");
            }
            if (this.closeInProgress) {
                throw new IllegalStateException("Aeron publisher close is already in progress");
            }
            this.closeInProgress = true;
            this.closeRequested = true;
            pending = this.pendingTransaction;
            abortPending = abortPendingOnClose && pending != null && !pending.terminal;
            if (!abortPending && pending != null && !pending.terminal) {
                /* The caller explicitly chose the fail-closed shutdown path.  Detach the
                 * token before closing the publication, but leave any coordinator fence on
                 * disk to force reseed on the next writer process. */
                pending.terminal = true;
                this.pendingTransaction = null;
                this.failed = true;
            }
            if (this.reservedSequence != -1L) {
                /* A fence can exist before preparation creates a token.  Keep the
                 * sequence consumed for fail-closed recovery, but drop the in-memory
                 * reservation so close retry cannot reuse it. */
                this.reservedSequence = -1L;
                this.failed = true;
            }
        }
        RuntimeException failure = null;
        Error fatalFailure = null;
        boolean abortCompleted = !abortPending;
        if (abortPending) {
            boolean abortMarkerOffered = false;
            try {
                final long offeredPosition;
                synchronized (this) {
                    offeredPosition = this.offerMarker(pending.sequence, AeronReplicationEnvelope.Kind.ABORT,
                            pending.dataLength, pending.dataChunkCount, 0);
                    pending.abortAttempted = true;
                }
                abortMarkerOffered = true;
                final long position = this.commitPositionAwaiter.applyAsLong(offeredPosition);
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
                     * coordinator callback records COMMITTING_UNCERTAIN and keeps restart
                     * on the safe reseed path. */
                    this.invokeAbortActionAfterFailure(pending, abortFailure);
                    /* Aeron accepted the marker, so a second close must never emit a
                     * contradictory terminal marker.  Its recorded position is unknown,
                     * therefore the checkpoint callback is intentionally not invoked and
                     * recovery remains fail-closed. */
                    abortCompleted = true;
                } else {
                    synchronized (this) {
                        this.closeRequested = false;
                    }
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
                boolean free;
                synchronized (this) {
                    free = !this.envelopeFreed;
                    this.envelopeFreed = true;
                }
                if (free) this.directBufferCleanup.run();
                this.cleanable.clean();
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
            synchronized (this) {
                this.closed = true;
            }
        } finally {
            synchronized (this) {
                this.closeInProgress = false;
            }
        }
    }

    /** Cleaner action deliberately contains no reference to the publisher. */
    private static final class DirectBufferCleanup implements Runnable {
        private final ByteBuffer buffer;
        private final AtomicBoolean freed = new AtomicBoolean();

        private DirectBufferCleanup(final ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void run() {
            if (this.freed.compareAndSet(false, true)) BufferUtil.free(this.buffer);
        }
    }

    /** Metadata retained while a transaction moves through writer states. */
    record TransactionMetadata(int dataLength, int dataChunkCount, int crc32c) {
    }

    /** Source metadata retained when preparing a transaction fails. */
    record FailedPrepare(long sequence, int dataLength, int dataChunkCount, int crc32c) {
    }

    /** Closeable handle that aborts an unfinished prepared transaction. */
    static final class PreparedTransaction implements AutoCloseable {
        private final AeronReplicationPublisher owner;
        private final long sequence;
        private final int dataLength;
        private final int dataChunkCount;
        private final int crc32c;
        private volatile java.util.function.LongConsumer abortAction;
        private boolean abortActionInvoked;
        private boolean abortAttempted;
        private volatile boolean terminal;
        private long abortPosition = Aeron.NULL_VALUE;

        private PreparedTransaction(final AeronReplicationPublisher owner, final long sequence, final int dataLength,
                                    final int dataChunkCount, final int crc32c) {
            this.owner = owner;
            this.sequence = sequence;
            this.dataLength = dataLength;
            this.dataChunkCount = dataChunkCount;
            this.crc32c = crc32c;
        }

        long sequence() {
            return this.sequence;
        }

        int dataLength() {
            return this.dataLength;
        }

        int dataChunkCount() {
            return this.dataChunkCount;
        }

        /** Returns the CRC32C of the Store binary carried by this transaction. */
        int dataCrc32c() {
            return this.crc32c;
        }

        /**
         * Registers a callback for an abort whose publication has been attempted.
         * The callback runs synchronously on the caller that completes the abort; a
         * late registration is invoked immediately when the publisher already
         * completed that path. A position of {@code -1} means the marker was offered
         * but its durable Archive position is unknown.
         *
         * @param action receives the recorded abort position
         */
        void onAbort(final java.util.function.LongConsumer action) {
            if (action == null) throw new NullPointerException("action");
            boolean invoke;
            synchronized (this.owner) {
                if (this.abortAction != null && this.abortAction != action) {
                    throw new IllegalStateException("abort callback is already registered");
                }
                this.abortAction = action;
                invoke = this.terminal && this.abortAttempted && !this.abortActionInvoked;
                if (invoke) this.abortActionInvoked = true;
            }
            final long position;
            synchronized (this.owner) {
                position = this.abortPosition;
            }
            if (invoke) action.accept(position);
        }

        private void invokeAbortAction(final long position) {
            final java.util.function.LongConsumer action;
            synchronized (this.owner) {
                if (!this.abortAttempted || this.abortActionInvoked || this.abortAction == null) return;
                this.abortActionInvoked = true;
                action = this.abortAction;
            }
            action.accept(position);
        }

        /**
         * Detaches this token without emitting an abort marker. This is used only
         * after the local Store accepted data but a later step failed: an abort would
         * contradict the accepted Store state, so the publisher is failed closed and
         * the durable uncertainty fence is left for restart recovery.
         */
        void abandonWithoutAbort() {
            synchronized (this.owner) {
                if (this.terminal) return;
                this.terminal = true;
                if (this.owner.pendingTransaction == this) this.owner.pendingTransaction = null;
                this.owner.failed = true;
            }
        }

        /**
         * Aborts an abandoned transaction so its sequence is terminated in the log.
         * Closing after commit or abort has no effect.
         */
        @Override
        public void close() {
            synchronized (this.owner) {
                if (this.terminal) return;
                if (this.owner.closeRequested || this.owner.closed) {
                    return;
                }
                if (this.owner.failed) {
                    this.terminal = true;
                    if (this.owner.pendingTransaction == this) this.owner.pendingTransaction = null;
                    return;
                }
            }
            this.owner.abort(this);
        }
    }
}
