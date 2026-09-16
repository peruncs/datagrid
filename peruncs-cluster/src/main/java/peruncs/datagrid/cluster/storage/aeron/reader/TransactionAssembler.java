package peruncs.datagrid.cluster.storage.aeron.reader;

import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataReceiver;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32C;

/// Reassembles chunks and releases a Store binary only after commit validation.
///
/// The production Archive reader and the test-only live reader use this same
/// state machine. That keeps ordering, checksum, and cursor hand-off rules in
/// one place.
final class TransactionAssembler {
    /* ChunksWrapper requires a direct buffer even for an empty binary.  Reuse one
     * immutable zero-capacity view instead of allocating native memory per empty
     * transaction. */
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(0);
    private final AeronReplicationConfiguration configuration;
    private final UUID clusterId;
    private final long epoch;
    private final StorageBinaryDataReceiver receiver;
    private final Runnable transactionResolved;
    private final ReaderDeliveryListener deliveryListener;
    private final AtomicLong lastResolvedSequence = new AtomicLong();
    /* Materialisation can succeed before the durable cursor callback completes.
     * Keep that observation separate for health/lag reporting. */
    private final AtomicLong lastAppliedSequence = new AtomicLong();
    private final AtomicLong lastResolvedPosition = new AtomicLong();
    private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
    private final AeronReplicationEnvelope.EnvelopeView envelopeView =
            new AeronReplicationEnvelope.EnvelopeView();
    private final Delivery delivery = new Delivery();
    /* The subscription callback is single-threaded. Reuse the checksum state and
     * copy scratch across transactions instead of allocating both for every
     * transaction, including duplicate replay validation. */
    private final CRC32C dataCrc = new CRC32C();
    private final byte[] crcScratch = new byte[16 * 1024];
    /* Dictionaries repeat nearly verbatim across transactions. Decode the
     * assembled direct range with one reused decoder instead of copying it
     * through a per-transaction heap array first. Only the API-required
     * String itself still allocates. */
    private final java.nio.charset.CharsetDecoder dictionaryDecoder =
            StandardCharsets.UTF_8.newDecoder();
    /* The next sequence is reserved while the assembler monitor is held. It
     * closes the gap between accepting a terminal marker and invoking the
     * receiver callback (which deliberately runs outside the monitor). */
    private long nextExpectedSequence;
    /* The commit/abort witness for the last resolved sequence. A resumed
     * assembler has no witness until it resolves one message locally. These
     * fields are accessed only on the subscription owner thread; cursorSnapshot()
     * is the synchronized cross-thread boundary. */
    private int lastResolutionCrc32c;
    private AeronReplicationEnvelope.Kind lastResolutionKind;
    private int lastResolutionDataLength;
    private int lastResolutionDataChunkCount;
    private int lastResolutionDictionaryLength;
    private int lastResolutionDictionaryChunkCount;
    private Transaction transaction;

    TransactionAssembler(
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final StorageBinaryDataReceiver receiver
    ) {
        this(configuration, clusterId, epoch, -1, receiver, () -> {
        });
    }

    TransactionAssembler(
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence,
            final StorageBinaryDataReceiver receiver
    ) {
        this(configuration, clusterId, epoch, initialSequence, receiver, () -> {
        });
    }

    TransactionAssembler(
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence,
            final StorageBinaryDataReceiver receiver,
            final Runnable transactionResolved
    ) {
        this(configuration, clusterId, epoch, initialSequence, receiver, transactionResolved, null);
    }

    TransactionAssembler(
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence,
            final StorageBinaryDataReceiver receiver,
            final Runnable transactionResolved,
            final ReaderDeliveryListener deliveryListener
    ) {
        this(configuration, clusterId, epoch, initialSequence, -1L, receiver, transactionResolved,
                deliveryListener);
    }

    TransactionAssembler(
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence,
            final long initialPosition,
            final StorageBinaryDataReceiver receiver,
            final Runnable transactionResolved,
            final ReaderDeliveryListener deliveryListener
    ) {
        this.configuration = configuration;
        this.clusterId = clusterId;
        this.epoch = epoch;
        this.receiver = receiver;
        this.transactionResolved = transactionResolved;
        this.deliveryListener = deliveryListener;
        if (initialSequence < -1 || initialSequence == Long.MAX_VALUE || initialPosition < -1) {
            throw new IllegalArgumentException("initial cursor must be sequence >= -1 and position >= -1");
        }
        this.lastResolvedSequence.set(initialSequence);
        this.lastAppliedSequence.set(initialSequence);
        this.lastResolvedPosition.set(initialPosition);
        this.nextExpectedSequence = initialSequence + 1;
    }

    void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        try {
            /* A single reusable Delivery carries the detached transaction. The Aeron
             * subscription normally invokes this callback on one polling thread. Keep
             * the delivery monitor around both detachment and execution as a hard
             * boundary for direct/concurrent callers, while the assembler monitor is
             * still released before Store import and fsync. */
            synchronized (this.delivery) {
                final boolean deliver;
                synchronized (this) {
                    final AeronReplicationEnvelope.EnvelopeView envelope =
                            AeronReplicationEnvelope.decodeView(buffer, offset, length, this.envelopeView);
                    if (this.failure.get() != null) return;
                    deliver = this.accept(envelope, header == null ? -1 : header.position());
                }
                if (deliver) this.delivery.run();
            }
        } catch (final RuntimeException e) {
            this.failure(e);
            throw e;
        } catch (final Error e) {
            this.failure(new IllegalStateException("Aeron envelope delivery failed", e));
            throw e;
        }
    }

    private boolean accept(final AeronReplicationEnvelope.EnvelopeView envelope, final long position) {
        if (!envelope.matches(this.clusterId) || this.epoch != envelope.epoch()) {
            throw new IllegalArgumentException("cluster or epoch mismatch");
        }
        final long lastResolvedSequence = this.lastResolvedSequence.get();
        if (envelope.sequence() < lastResolvedSequence) {
            throw new IllegalStateException("replication sequence regressed: last resolved %s, received %s".formatted(lastResolvedSequence, envelope.sequence()));
        }
        if (envelope.sequence() == lastResolvedSequence) {
            if (envelope.kind() != AeronReplicationEnvelope.Kind.COMMIT &&
                envelope.kind() != AeronReplicationEnvelope.Kind.ABORT) {
                if (this.lastResolutionKind != AeronReplicationEnvelope.Kind.COMMIT ||
                    (envelope.kind() == AeronReplicationEnvelope.Kind.STORE_BINARY &&
                     (envelope.payloadLength() != this.lastResolutionDataLength ||
                      envelope.chunkCount() != this.lastResolutionDataChunkCount)) ||
                    (envelope.kind() == AeronReplicationEnvelope.Kind.TYPE_DICTIONARY &&
                     (envelope.payloadLength() != this.lastResolutionDictionaryLength ||
                      envelope.chunkCount() != this.lastResolutionDictionaryChunkCount))) {
                    throw new IllegalStateException("replayed data does not match the resolved transaction %s".formatted(lastResolvedSequence));
                }
                if (this.transaction == null) {
                    this.transaction = new Transaction(envelope.sequence(), this.configuration.maxTransactionBytes(), true,
                            this.dataCrc, this.crcScratch);
                }
                if (this.transaction.sequence != envelope.sequence() || !this.transaction.duplicate)
                    throw new IllegalStateException("interleaved replayed transaction");
                this.transaction.add(envelope);
                return false;
            }
            if (this.lastResolutionKind == null) {
                /* A resumed reader has only a cursor, not the terminal witness.  It
                 * must not silently accept a contradictory terminal at that cursor;
                 * restart from the persisted position instead. */
                throw new IllegalStateException("terminal witness is unavailable for resolved sequence %s".formatted(lastResolvedSequence));
            }
            if (envelope.kind() != this.lastResolutionKind) {
                throw new IllegalStateException("duplicate terminal has a different kind: expected %s, received %s".formatted(this.lastResolutionKind, envelope.kind()));
            }
            if (envelope.payloadLength() != this.lastResolutionDataLength ||
                envelope.chunkCount() != this.lastResolutionDataChunkCount) {
                throw new IllegalStateException("duplicate terminal has different transaction metadata");
            }
            if (envelope.kind() == AeronReplicationEnvelope.Kind.COMMIT &&
                envelope.commitCrc32c() != this.lastResolutionCrc32c) {
                throw new IllegalStateException("duplicate commit has a different payload checksum");
            }
            if (this.transaction != null) {
                if (!this.transaction.duplicate) throw new IllegalStateException("interleaved resolved transaction");
                this.validateDuplicateCommit(envelope);
                this.transaction.dispose();
                this.transaction = null;
            }
            return false;
        }
        if (envelope.sequence() != this.nextExpectedSequence) {
            throw new IllegalStateException(
                    "replication sequence gap: expected %s, received %s".formatted(this.nextExpectedSequence, envelope.sequence()));
        }
        if (envelope.kind() == AeronReplicationEnvelope.Kind.COMMIT) {
            this.commit(envelope, position);
            return true;
        }
        if (envelope.kind() == AeronReplicationEnvelope.Kind.ABORT) {
            /* decodeView() already enforces this canonical form. Keep the semantic
             * check at the state-machine boundary too: a future decoder or test
             * adapter must not turn an abort with a commit witness into a valid
             * terminal decision. */
            if (envelope.commitCrc32c() != 0) {
                throw new IllegalStateException("abort marker carries a non-zero commit checksum");
            }
            if (this.transaction != null) {
                this.transaction.dispose();
                this.transaction = null;
            }
            this.nextExpectedSequence = envelope.sequence() + 1;
            this.delivery.prepare(null, null, null, envelope.sequence(), position, envelope.payloadLength(),
                    envelope.chunkCount(), 0, AeronReplicationEnvelope.Kind.ABORT);
            return true;
        }
        if (envelope.kind() != AeronReplicationEnvelope.Kind.TYPE_DICTIONARY &&
            envelope.kind() != AeronReplicationEnvelope.Kind.STORE_BINARY) {
            throw new IllegalArgumentException("non-data envelope on replication data stream: %s".formatted(envelope.kind()));
        }
        if (this.transaction == null) {
            this.transaction = new Transaction(envelope.sequence(), this.configuration.maxTransactionBytes(), false,
                    this.dataCrc, this.crcScratch);
        }
        if (this.transaction.sequence != envelope.sequence()) {
            throw new IllegalStateException("interleaved replication transaction");
        }
        this.transaction.add(envelope);
        return false;
    }

    private void validateDuplicateCommit(final AeronReplicationEnvelope.EnvelopeView envelope) {
        final Transaction duplicate = this.transaction;
        if (duplicate.dataLength != envelope.payloadLength() ||
            duplicate.dataChunkCount != envelope.chunkCount() ||
            duplicate.dataNextChunk != duplicate.dataChunkCount ||
            duplicate.dataOffset != duplicate.dataLength ||
            (duplicate.dictionary != null &&
             (duplicate.dictionaryOffset != duplicate.dictionaryLength ||
              duplicate.dictionaryNextChunk != duplicate.dictionaryChunkCount)) ||
            duplicate.dataCrc32c() != envelope.commitCrc32c()) {
            throw new IllegalStateException("replayed transaction does not match its resolved commit");
        }
    }

    private void commit(final AeronReplicationEnvelope.EnvelopeView envelope, final long position) {
        /* The writer always precedes a commit with at least one data envelope,
         * even for an empty binary, so a bare commit marker is a protocol
         * violation rather than an empty transaction. Fail closed instead of
         * inventing data the log never carried. */
        if (this.transaction == null) {
            throw new IllegalStateException("commit without data chunks");
        }
        if (this.transaction.dataLength != envelope.payloadLength() ||
            this.transaction.dataChunkCount != envelope.chunkCount() ||
            this.transaction.dataNextChunk != this.transaction.dataChunkCount ||
            this.transaction.dataOffset != this.transaction.dataLength ||
            this.transaction.dictionary != null &&
            (this.transaction.dictionaryOffset != this.transaction.dictionaryLength ||
             this.transaction.dictionaryNextChunk != this.transaction.dictionaryChunkCount) ||
            this.transaction.dataCrc32c() !=
            envelope.commitCrc32c()) {
            this.transaction.dispose();
            this.transaction = null;
            throw new IllegalStateException("commit does not match assembled Store binary");
        }
        final Transaction completed = this.transaction;
        final String dictionary;
        if (completed.dictionary == null) {
            dictionary = null;
        } else {
            final ByteBuffer view = completed.dictionaryStorage.asReadOnlyBuffer();
            view.position(0).limit(completed.dictionaryLength);
            try {
                dictionary = this.dictionaryDecoder.reset().decode(view).toString();
            } catch (final java.nio.charset.CharacterCodingException failure) {
                throw new IllegalStateException("type dictionary is not valid UTF-8", failure);
            }
        }
        final ByteBuffer direct = completed.dataStorage == null
                ? EMPTY_BUFFER.duplicate()
                : completed.dataStorage;
        if (completed.dataStorage == null) {
            /* Keep the direct-buffer contract required by ChunksWrapper while
             * representing an actually empty Store binary. */
            direct.clear();
            direct.limit(0);
        } else {
            /* ChunksWrapper uses the source position as its logical length. The
             * buffer was filled from offset zero, so expose the completed write
             * position while retaining the exact limit used by Store import. */
            direct.limit(completed.dataLength);
            direct.position(completed.dataLength);
        }
        this.transaction = null;
        this.nextExpectedSequence = envelope.sequence() + 1;
        this.delivery.prepare(dictionary, direct, completed, envelope.sequence(), position, envelope.payloadLength(),
                envelope.chunkCount(), envelope.commitCrc32c(), AeronReplicationEnvelope.Kind.COMMIT);
    }

    long lastResolvedSequence() {
        return this.lastResolvedSequence.get();
    }

    long lastAppliedSequence() {
        return this.lastAppliedSequence.get();
    }

    UUID clusterId() {
        return this.clusterId;
    }

    long epoch() {
        return this.epoch;
    }

    long lastResolvedPosition() {
        return this.lastResolvedPosition.get();
    }

    synchronized CursorSnapshot cursorSnapshot() {
        return new CursorSnapshot(this.lastResolvedSequence.get(), this.lastResolvedPosition.get());
    }

        /// Returns whether chunks are waiting for a terminal marker.
    synchronized boolean hasIncompleteTransaction() {
        return this.transaction != null;
    }

    RuntimeException failure() {
        return this.failure.get();
    }

    void failure(final RuntimeException exception) {
        Objects.requireNonNull(exception, "exception");
        synchronized (this.delivery) {
            synchronized (this) {
                if (this.failure.compareAndSet(null, exception)) {
                    if (this.transaction != null) {
                        this.transaction.dispose();
                        this.transaction = null;
                    }
                }
            }
        }
    }

        /// Releases native buffers retained by an incomplete transaction.
    void dispose() {
        synchronized (this.delivery) {
            synchronized (this) {
                if (this.transaction != null) {
                    this.transaction.dispose();
                    this.transaction = null;
                }
            }
        }
    }

        /// Holds fragments and commit metadata for one transaction.
    static final class Transaction {
        private final long sequence;
        private final int maxBytes;
        private final boolean duplicate;
        private final CRC32C dataCrc;
        private final byte[] crcScratch;
        private UnsafeBuffer dictionary;
        private ByteBuffer dictionaryStorage;
        private UnsafeBuffer data;
        private ByteBuffer dataStorage;
        private int dictionaryOffset;
        private int dataOffset;
        private int dictionaryNextChunk;
        private int dataNextChunk;
        private int dictionaryChunkCount = -1;
        private int dataChunkCount = -1;
        private int dictionaryLength;
        private int dataLength;

        Transaction(final long sequence, final int maxBytes, final boolean duplicate,
                    final CRC32C dataCrc, final byte[] crcScratch) {
            this.sequence = sequence;
            this.maxBytes = maxBytes;
            this.duplicate = duplicate;
            this.dataCrc = dataCrc;
            this.crcScratch = crcScratch;
            this.dataCrc.reset();
        }

        void add(final AeronReplicationEnvelope.EnvelopeView envelope) {
            final int payloadLength = envelope.payloadLength();
            final int wireLength = envelope.payloadLengthOnWire();
            if (payloadLength > this.maxBytes) throw new IllegalArgumentException("payload exceeds maxTransactionBytes");
            final boolean dictionary = envelope.kind() == AeronReplicationEnvelope.Kind.TYPE_DICTIONARY;
            if (dictionary && this.dataNextChunk != 0) throw new IllegalStateException("type dictionary follows Store binary chunks");
            if (!dictionary && this.dictionary != null && this.dictionaryNextChunk != this.dictionaryChunkCount) {
                throw new IllegalStateException("Store binary chunk arrived before type dictionary completed");
            }
            if (dictionary && (long) payloadLength + this.dataLength > this.maxBytes ||
                !dictionary && this.dictionary != null && (long) payloadLength + this.dictionaryLength > this.maxBytes) {
                throw new IllegalArgumentException("dictionary and Store payload exceed maxTransactionBytes");
            }
            final int expectedCount = dictionary ? this.dictionaryChunkCount : this.dataChunkCount;
            if (expectedCount != -1 && expectedCount != envelope.chunkCount()) throw new IllegalArgumentException("chunk count changed within transaction");
            if (envelope.chunkIndex() != (dictionary ? this.dictionaryNextChunk : this.dataNextChunk)) throw new IllegalStateException("unexpected chunk index");
            final int offset = dictionary ? this.dictionaryOffset : this.dataOffset;
            if (envelope.chunkOffset() != offset || (long) envelope.chunkOffset() + wireLength > payloadLength) {
                throw new IllegalArgumentException("non-contiguous or oversized chunk");
            }
            if (dictionary) {
                if (this.dictionary == null) {
                    this.dictionaryLength = payloadLength;
                    if (payloadLength != 0) {
                        this.dictionaryStorage = XMemory.allocateDirectNative(payloadLength);
                        this.dictionary = new UnsafeBuffer(this.dictionaryStorage);
                    }
                }
                if (this.dictionaryLength != payloadLength) throw new IllegalArgumentException("dictionary length changed within transaction");
                if (wireLength != 0) {
                    if (this.dictionary == null) throw new IllegalStateException("dictionary storage is unavailable");
                    this.dictionary.putBytes(offset, envelope.source(), envelope.payloadOffset(), wireLength);
                }
                this.dictionaryOffset += wireLength;
                this.dictionaryNextChunk++;
                this.dictionaryChunkCount = envelope.chunkCount();
            } else {
                if (this.data == null && payloadLength != 0) {
                    this.dataStorage = XMemory.allocateDirectNative(payloadLength);
                    this.data = new UnsafeBuffer(this.dataStorage);
                }
                if (this.dataLength != 0 && this.dataLength != payloadLength) throw new IllegalArgumentException("Store binary length changed within transaction");
                this.dataLength = payloadLength;
                if (wireLength != 0) {
                    if (this.data == null) throw new IllegalStateException("Store data storage is unavailable");
                    this.data.putBytes(offset, envelope.source(), envelope.payloadOffset(), wireLength);
                    this.updateDataCrc(envelope.source(), envelope.payloadOffset(), wireLength);
                }
                this.dataOffset += wireLength;
                this.dataNextChunk++;
                this.dataChunkCount = envelope.chunkCount();
            }
        }

        private void updateDataCrc(final DirectBuffer source, final int offset, final int length) {
            for (int copied = 0; copied < length; ) {
                final int amount = Math.min(this.crcScratch.length, length - copied);
                source.getBytes(offset + copied, this.crcScratch, 0, amount);
                this.dataCrc.update(this.crcScratch, 0, amount);
                copied += amount;
            }
        }

        int dataCrc32c() {
            return (int) this.dataCrc.getValue();
        }

        void dispose() {
            this.dispose(false);
        }

        void dispose(final boolean dataTransferred) {
            if (this.dictionaryStorage != null) {
                XMemory.deallocateDirectByteBuffer(this.dictionaryStorage);
                this.dictionaryStorage = null;
            }
            if (!dataTransferred && this.dataStorage != null) {
                XMemory.deallocateDirectByteBuffer(this.dataStorage);
                this.dataStorage = null;
            }
            this.dictionary = null;
            this.data = null;
        }

        void detachDataStorage() {
            this.dataStorage = null;
            this.data = null;
        }
    }

        /// Delivers one validated transaction outside the assembler monitor.
    private final class Delivery {
        private String dictionary;
        private ByteBuffer data;
        private Transaction completed;
        private long sequence;
        private long position;
        private int resolutionDataLength;
        private int resolutionDataChunkCount;
        private int resolutionCrc32c;
        private AeronReplicationEnvelope.Kind resolutionKind;

        void prepare(final String dictionary, final ByteBuffer data, final Transaction completed,
                     final long sequence, final long position, final int resolutionDataLength,
                     final int resolutionDataChunkCount, final int resolutionCrc32c,
                     final AeronReplicationEnvelope.Kind resolutionKind) {
            this.dictionary = dictionary;
            this.data = data;
            this.completed = completed;
            this.sequence = sequence;
            this.position = position;
            this.resolutionDataLength = resolutionDataLength;
            this.resolutionDataChunkCount = resolutionDataChunkCount;
            this.resolutionCrc32c = resolutionCrc32c;
            this.resolutionKind = resolutionKind;
        }

        void run() {
            boolean dataTransferred = false;
            try {
                if (this.dictionary != null) receiver.receiveTypeDictionary(this.dictionary);
                if (this.data != null) {
                    final int dataLength = this.completed.dataLength;
                    final int dataChunkCount = this.completed.dataChunkCount;
                    if (deliveryListener != null) {
                        deliveryListener.beforeStoreImport(
                                this.sequence, this.position, dataLength, dataChunkCount, this.resolutionCrc32c);
                    }
                    final org.eclipse.serializer.persistence.binary.types.Binary binary = ChunksWrapper.New(this.data);
                    if (receiver.canReceiveDataOwned()) {
                        /* The owned receiver releases the original buffer on every path,
                         * including a failure thrown from receiveDataOwned or awaitApplied. */
                        this.completed.detachDataStorage();
                        dataTransferred = true;
                        receiver.receiveDataOwned(binary);
                    } else {
                        dataTransferred = receiver.receiveDataOwned(binary);
                        if (dataTransferred) this.completed.detachDataStorage();
                    }
                    receiver.awaitApplied();
                }
                synchronized (TransactionAssembler.this) {
                    /* An abort resolves the replication cursor but does not materialise a
                     * Store image.  Keep the two observations distinct so health/lag
                     * callers never report an aborted sequence as applied data. */
                    if (this.resolutionKind == AeronReplicationEnvelope.Kind.COMMIT) {
                        lastAppliedSequence.set(this.sequence);
                    }
                    lastResolvedSequence.set(this.sequence);
                    lastResolvedPosition.set(this.position);
                    lastResolutionCrc32c = this.resolutionCrc32c;
                    lastResolutionKind = this.resolutionKind;
                    lastResolutionDataLength = this.resolutionDataLength;
                    lastResolutionDataChunkCount = this.resolutionDataChunkCount;
                    lastResolutionDictionaryLength = this.completed == null ? 0 : this.completed.dictionaryLength;
                    lastResolutionDictionaryChunkCount = this.completed == null ? 0 : this.completed.dictionaryChunkCount;
                }
                transactionResolved.run();
                if (this.data != null && deliveryListener != null) {
                    deliveryListener.afterStoreImport();
                }
            } finally {
                if (this.completed != null) this.completed.dispose(dataTransferred);
                this.dictionary = null;
                this.data = null;
                this.completed = null;
                this.resolutionKind = null;
            }
        }
    }
}
