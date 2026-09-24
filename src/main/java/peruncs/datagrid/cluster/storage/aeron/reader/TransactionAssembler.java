package peruncs.datagrid.cluster.storage.aeron.reader;

import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.storage.binary.StorageBinaryDataReceiver;

import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
    private final long wireNonce;
    private final long epoch;
    private final StorageBinaryDataReceiver receiver;
    private final Consumer<CursorSnapshot> transactionResolved;
    private final ReaderDeliveryListener deliveryListener;
    /* The durable boundary is one immutable snapshot so a concurrent
     * status or cursor reader can never observe a new sequence paired with
     * the previous position (or vice versa). */
    private volatile CursorSnapshot resolvedBoundary;
    /* Materialization can succeed before the durable cursor callback completes.
     * Keep that observation separate for health/lag reporting. */
    private volatile long lastAppliedSequence;
    /* Greatest writer fencing token accepted so far. A lower token proves the
     * frame comes from a deposed writer that lost the lease race; it fails
     * closed instead of interleaving stale history. Seeded from the durable
     * cursor at startup so a restart never re-accepts superseded history.
     * Raised only by fully accepted terminal frames (data-chunk tokens are
     * adopted at commit time), never before validation. */
    private volatile long lastAcceptedFencingToken;
    private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
    private final AeronReplicationEnvelope.EnvelopeView envelopeView =
            new AeronReplicationEnvelope.EnvelopeView();
    private final Delivery delivery = new Delivery();
    /* The subscription callback is single-threaded. Reuse the checksum state and
     * copy scratch across transactions instead of allocating both for every
     * transaction, including duplicate replay validation. */
    private final CRC32C dataCrc = new CRC32C();
    /* Dictionaries repeat nearly verbatim across transactions. Decode the
     * assembled direct range with one reused decoder instead of copying it
     * through a per-transaction heap array first. Only the API-required
     * String itself still allocates. */
    private final CharsetDecoder dictionaryDecoder = StandardCharsets.UTF_8.newDecoder();
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
    /* Delivery batching state. Every resolved transaction is staged onto
     * `pendingDeliveries` and published to cursor/listener callbacks only when
     * the barrier flushes: at the configured window size, at an idle poll, or
     * at a lifecycle stop. The barrier turns the per-transaction fsync storm —
     * one Store import, one uncertainty-marker write, and one forced cursor
     * write per transaction — into one of each per barrier, which is what
     * makes backlog replay fast. Durability is unchanged: the marker is
     * written before the first staged import of the batch can reach the Store
     * and is deleted only after the barrier's cursor is forced, so a crash
     * either keeps both (replay resumes at the cursor) or keeps only the
     * marker (fail-closed reseed). All fields are guarded by the delivery
     * monitor; only the polling thread ever stages or flushes. */
    private final ArrayDeque<PendingDelivery> pendingDeliveries = new ArrayDeque<>();
    private boolean deliveryMarkerOpen;
    /* Set when any staged entry carries a Store binary; the flush waits for
     * materialization only then — abort-only barriers enqueue nothing. */
    private boolean barrierHasData;
    private long barrierBytes;
    /* Dedicated monitor for the barrier staging queue. The flush parks in the
     * receiver's awaitApplied() for the whole batch; it must never hold the
     * delivery monitor across that park, because dispose() is specified to
     * return while a delivery is parked (see TransactionAssemblerFailureTest).
     * The staging lock order is therefore: delivery monitor outside, barrier
     * monitor inside; the flush only parks on the barrier monitor… no — even
     * the barrier monitor is released during the await. All producers and the
     * flush run on the polling thread; only dispose() touches this monitor
     * from another thread, to drop staged entries. */
    private final Object barrierLock = new Object();

    /// Creates the one production assembler state machine.
    ///
    /// Callers pass the wire nonce explicitly so a fixture or a deployment can
    /// never silently derive a nonce while its peer uses another. Test code
    /// builds assemblers through `TransactionAssemblerTestSupport` factories
    /// that fill in fixture defaults.
    ///
    /// @param configuration      framing and timeout limits shared with the writer
    /// @param clusterId          expected cluster identity
    /// @param epoch              expected writer epoch
    /// @param initialSequence    last sequence already resolved, or `-1` before the first
    /// @param initialPosition    last resolved Archive position, or `-1` before the first
    /// @param receiver           destination for complete Store binaries
    /// @param transactionResolved callback after a delivery barrier resolves durably; never `null`
    /// @param deliveryListener   callback around Store materialization, or `null`
    /// @param wireNonce          expected accidental-cross-wiring nonce; must not be zero
    TransactionAssembler(
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence,
            final long initialPosition,
            final StorageBinaryDataReceiver receiver,
            final Consumer<CursorSnapshot> transactionResolved,
            final ReaderDeliveryListener deliveryListener,
            final long wireNonce
    ) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        if (wireNonce == 0L) throw new IllegalArgumentException("wireNonce must not be zero");
        this.wireNonce = wireNonce;
        this.epoch = epoch;
        this.receiver = Objects.requireNonNull(receiver, "receiver");
        this.transactionResolved = Objects.requireNonNull(transactionResolved, "transactionResolved");
        this.deliveryListener = deliveryListener;
        if (initialSequence < -1 || initialSequence == Long.MAX_VALUE || initialPosition < -1) {
            throw new IllegalArgumentException("initial cursor must be sequence >= -1 and position >= -1");
        }
        this.resolvedBoundary = new CursorSnapshot(initialSequence, initialPosition);
        this.lastAppliedSequence = initialSequence;
        this.nextExpectedSequence = initialSequence + 1;
        /* ScopedValue bindings are not inherited by the virtual-thread poller
         * (final Scoped Values dropped inheritance), so the hook must be
         * captured here, on the thread that constructs the assembler, while
         * the caller's binding is still dynamically visible. */
        this.chunkObserver = CHUNK_HOOK.isBound() ? CHUNK_HOOK.get() : null;
    }

    /// Consumes one fragment from the subscription callback.
    ///
    /// The delivery monitor bounds detachment and execution to one delivery at
    /// a time, but it is never acquired by the failure path: a Store import can
    /// hold it for seconds. A latched failure is observed here, on the polling
    /// thread, and releases any incomplete transaction before returning.
    ///
    /// @param buffer fragment source
    /// @param offset fragment offset
    /// @param length fragment length
    /// @param header Aeron header, or `null` in direct tests
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
                    if (this.failure.get() != null) {
                        this.releaseIncompleteTransaction();
                        return;
                    }
                    final AeronReplicationEnvelope.EnvelopeView envelope =
                            AeronReplicationEnvelope.decodeView(buffer, offset, length, this.envelopeView);
                    if (this.failure.get() != null) {
                        this.releaseIncompleteTransaction();
                        return;
                    }
                    deliver = this.accept(envelope, header == null ? -1 : header.position());
                }
                if (deliver) {
                    /* Stage only: the blocking materialization wait must run
                     * outside the fragment callback, or a full barrier would
                     * stall the whole controlledPoll. The reader breaks the
                     * poll when the barrier fills and flushes from its own
                     * loop; an incomplete barrier flushes on the next idle
                     * poll or lifecycle stop. */
                    this.delivery.stage();
                }
            }
        } catch (final RuntimeException e) {
            this.failure(e);
            this.releaseIncompleteTransaction();
            throw e;
        } catch (final Error e) {
            this.failure(new IllegalStateException("Aeron envelope delivery failed", e));
            this.releaseIncompleteTransaction();
            throw e;
        }
    }

    private boolean accept(final AeronReplicationEnvelope.EnvelopeView envelope, final long position) {
        if (!envelope.matches(this.clusterId, this.wireNonce) || this.epoch != envelope.epoch()) {
            throw new IllegalArgumentException("cluster or epoch mismatch");
        }
        /* Reject stale tokens on every frame, but raise the floor only after a
         * frame is fully accepted below. Raising it here would let a poison
         * frame with a higher token but an invalid sequence or checksum lift
         * the floor before validation fails; that poisoned floor would then
         * persist through checkpointed cursors. Data chunks therefore adopt
         * their token at commit time, when the whole transaction validates. */
        final long token = envelope.fencingToken();
        final long floor = this.lastAcceptedFencingToken;
        if (token < floor) {
            throw new IllegalStateException(
                    "stale writer fencing token %s below accepted %s; the writer lost the lease race".formatted(token, floor));
        }
        final long lastResolvedSequence = this.resolvedBoundary.sequence();
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
                    this.transaction = new Transaction(envelope.sequence(), envelope.fencingToken(),
                            this.configuration.maxTransactionBytes(), true,
                            this.dataCrc);
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
            this.adoptFencingToken(token);
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
            this.adoptFencingToken(token);
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
            this.transaction = new Transaction(envelope.sequence(), envelope.fencingToken(),
                    this.configuration.maxTransactionBytes(), false,
                    this.dataCrc);
        }
        if (this.transaction.sequence != envelope.sequence()) {
            throw new IllegalStateException("interleaved replication transaction");
        }
        this.transaction.add(envelope);
        /* Crash-test observation point after a data chunk is buffered: the
         * natural-memory state of a partially assembled transaction is
         * otherwise unreachable from forked children. The hook fires only for
         * multi-chunk transactions, so single-chunk cells never park on it. */
        final ChunkObserver observer = this.chunkObserver;
        if (observer != null && envelope.chunkCount() > 1) {
            observer.afterChunkBuffered(envelope.sequence(), envelope.chunkIndex(), envelope.chunkCount());
        }
        return false;
    }

        /// Observation hook for crash tests: fires after one data chunk of a
    /// multi-chunk transaction has been buffered.
    @FunctionalInterface
    interface ChunkObserver {
        /// Reports one buffered chunk.
        ///
        /// @param sequence   transaction sequence
        /// @param chunkIndex buffered chunk index
        /// @param chunkCount transaction chunk count
        void afterChunkBuffered(long sequence, int chunkIndex, int chunkCount);
    }

    private static final ScopedValue<ChunkObserver> CHUNK_HOOK = ScopedValue.newInstance();
    /// Crash-test hook captured at construction; `null` in production.
    private final ChunkObserver chunkObserver;

        /// Runs an action with the chunk observer bound to its dynamic scope.
    ///
    /// The binding must surround the reader CONSTRUCTION, not its polling:
    /// scoped values are captured when the [TransactionAssembler] is built on
    /// the caller thread, because final [ScopedValue] bindings are not
    /// inherited by the reader's virtual-thread poller. Test bridge only;
    /// unbound in production, and the assembler never allocates for the hook
    /// when it is unbound.
    ///
    /// @param observer hook invoked after each buffered multi-chunk chunk
    /// @param action  action to run with the hook bound
    /// @return the action's result
    /// @param <T>    action result type
    static <T> T runWithChunkObserver(final ChunkObserver observer, final java.util.concurrent.Callable<T> action) {
        try {
            return ScopedValue.where(CHUNK_HOOK, observer).call(action::call);
        } catch (final RuntimeException failure) {
            throw failure;
        } catch (final Exception checked) {
            throw new IllegalStateException("chunk-observer action failed", checked);
        }
    }

    private void validateDuplicateCommit(final AeronReplicationEnvelope.EnvelopeView envelope) {
        final Transaction duplicate = this.transaction;
        if (duplicate.dataLength != envelope.payloadLength() ||
            duplicate.fencingToken != envelope.fencingToken() ||
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
        if (this.transaction.fencingToken != envelope.fencingToken() ||
            this.transaction.dataLength != envelope.payloadLength() ||
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
        /* The frame is fully accepted only now: every length, order, checksum,
         * and dictionary-encoding claim validated. Adopt its fencing token here
         * so a poison frame can never lift the floor that cursors persist. */
        this.adoptFencingToken(envelope.fencingToken());
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

        /// Raises the stale-token floor after a frame is fully accepted.
    ///
    /// Data chunks never call this directly; their token is adopted by the
    /// commit that validates the assembled transaction.
    private void adoptFencingToken(final long token) {
        if (token > this.lastAcceptedFencingToken) {
            this.lastAcceptedFencingToken = token;
        }
    }

        /// Returns the last sequence resolved by a terminal marker.
    ///
    /// @return last resolved transaction sequence, or the initial value
    long lastResolvedSequence() {
        return this.resolvedBoundary.sequence();
    }

        /// Returns the last sequence materialized by the Store receiver.
    ///
    /// @return last applied transaction sequence, or the initial value
    long lastAppliedSequence() {
        return this.lastAppliedSequence;
    }

        /// Returns the expected cluster identity.
    ///
    /// @return cluster identity enforced on every frame
    UUID clusterId() {
        return this.clusterId;
    }

        /// Returns the expected writer epoch.
    ///
    /// @return writer epoch enforced on every frame
    long epoch() {
        return this.epoch;
    }

        /// Seeds the stale-token floor from the durable cursor before any frame is accepted.
    ///
    /// @param fencingToken greatest token the persisted cursor accepted, or `0` for a new reader
    void startingFencingToken(final long fencingToken) {
        if (fencingToken < 0) {
            throw new IllegalArgumentException("starting fencing token must not be negative");
        }
        this.lastAcceptedFencingToken = fencingToken;
    }

        /// Returns the greatest writer fencing token accepted so far.
    ///
    /// @return greatest accepted fencing token, or the seeded floor
    long fencingToken() {
        return this.lastAcceptedFencingToken;
    }

        /// Returns the Archive position of the last resolved transaction.
    ///
    /// @return last resolved Archive position, or the initial value
    long lastResolvedPosition() {
        return this.resolvedBoundary.position();
    }

    /// Returns whether the current delivery barrier has reached either limit.
    synchronized boolean deliveryBarrierFull() {
        return this.unflushedDeliveryCount() >= this.configuration.readerBarrierMaxTransactions()
                || this.unflushedDeliveryBytes() >= this.configuration.maxTransactionBytes();
    }

        /// Returns an atomic sequence and position snapshot for cursor persistence.
    ///
    /// @return consistent cursor snapshot; atomic by construction
    CursorSnapshot cursorSnapshot() {
        return this.resolvedBoundary;
    }

        /// Returns whether chunks are waiting for a terminal marker.
    ///
    /// @return `true` while an incomplete transaction retains native buffers
    synchronized boolean hasIncompleteTransaction() {
        return this.transaction != null;
    }

        /// Returns the latched terminal failure, or `null` while healthy.
    ///
    /// @return terminal failure, or `null`
    RuntimeException failure() {
        return this.failure.get();
    }

        /// Latches the terminal failure without waiting for an in-flight delivery.
    ///
    /// Aeron invokes this from its client conductor error handler, where a
    /// Store import that holds the delivery monitor for seconds would block the
    /// conductor past the driver timeout and tear down the whole client. The
    /// compare-and-set is therefore the whole operation: the polling thread
    /// releases an incomplete transaction when it next observes the failure,
    /// and [dispose] releases one if polling never resumes.
    ///
    /// @param exception terminal failure to latch
    void failure(final RuntimeException exception) {
        Objects.requireNonNull(exception, "exception");
        this.failure.compareAndSet(null, exception);
    }

        /// Releases native buffers retained by an incomplete transaction.
    ///
    /// This only acquires the assembler monitor, whose critical section never
    /// covers Store import, so disposal cannot be blocked by a slow receiver.
    /// An in-flight [Delivery] owns its detached transaction and is unaffected.
    void dispose() {
        this.releaseIncompleteTransaction();
        this.discardPendingDeliveries();
    }

    private void releaseIncompleteTransaction() {
        synchronized (this) {
            if (this.transaction != null) {
                this.transaction.dispose();
                this.transaction = null;
            }
        }
    }

        /// Holds fragments and commit metadata for one transaction.
    static final class Transaction {
        private final long sequence;
        private final long fencingToken;
        private final int maxBytes;
        private final boolean duplicate;
        private final CRC32C dataCrc;
        private UnsafeBuffer dictionary;
        private ByteBuffer dictionaryStorage;
        private UnsafeBuffer data;
        private ByteBuffer dataStorage;
        /* One reusable view over [dataStorage] for incremental CRC updates. The
         * JDK ByteBuffer checksum API advances the buffer position, so hashing the
         * destination range through a stable duplicate avoids the per-chunk
         * duplicate that Crc32C.update would allocate for a direct source. */
        private ByteBuffer dataCrcView;
        private int dictionaryOffset;
        private int dataOffset;
        private int dictionaryNextChunk;
        private int dataNextChunk;
        private int dictionaryChunkCount = -1;
        private int dataChunkCount = -1;
        private int dictionaryLength;
        private int dataLength;

        Transaction(final long sequence, final long fencingToken, final int maxBytes, final boolean duplicate,
                    final CRC32C dataCrc) {
            this.sequence = sequence;
            this.fencingToken = fencingToken;
            this.maxBytes = maxBytes;
            this.duplicate = duplicate;
            this.dataCrc = dataCrc;
            this.dataCrc.reset();
        }

        /// Appends one validated chunk to the dictionary or the Store binary.
        ///
        /// The chunk must be contiguous with everything already buffered and
        /// must repeat the transaction's chunk count. Any mismatch fails the
        /// transaction instead of delivering a partial or reordered binary.
        ///
        /// @param envelope decoded data or dictionary chunk
        /// @throws IllegalStateException    when chunks interleave or repeat
        /// @throws IllegalArgumentException when bounds or lengths disagree
        void add(final AeronReplicationEnvelope.EnvelopeView envelope) {
            if (envelope.fencingToken() != this.fencingToken) {
                throw new IllegalStateException("transaction mixes writer fencing tokens");
            }
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
                        this.ensureCapacity(true, payloadLength);
                    }
                }
                if (this.dictionaryLength != payloadLength) throw new IllegalArgumentException("dictionary length changed within transaction");
                if (wireLength != 0) {
                    if (this.dictionary == null) throw new IllegalStateException("dictionary storage is unavailable");
                    this.ensureCapacity(true, this.dictionaryOffset + wireLength);
                    this.dictionary.putBytes(offset, envelope.source(), envelope.payloadOffset(), wireLength);
                }
                this.dictionaryOffset += wireLength;
                this.dictionaryNextChunk++;
                this.dictionaryChunkCount = envelope.chunkCount();
            } else {
                if (this.data == null && payloadLength != 0) {
                    this.ensureCapacity(false, payloadLength);
                }
                if (this.dataLength != 0 && this.dataLength != payloadLength) throw new IllegalArgumentException("Store binary length changed within transaction");
                this.dataLength = payloadLength;
                if (wireLength != 0) {
                    if (this.data == null) throw new IllegalStateException("Store data storage is unavailable");
                    this.ensureCapacity(false, this.dataOffset + wireLength);
                    this.data.putBytes(offset, envelope.source(), envelope.payloadOffset(), wireLength);
                    this.updateDataCrc(offset, wireLength);
                }
                this.dataOffset += wireLength;
                this.dataNextChunk++;
                this.dataChunkCount = envelope.chunkCount();
            }
        }

        private void updateDataCrc(final int storageOffset, final int length) {
            final ByteBuffer view = this.dataCrcView;
            view.clear();
            view.position(storageOffset).limit(storageOffset + length);
            this.dataCrc.update(view);
        }

        private void ensureCapacity(final boolean dictionary, final int required) {
            if (required <= 0) return;
            final ByteBuffer current = dictionary ? this.dictionaryStorage : this.dataStorage;
            if (current != null && current.capacity() >= required) return;
            final int oldCapacity = current == null ? 0 : current.capacity();
            final long doubled = oldCapacity == 0 ? Math.min(required, 64 * 1024L) : (long) oldCapacity * 2L;
            final int capacity = (int) Math.min(this.maxBytes, Math.max(required, doubled));
            final ByteBuffer replacementStorage = XMemory.allocateDirectNative(capacity);
            try {
                final UnsafeBuffer replacement = new UnsafeBuffer(replacementStorage);
                final int copied = dictionary ? this.dictionaryOffset : this.dataOffset;
                if (current != null && copied != 0) {
                    replacement.putBytes(0, dictionary ? this.dictionary : this.data, 0, copied);
                }
                if (dictionary) {
                    this.dictionaryStorage = replacementStorage;
                    this.dictionary = replacement;
                } else {
                    this.dataStorage = replacementStorage;
                    this.data = replacement;
                    this.dataCrcView = replacementStorage.duplicate();
                }
            } catch (final RuntimeException | Error failure) {
                XMemory.deallocateDirectByteBuffer(replacementStorage);
                throw failure;
            }
            if (current != null) XMemory.deallocateDirectByteBuffer(current);
        }

        /// Returns the incremental checksum of the assembled Store binary.
        ///
        /// @return CRC32C accumulated over every data chunk copied so far
        int dataCrc32c() {
            return (int) this.dataCrc.getValue();
        }

        /// Releases retained dictionary and Store buffers.
        void dispose() {
            this.dispose(false);
        }

        /// Releases retained buffers, optionally keeping the delivered Store storage.
        ///
        /// @param dataTransferred whether the receiver already owns the Store storage
        void dispose(final boolean dataTransferred) {
            if (this.dictionaryStorage != null) {
                XMemory.deallocateDirectByteBuffer(this.dictionaryStorage);
                this.dictionaryStorage = null;
            }
            if (!dataTransferred && this.dataStorage != null) {
                XMemory.deallocateDirectByteBuffer(this.dataStorage);
                this.dataStorage = null;
            }
            this.dataCrcView = null;
            this.dictionary = null;
            this.data = null;
        }

        /// Forgets the Store storage after an owned receiver took responsibility for it.
        void detachDataStorage() {
            this.dataStorage = null;
            this.data = null;
            this.dataCrcView = null;
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

        /// Stages one resolved transaction for delivery outside the assembler monitor.
        ///
        /// @param dictionary                 assembled type dictionary, or `null`
        /// @param data                       assembled Store binary storage, or `null` for abort
        /// @param completed                  transaction that owns the storage
        /// @param sequence                   terminal replication sequence
        /// @param position                   terminal Archive position
        /// @param resolutionDataLength       represented Store binary length
        /// @param resolutionDataChunkCount   represented Store chunk count
        /// @param resolutionCrc32c           commit checksum, or `0` for abort
        /// @param resolutionKind             terminal marker kind
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

        /// Stages the validated transaction for the current delivery barrier.
        ///
        /// The dictionary merge and the buffer hand-off run immediately; the
        /// durability flip — Store backpressure wait, resolved-sequence
        /// publication, resolved callback, and marker removal — is deferred to
        /// [TransactionAssembler#flushDeliveries()] so consecutive
        /// transactions replay in one barrier. The uncertainty marker is
        /// opened before the first staged import of a barrier and covers every
        /// later staged import of the same barrier; it is closed only after
        /// the barrier's cursor callbacks ran. A receiver failure leaves the
        /// marker open and the barrier unflushed, which restart treats
        /// fail-closed.
        ///
        /// The receiver hand-off runs without the assembler monitor, so a slow
        /// Store never blocks failure latching; the barrier bookkeeping runs
        /// under the delivery monitor held by the caller.
        void stage() {
            boolean dataTransferred = false;
            try {
                if (this.dictionary != null) receiver.receiveTypeDictionary(this.dictionary);
                if (this.data != null) {
                    final int dataLength = this.completed.dataLength;
                    final int dataChunkCount = this.completed.dataChunkCount;
                    synchronized (barrierLock) {
                        if (deliveryListener != null && !TransactionAssembler.this.deliveryMarkerOpen) {
                            deliveryListener.beforeStoreImport(
                                    this.sequence, this.position, dataLength, dataChunkCount, this.resolutionCrc32c);
                            TransactionAssembler.this.deliveryMarkerOpen = true;
                        }
                        TransactionAssembler.this.barrierHasData = true;
                    }
                    final Binary binary = ChunksWrapper.New(this.data);
                    if (receiver.canReceiveDataOwned()) {
                        /* The owned receiver releases the original buffer on every path,
                         * including a failure thrown from receiveDataOwned or a later
                         * barrier-flush await. */
                        this.completed.detachDataStorage();
                        dataTransferred = true;
                        receiver.receiveDataOwned(binary);
                    } else {
                        dataTransferred = receiver.receiveDataOwned(binary);
                        if (dataTransferred) this.completed.detachDataStorage();
                    }
                }
                synchronized (barrierLock) {
                    pendingDeliveries.add(new PendingDelivery(this.sequence, this.position,
                            this.resolutionKind, this.resolutionCrc32c, this.resolutionDataLength,
                            this.resolutionDataChunkCount, this.completed == null ? 0 : this.completed.dictionaryLength,
                            this.completed == null ? 0 : this.completed.dictionaryChunkCount, this.data != null));
                    barrierBytes = Math.addExact(barrierBytes,
                            (long) this.resolutionDataLength
                                    + (this.completed == null ? 0L : this.completed.dictionaryLength));
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

        /// One resolved transaction staged onto the delivery barrier.
    ///
    /// Carries everything the flush needs to publish the transaction: sequence
    /// and Archive position for the cursor, the terminal kind, and the commit
    /// witness fields for the terminal-marker replay validation.
    private record PendingDelivery(
            long sequence,
            long position,
            AeronReplicationEnvelope.Kind kind,
            int crc32c,
            int dataLength,
            int dataChunkCount,
            int dictionaryLength,
            int dictionaryChunkCount,
            boolean hasData
    ) {
    }

        /// Flushes every staged transaction of the current delivery barrier.
    ///
    /// Called by the polling loop after the poll breaks on a full window,
    /// when a poll returns no fragments, or when the reader stops, so a live
    /// transaction is never held back by more than one idle poll cycle while
    /// a backlog replays at full window size — and so the blocking
    /// materialization wait never runs inside a fragment callback. A latched
    /// failure makes the flush a no-op: the queued buffers belong to the
    /// receiver, whose failure path releases them, and the open uncertainty
    /// marker correctly fails the next start closed.
    ///
    /// @throws RuntimeException when the receiver's materialization failed
    void flushDeliveries() {
        boolean markerHeld;
        boolean hasData;
        synchronized (this.barrierLock) {
            if (this.pendingDeliveries.isEmpty() || this.failure.get() != null) return;
            markerHeld = this.deliveryMarkerOpen;
            hasData = this.barrierHasData;
        }
        /* One materialization wait covers the whole barrier: the receiver's
         * import, materialization, and index maintenance run once for every
         * staged transaction that has data. Abort-only barriers enqueue nothing
         * and the receiver's wait returns immediately. This park deliberately
         * holds no monitor: failure latching and disposal stay lock-free, and a
         * snapshot of the staged entries is only taken below. */
        if (hasData) receiver.awaitApplied();
        /* The staged barrier is inspected, not drained: a transient
         * durability-callback failure must stay retryable, so the drained
         * entries are dropped only after the callback reports the new
         * boundary durable. Until then status and cursorSnapshot() keep
         * exposing the preceding durable boundary. */
        final PendingDelivery resolvedTail;
        long candidateAppliedSequence = this.lastAppliedSequence;
        synchronized (this.barrierLock) {
            resolvedTail = this.pendingDeliveries.peekLast();
            if (resolvedTail == null) return;
            /* An abort advances the cursor without materializing a Store
             * image; commits carry the applied-sequence watermark. */
            for (final PendingDelivery entry : this.pendingDeliveries) {
                if (entry.kind() == AeronReplicationEnvelope.Kind.COMMIT) {
                    candidateAppliedSequence = entry.sequence();
                }
            }
        }
        /* The staged barrier is retained on a callback failure so the flush
         * stays retryable: latching the failure is the caller's decision —
         * the reader loop does exactly that when this exception reaches it,
         * while a standalone retry can still complete the boundary. */
        this.transactionResolved.accept(new CursorSnapshot(resolvedTail.sequence(), resolvedTail.position()));
        /* The callback made the new boundary durable: publish the snapshot
         * and only now retire the staged barrier. */
        this.lastAppliedSequence = candidateAppliedSequence;
        synchronized (this) {
            this.resolvedBoundary = new CursorSnapshot(resolvedTail.sequence(), resolvedTail.position());
            this.lastResolutionCrc32c = resolvedTail.crc32c();
            this.lastResolutionKind = resolvedTail.kind();
            this.lastResolutionDataLength = resolvedTail.dataLength();
            this.lastResolutionDataChunkCount = resolvedTail.dataChunkCount();
            this.lastResolutionDictionaryLength = resolvedTail.dictionaryLength();
            this.lastResolutionDictionaryChunkCount = resolvedTail.dictionaryChunkCount();
        }
        synchronized (this.barrierLock) {
            this.pendingDeliveries.clear();
            this.barrierBytes = 0L;
            this.deliveryMarkerOpen = false;
            this.barrierHasData = false;
        }
        if (markerHeld && deliveryListener != null) {
            this.deliveryListener.afterStoreImport();
        }
    }

        /// Reports transactions staged but not yet published by a barrier flush.
    ///
    /// The cursor callback uses this to persist only the barrier's tail
    /// cursor: intermediate cursors within one barrier are superseded by the
    /// tail the moment it flushes, so persisting them would add fsyncs without
    /// moving the durable boundary further.
    ///
    /// @return staged-but-unflushed transaction count
    int unflushedDeliveryCount() {
        synchronized (this.barrierLock) {
            return this.pendingDeliveries.size();
        }
    }

    long unflushedDeliveryBytes() {
        synchronized (this.barrierLock) {
            return this.barrierBytes;
        }
    }

    private void discardPendingDeliveries() {
        synchronized (this.barrierLock) {
            this.pendingDeliveries.clear();
            this.barrierBytes = 0L;
            this.deliveryMarkerOpen = false;
            this.barrierHasData = false;
        }
    }
}
