package peruncs.datagrid.cache.aeron;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.eclipse.serializer.typing.Disposable;
import peruncs.datagrid.cache.types.ClusteredCacheMessageAcceptor;
import peruncs.datagrid.cache.types.TimestampsRegionUpdateMessage;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/// Consumes clustered-cache invalidations from one Aeron subscription.
///
/// The receiver owns one daemon polling thread with an Agrona
/// [BackoffIdleStrategy]. This keeps the polling thread responsive while
/// spending almost no CPU when the cluster is idle. It polls the subscription,
/// reassembles fragmented frames, ignores frames published by its own sender
/// identity, and decodes the rest with the fixed timestamp-update payload codec.
///
/// Self-suppression compares the 16-byte sender identity in the frame
/// against the identity shared by this node's sender and receiver; a matching
/// frame is skipped without copying or decoding its payload.
///
/// Publication acceptance only proves admission, not delivery, so this
/// receiver never assumes it is healthy just because it is polling. Every
/// validated frame — invalidation or heartbeat, remote or self — refreshes a
/// freshness deadline; total silence past the deadline marks the receiver
/// stale, records a terminal failure, and stops it. A stale receiver requires
/// re-synchronization: it never serves reads as healthy again until its
/// caches are invalidated and a fresh stream is accepted.
///
/// A volatile broadcast cannot detect a peer it has never heard from, so a
/// fresh receiver with only self traffic would otherwise report healthy while
/// partitioned from such a peer. Deployments that need verified reads
/// configure how many distinct remote senders must prove liveness first; the
/// receiver refuses reads until that quorum is observed, without failing
/// closed, so a late peer still completes it.
///
/// A malformed or undecodable frame stops this receiver and is exposed
/// through [#failure()]. A volatile broadcast cannot prove that a bad
/// frame was harmless or reconstruct a missing invalidation; continuing would
/// make the local cache permanently stale. The same fail-closed rule applies
/// when a valid message cannot be applied, or when a sender sequence gap is
/// found. On an empty receiver, a sender joining with a non-zero first sequence
/// causes a full cache invalidation before that sequence becomes its baseline;
/// once a sender is admitted, an unknown sender fails closed and requires an
/// explicit re-synchronization. Every failure path invalidates the local caches
/// before the receiver can ever be declared healthy again.
///
/// Startup always invalidates the local caches first, because a restart may
/// have missed traffic while down. Persisted per-sender cursors additionally
/// validate the first sequence after a restart: cursors are flushed to an
/// atomic file periodically and on disposal, preloaded on startup, and the
/// first live frame must continue the persisted cursor. On mismatch the
/// receiver invalidates everything and fails closed.
///
/// A receiver is single-use: after [#dispose()] it cannot be started again.
/// Create a new receiver from the provider when a new lifecycle is needed.
/// After a terminal failure short of disposal, [#resynchronize()] invalidates
/// the caches, re-baselines the known senders, and restarts polling.
///
/// Observability: [#isRunning()] is the programmatic health surface,
/// and received/self-skipped/heartbeat/malformed/gap counters are reported in
/// the dispose debug log; polling failures are retained through [#failure()].
public final class AeronClusteredCacheMessageReceiver implements Disposable {
    private static final System.Logger LOGGER =
            System.getLogger(AeronClusteredCacheMessageReceiver.class.getName());
    private static final int FRAGMENT_LIMIT = 10;
        /// How often dirty cursors are flushed to the cursor file, in nanoseconds.
    private static final long CURSOR_FLUSH_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1L);
        /// Drops frames buffered while no polling thread ran; the resync invalidation covered them.
    private static final FragmentHandler DISCARD =
            (buffer, offset, length, header) -> {
            };
    /* A volatile invalidation stream tracks only a bounded number of sender
     * identities. Without a cap, an untrusted or misconfigured peer can force
     * an unbounded HashMap allocation simply by changing its node id. Past the
     * cap the receiver fails closed: evicting continuity would allow a sender
     * to advance unseen and return with a false baseline. */
    static final int MAX_TRACKED_SENDERS = 1_024;
    private static final String ROLE_NAME = "eclipse-datagrid-cache-invalidation-aeron";

    private final AeronClusteredCacheResources resources;
    private final byte[] senderId;
    private final Runnable releaseSequence;
    private final ClusteredCacheMessageAcceptor messageAcceptor;
    private final int maxPayloadBytes;
    private final long freshnessTimeoutNanos;
    private final int expectedRemoteSenders;
    private final AeronClusteredCacheCursorStore cursorStore;
    private final byte[] hmacSecret;
    private final byte[] previousHmacSecret;
    private final FragmentAssembler assembler = new FragmentAssembler(this::onFragment);
    private final BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final LongAdder received = new LongAdder();
    private final LongAdder selfSkipped = new LongAdder();
    private final LongAdder heartbeats = new LongAdder();
    private final LongAdder malformed = new LongAdder();
    private final LongAdder gaps = new LongAdder();
    /* Mutated on the polling thread; iterated for cursor snapshots on disposal,
     * so the map itself is concurrent while its logical owner is the poller. */
    private final ConcurrentHashMap<AeronClusteredCacheMessageCodec.SenderId, Long> lastSequenceBySender =
            new ConcurrentHashMap<>();
    /* Last-seen timestamps used to fail closed when an admitted sender goes
     * silent; updated wherever a sender cursor is accepted. */
    private final ConcurrentHashMap<AeronClusteredCacheMessageCodec.SenderId, Long> lastSeenNanos =
            new ConcurrentHashMap<>();
    /* Senders whose next frame is accepted as a re-synchronization baseline.
     * Replaced while no polling thread runs; the poller only removes entries.
     * This set is never used for LRU eviction: only an explicit invalidation
     * establishes a safe re-baseline boundary. */
    private volatile Set<AeronClusteredCacheMessageCodec.SenderId> rebaseSenders =
            ConcurrentHashMap.newKeySet();
    /* Remote senders that proved liveness with a frame accepted in this
     * lifecycle. Persisted cursors and rebase entries alone never count: only
     * observed traffic proves the peer is reachable now. Cleared with the
     * cursors on re-synchronization, so a recovered receiver rebuilds its
     * quorum from post-recovery frames. */
    private final Set<AeronClusteredCacheMessageCodec.SenderId> observedSenders =
            ConcurrentHashMap.newKeySet();
    private final AtomicReference<RuntimeException> agentFailure = new AtomicReference<>();
    private volatile Subscription subscription;
    private volatile Thread agentThread;
    private volatile boolean disposed;
    private volatile boolean running;
    private volatile CountDownLatch stopped;
    private volatile long lastActivityNanos;
    private volatile long lastFlushNanos;
    private volatile boolean cursorsDirty;
    /* True while the polling thread applies the first-sender join invalidation.
     * Reads must be refused until the wipe finishes and the baseline is recorded;
     * the wipe clears caches one at a time and would otherwise serve a torn view. */
    private volatile boolean joinInvalidationInProgress;
    /* Test seam invoked on the re-synchronizing thread after the
     * pre-invalidation drain and before the invalidation. A frame published
     * from this hook must be consumed by the restarted polling thread, never
     * discarded: it postdates the drain, so no later step may treat it as
     * already-covered residue. */
    Runnable resyncAfterDrainHook;
    /* When non-negative, a cache application started at this timestamp and has
     * not finished. The polling thread cannot watch the freshness deadline
     * while blocked inside the acceptor, so health checks consult this stamp
     * instead: an apply running longer than the freshness timeout is suspect. */
    private volatile long applyStartNanos = -1L;
        /// Test seam invoked on the re-synchronizing thread after the new polling
    /// thread starts and before the post-transition health re-check.
    Runnable resyncTransitionHook;

        /// Creates a receiver for one provider.
    ///
    /// @param resources            shared Aeron resources for this node
    /// @param senderId             sender identity used to ignore this node's frames
    /// @param messageAcceptor      target for accepted invalidations
    /// @param maxPayloadBytes      maximum accepted payload size
    /// @param freshnessTimeoutNanos how long total silence is tolerated before staleness; also
    ///                             bounds one cache application before it is treated as suspect
    /// @param cursorStore          persisted per-sender cursors for restart validation
    /// @param hmacSecret           HMAC secret authenticating every frame, or `null` for unsigned frames
    /// @param previousHmacSecret retiring HMAC secret accepted during rotation overlap, or `null`
    AeronClusteredCacheMessageReceiver(
            final AeronClusteredCacheResources resources,
            final byte[] senderId,
            final Runnable releaseSequence,
            final ClusteredCacheMessageAcceptor messageAcceptor,
            final int maxPayloadBytes,
            final long freshnessTimeoutNanos,
            final int expectedRemoteSenders,
            final AeronClusteredCacheCursorStore cursorStore,
            final byte[] hmacSecret,
            final byte[] previousHmacSecret
    ) {
        this.resources = Objects.requireNonNull(resources, "resources");
        this.senderId = Objects.requireNonNull(senderId, "senderId").clone();
        this.releaseSequence = Objects.requireNonNull(releaseSequence, "releaseSequence");
        this.messageAcceptor = Objects.requireNonNull(messageAcceptor, "messageAcceptor");
        this.cursorStore = Objects.requireNonNull(cursorStore, "cursorStore");
        if (hmacSecret != null && hmacSecret.length < AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES) {
            throw new IllegalArgumentException("HMAC secret must contain at least %s bytes".formatted(
                    AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES));
        }
        if (previousHmacSecret != null &&
            previousHmacSecret.length < AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES) {
            throw new IllegalArgumentException("previous HMAC secret must contain at least %s bytes".formatted(
                    AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES));
        }
        this.hmacSecret = hmacSecret == null ? null : hmacSecret.clone();
        this.previousHmacSecret = previousHmacSecret == null ? null : previousHmacSecret.clone();
        if (senderId.length != Long.BYTES * 2) {
            throw new IllegalArgumentException("sender id must be exactly 16 bytes");
        }
        final int maxFramePayloadBytes = Integer.MAX_VALUE -
                AeronClusteredCacheMessageCodec.HEADER_LENGTH - AeronClusteredCacheMessageCodec.CRC_LENGTH -
                (hmacSecret == null ? 0 : AeronClusteredCacheMessageCodec.HMAC_LENGTH);
        if (maxPayloadBytes < 1 || maxPayloadBytes > maxFramePayloadBytes) {
            throw new IllegalArgumentException("maxPayloadBytes is outside the supported frame size");
        }
        if (freshnessTimeoutNanos < 1) {
            throw new IllegalArgumentException(
                    "freshnessTimeoutNanos must be positive: %s".formatted(freshnessTimeoutNanos));
        }
        if (expectedRemoteSenders < 0) {
            throw new IllegalArgumentException(
                    "expectedRemoteSenders must not be negative: %s".formatted(expectedRemoteSenders));
        }
        this.maxPayloadBytes = maxPayloadBytes;
        this.freshnessTimeoutNanos = freshnessTimeoutNanos;
        this.expectedRemoteSenders = expectedRemoteSenders;
    }

        /// Starts the polling loop and its daemon thread.
    ///
    /// Startup invalidates the local caches before the receiver is declared
    /// healthy: a restart may have missed traffic while down, and the first
    /// live frame only proves continuity from that point on. Persisted
    /// per-sender cursors are preloaded first so that first frame is validated
    /// against what the previous lifecycle applied.
    ///
    /// A receiver starts exactly once; restarting or starting after disposal
    /// fails. A failed startup rolls everything back — the subscription is
    /// closed and the shared sequence lease released, because no polling
    /// thread will ever exist to release it later — with cleanup failures
    /// suppressed into the startup failure.
    ///
    /// @throws IllegalStateException when the receiver was already started or disposed
    public synchronized void start() {
        if (this.disposed) {
            throw new IllegalStateException("Aeron clustered-cache receiver is disposed");
        }
        if (this.agentThread != null) {
            throw new IllegalStateException("Aeron clustered-cache receiver is already started");
        }
        try {
            final Map<AeronClusteredCacheMessageCodec.SenderId, Long> persisted = this.cursorStore.load();
            if (persisted.size() > MAX_TRACKED_SENDERS) {
                throw new IllegalStateException(
                        "Aeron clustered-cache cursor file exceeds the maximum sender identity count: %s"
                                .formatted(MAX_TRACKED_SENDERS));
            }
            this.lastSequenceBySender.putAll(persisted);
            this.messageAcceptor.invalidateAll();
            this.subscription = this.resources.subscription();
            final long now = System.nanoTime();
            /* Every persisted sender must prove liveness within one freshness
             * interval. Without a deadline a sender that disappears while another
             * keeps sending would leave the receiver healthy despite missing
             * that sender's updates. */
            for (final var sender : persisted.keySet()) {
                this.lastSeenNanos.put(sender, now);
            }
            this.lastActivityNanos = now;
            this.lastFlushNanos = now;
            this.stopped = new CountDownLatch(1);
            this.running = true;
            final Thread worker = Thread.ofVirtual().name(ROLE_NAME).unstarted(this::run);
            this.agentThread = worker;
            worker.start();
        } catch (final RuntimeException | Error failure) {
            /* A failed startup must not leak the subscription it already created. */
            this.running = false;
            this.stopped = null;
            this.agentThread = null;
            this.subscription = null;
            this.lastSequenceBySender.clear();
            this.lastSeenNanos.clear();
            this.observedSenders.clear();
            this.applyStartNanos = -1L;
            try {
                this.resources.closeSubscription();
            } catch (final RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            /* No receiver thread was published, so there will be no later
             * receiverClosed callback to release a configured shared sequence. */
            try {
                this.releaseSequence.run();
            } catch (final Throwable releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
            throw failure;
        }
        LOGGER.log(System.Logger.Level.DEBUG, "Started Aeron clustered-cache receiver");
    }

        /// Re-synchronizes a failed receiver without disposing it.
    ///
    /// Invalidation runs before the receiver is declared healthy again, and
    /// every sender known before the failure is re-baselined: its next frame
    /// is trusted as the new cursor because the invalidation just covered the
    /// gap. The first sender observed after this full invalidation may start at
    /// any sequence; any later unknown sender fails closed again. The freshness
    /// deadline restarts from this call. Recovery is observable
    /// through [#isRunning()] turning true and [#failure()] clearing.
    ///
    /// When the previous polling thread is still stopping — for example while
    /// blocked applying a message — this call fails and can be retried once
    /// the thread has exited. A receiver that never started or was already
    /// disposed cannot be re-synchronized.
    ///
    /// @throws IllegalStateException when disposed, never started, or still stopping
    public synchronized void resynchronize() {
        if (this.disposed) {
            throw new IllegalStateException("Aeron clustered-cache receiver is disposed");
        }
        if (this.subscription == null) {
            throw new IllegalStateException("Aeron clustered-cache receiver was never started");
        }
        /* Consult the full health check, not the raw flags: a receiver stuck
         * inside an over-long cache application must re-synchronize instead of
         * reporting healthy. A detected stall fails closed here and the
         * re-synchronization proceeds below. */
        if (this.isRunning()) {
            return;
        }
        final Thread worker = this.agentThread;
        if (worker != null && worker.isAlive()) {
            throw new IllegalStateException(
                    "Aeron clustered-cache receiver is still stopping; retry re-synchronization");
        }
        /* Frames may have piled up in the subscription while no thread was
         * polling. Drain them BEFORE the invalidation below: the wipe then
         * covers every discarded frame, while anything published after the
         * drain stays buffered and becomes the re-baselined live stream.
         * Draining after the invalidation would discard peer updates the wipe
         * never covered and let a later heartbeat become a false baseline.
         * The drain is bounded; any residue is still safe, it can only cause
         * a conservative gap failure. */
        final Subscription draining = this.subscription;
        if (draining != null) {
            for (int round = 0; round < 1_000; round++) {
                if (draining.poll(DISCARD, FRAGMENT_LIMIT) == 0) {
                    break;
                }
            }
        }
        final Runnable afterDrain = this.resyncAfterDrainHook;
        if (afterDrain != null) {
            afterDrain.run();
        }
        try {
            this.messageAcceptor.invalidateAll();
        } catch (final RuntimeException failure) {
            this.agentFailure.compareAndSet(null, failure);
            throw failure;
        }
        final Set<AeronClusteredCacheMessageCodec.SenderId> rebase = ConcurrentHashMap.newKeySet();
        rebase.addAll(this.lastSequenceBySender.keySet());
        this.rebaseSenders = rebase;
        this.lastSequenceBySender.clear();
        this.observedSenders.clear();
        /* Rebasing senders keep a freshness deadline from this call: each must
         * re-prove liveness with a post-invalidation frame, or the receiver
         * fails closed instead of silently dropping that sender. */
        final long rebaseStart = System.nanoTime();
        this.lastSeenNanos.clear();
        for (final var sender : rebase) {
            this.lastSeenNanos.put(sender, rebaseStart);
        }
        /* No second drain follows: consumption starts immediately below, so a
         * frame published during the invalidation is consumed as the new
         * baseline instead of being discarded as residue. */
        this.agentFailure.set(null);
        final long now = System.nanoTime();
        this.lastActivityNanos = now;
        this.lastFlushNanos = now;
        this.applyStartNanos = -1L;
        this.cursorsDirty = false;
        this.stopped = new CountDownLatch(1);
        this.running = true;
        final Thread next = Thread.ofVirtual().name(ROLE_NAME).unstarted(this::run);
        this.agentThread = next;
        next.start();
        final Runnable hook = this.resyncTransitionHook;
        if (hook != null) {
            hook.run();
        }
        /* Re-check under the monitor: a failure that landed anywhere in the
         * transitions above — during the invalidation, the drain, or the
         * thread start — must not leave this receiver reporting healthy. */
        final RuntimeException raced = this.agentFailure.get();
        if (raced != null) {
            this.running = false;
            throw new IllegalStateException(
                    "Aeron clustered-cache receiver failed during re-synchronization", raced);
        }
        LOGGER.log(System.Logger.Level.DEBUG, "Re-synchronized Aeron clustered-cache receiver");
    }

    /// Returns whether the receiver is currently consuming invalidations.
    ///
    /// Reports `false` before [#start()] and after disposal, and also after
    /// any terminal failure — a transport failure, a sequence gap, or
    /// staleness past the freshness deadline — so a node that may serve stale
    /// timestamps is observable. A failed receiver stays unhealthy until
    /// [#resynchronize()] completes; it never recovers on its own.
    ///
    /// The polling thread cannot watch the freshness deadline while it is
    /// blocked applying a message, so this check also watches the acceptor:
    /// an application running longer than the freshness timeout is treated as
    /// suspect, fails the receiver closed, and reports unhealthy. This check
    /// therefore has a fail-closed side effect on a stalled receiver.
    ///
    /// @return `true` while the receiver is running
    public boolean isRunning() {
        if (!this.running || this.disposed || this.agentFailure.get() != null) {
            return false;
        }
        /* A join invalidation clears caches one at a time on the polling thread.
         * Refuse reads until the wipe finishes and the baseline is recorded. */
        if (this.joinInvalidationInProgress) {
            return false;
        }
        final long now = System.nanoTime();
        final long applyStart = this.applyStartNanos;
        if (applyStart >= 0 && now - applyStart >= this.freshnessTimeoutNanos) {
            this.failClosed(
                    "Aeron clustered-cache receiver is suspect: a cache application did not complete within %s ms".formatted(this.freshnessTimeoutNanos / 1_000_000L),
                    new IllegalStateException(
                            "Aeron clustered-cache receiver is suspect: a cache application did not complete within %s ms".formatted(this.freshnessTimeoutNanos / 1_000_000L)));
            return false;
        }
        /* Known senders — persisted across restart or rebasing after recovery —
         * must prove liveness even when other traffic keeps the global deadline
         * fresh. Check synchronously so region reads refuse without waiting for
         * the next poll iteration to notice the silence. */
        for (final var seen : this.lastSeenNanos.entrySet()) {
            if (now - seen.getValue() >= this.freshnessTimeoutNanos) {
                this.failClosed(
                        "Aeron clustered-cache sender %s is stale: silent for more than %s ms".formatted(seen.getKey(), this.freshnessTimeoutNanos / 1_000_000L),
                        new IllegalStateException(
                                "Aeron clustered-cache sender %s is stale: silent for more than %s ms".formatted(seen.getKey(), this.freshnessTimeoutNanos / 1_000_000L)));
                return false;
            }
        }
        /* A volatile broadcast cannot name a peer it has never heard from, so
         * a fresh receiver with self traffic alone would otherwise report
         * healthy while partitioned from a peer whose first frame never
         * arrived. With a configured quorum, reads stay refused until enough
         * distinct remote senders prove liveness with post-start frames. This
         * deliberately does not fail closed: the missing peer may still
         * arrive, and polling continues so its first frame completes the
         * quorum instead of being ignored by a terminal state. */
        if (this.expectedRemoteSenders > 0 &&
            this.freshObservedSenders(now) < this.expectedRemoteSenders) {
            return false;
        }
        return true;
    }

    /// Returns the terminal failure that stopped this receiver, or `null`
    /// while it is running or was disposed cleanly.
    ///
    /// @return terminal failure, or `null`
    public RuntimeException failure() {
        return this.agentFailure.get();
    }

        /// Returns whether disposal has started; disposed receivers must never be reused.
    boolean isDisposed() {
        return this.disposed;
    }

        /// Runs one Agrona-idled subscription polling loop until disposal or failure.
    /// Total silence past the freshness deadline fails the receiver closed:
    /// polling alone never proves delivery, only traffic does.
    private void run() {
        try {
            while (this.running) {
                final RuntimeException resourceFailure = this.resources.failure();
                if (resourceFailure != null) {
                    this.failClosed("Aeron clustered-cache receiver transport has failed",
                            new IllegalStateException(
                                    "Aeron clustered-cache receiver transport has failed", resourceFailure));
                    break;
                }
                final Subscription current = this.subscription;
                final int work = current == null ? 0 : current.poll(this.assembler, FRAGMENT_LIMIT);
                final long now = System.nanoTime();
                this.flushCursorsIfDue(now);
                if (this.agentFailure.get() == null &&
                    now - this.lastActivityNanos >= this.freshnessTimeoutNanos) {
                    this.failClosed(
                            "Aeron clustered-cache receiver is stale: no frame or heartbeat within %s ms".formatted(this.freshnessTimeoutNanos / 1_000_000L),
                            new IllegalStateException(
                                    "Aeron clustered-cache receiver is stale: no frame or heartbeat within %s ms".formatted(this.freshnessTimeoutNanos / 1_000_000L)));
                    break;
                }
                /* Per-sender staleness: a sender heard earlier that has since
                 * gone silent while other traffic keeps flowing is otherwise
                 * undetectable — self frames would refresh the global
                 * deadline forever, and no later frame from the lost peer can
                 * reveal a gap that was never followed by traffic. Its
                 * invalidations may have been lost, so continuing would serve
                 * stale query results. */
                final AeronClusteredCacheMessageCodec.SenderId silentSender =
                        this.silentSender(now);
                if (silentSender != null) {
                    this.failClosed(
                            "Aeron clustered-cache sender %s is stale: silent for more than %s ms".formatted(silentSender, this.freshnessTimeoutNanos / 1_000_000L),
                            new IllegalStateException(
                                    "Aeron clustered-cache sender %s is stale: silent for more than %s ms".formatted(silentSender, this.freshnessTimeoutNanos / 1_000_000L)));
                    break;
                }
                this.idleStrategy.idle(work);
            }
        } catch (final Throwable failure) {
            if (!this.disposed) {
                this.agentFailure.compareAndSet(null, failure instanceof RuntimeException runtime
                        ? runtime : new IllegalStateException("Aeron receiver polling loop failed", failure));
                LOGGER.log(System.Logger.Level.ERROR, "Aeron clustered-cache receiver failed", failure);
            }
            if (failure instanceof Error error) {
                throw error;
            }
        } finally {
            this.running = false;
            final CountDownLatch currentStopped = this.stopped;
            if (currentStopped != null) {
                currentStopped.countDown();
            }
            AeronClusteredCacheMessageCodec.clearThreadLocalAuthenticationState();
        }
    }

        /// Package-private test seams for the counters.
    long received() {
        return this.received.sum();
    }

    long selfSkipped() {
        return this.selfSkipped.sum();
    }

    long heartbeats() {
        return this.heartbeats.sum();
    }

    long gaps() {
        return this.gaps.sum();
    }

    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        /* A poll may already have fetched several fragments when one callback
         * fails.  Do not apply any later fragment from that batch after the
         * receiver has entered its terminal fail-closed state. */
        if (!this.running || this.disposed || this.agentFailure.get() != null) {
            return;
        }
        /* One validation covers the header, the sender identity, the sequence,
         * the payload bounds, the CRC, and the HMAC when a secret is
         * configured. */
        final AeronClusteredCacheMessageCodec.ValidatedFrame frame;
        try {
            frame = AeronClusteredCacheMessageCodec.validate(
                    buffer, offset, length, this.maxPayloadBytes, this.senderId, this.hmacSecret,
                    this.previousHmacSecret);
        } catch (final RuntimeException failure) {
            this.malformed.increment();
            this.failClosed("Malformed Aeron clustered-cache frame of %s bytes".formatted(length), failure);
            return;
        }
        /* Any validated frame proves the subscription is live, including a
         * self frame: it traversed the local publication and subscription. */
        this.lastActivityNanos = System.nanoTime();
        if (frame.self()) {
            this.selfSkipped.increment();
            return;
        }
        if (!this.acceptSequence(frame.sender(), frame.sequence())) {
            return;
        }
        if (frame.heartbeat()) {
            this.heartbeats.increment();
            return;
        }

        /* The polling thread cannot watch the freshness deadline while it is
         * blocked below, so stamp the apply start: health checks treat an
         * over-long application as suspect even though this thread is stalled. */
        this.applyStartNanos = System.nanoTime();
        try {
            final TimestampsRegionUpdateMessage message;
            try {
                /* Decoded straight from the receive buffer: the payload is never
                 * copied into a throwaway byte[] first. */
                message = AeronClusteredCachePayloadCodec.decode(
                        buffer, AeronClusteredCacheMessageCodec.payloadOffset(offset), frame.payloadLength());
            } catch (final RuntimeException failure) {
                this.malformed.increment();
                this.failClosed("Undecodable Aeron clustered-cache invalidation", failure);
                return;
            }
            try {
                this.messageAcceptor.accept(message);
                this.received.increment();
            } catch (final RuntimeException failure) {
                final RuntimeException terminal =
                        new IllegalStateException("Failed to apply Aeron clustered-cache invalidation", failure);
                this.failClosed("Aeron clustered-cache receiver failed", terminal);
            }
        } finally {
            /* Progress — applied or failed — proves liveness past this point,
             * and clears the suspect-apply stamp for the next frame. */
            this.applyStartNanos = -1L;
            this.lastActivityNanos = System.nanoTime();
        }
    }

        /// Stops delivery while retaining the first terminal cause for health
    /// checks, then invalidates the local caches so they can never be declared
    /// healthy again without a fresh synchronization. Invalidation is
    /// best-effort here: the terminal failure is already recorded, and an
    /// invalidation error is suppressed into it rather than replacing it.
    /// Package-visible so health checks and tests can fail a stalled receiver.
    void failClosed(final String message, final RuntimeException failure) {
        /* The first failure owns the terminal wipe; concurrent detections (the
         * polling thread and an application thread inside isRunning()) must not
         * wipe twice for one outage. The receiver stays unhealthy until an
         * explicit resynchronization wipes again. */
        if (!this.agentFailure.compareAndSet(null, failure)) {
            this.running = false;
            LOGGER.log(System.Logger.Level.ERROR, message, failure);
            return;
        }
        this.running = false;
        try {
            this.messageAcceptor.invalidateAll();
        } catch (final RuntimeException invalidateFailure) {
            failure.addSuppressed(invalidateFailure);
            LOGGER.log(System.Logger.Level.WARNING, "Aeron clustered-cache invalidation during fail-closed failed",
                    invalidateFailure);
        }
        LOGGER.log(System.Logger.Level.ERROR, message, failure);
    }

        /// Tracks the per-sender sequence. A gap means this volatile broadcast lost
    /// an invalidation (or the sender identity was reused); the receiver therefore
    /// fails closed instead of continuing with a cache that cannot be reconciled.
    /// The first sender admitted to an empty receiver may start at any
    /// sequence; a non-zero first sequence triggers a full invalidation before
    /// it becomes the baseline. Once one sender is tracked, a new sender is a
    /// late join and fails closed until an explicit re-synchronization.
    private boolean acceptSequence(final AeronClusteredCacheMessageCodec.SenderId sender, final long sequence) {
        /* Defense in depth: the codec's validateHeader already rejects a
         * negative or exhausted sequence, so this guard only fires if that
         * invariant is ever weakened. */
        if (sequence < 0) {
            final IllegalStateException invalid = new IllegalStateException(
                    "Aeron clustered-cache invalidation sequence must be non-negative: %s".formatted(sequence));
            this.failClosed("Aeron clustered-cache receiver rejected a negative sequence", invalid);
            return false;
        }
        final Long previous = this.lastSequenceBySender.get(sender);
        if (previous == null) {
            if (this.rebaseSenders.remove(sender)) {
                this.lastSequenceBySender.put(sender, sequence);
                this.lastSeenNanos.put(sender, System.nanoTime());
                this.observedSenders.add(sender);
                this.cursorsDirty = true;
                return true;
            }
            if (this.lastSequenceBySender.size() >= MAX_TRACKED_SENDERS) {
                final IllegalStateException overflow = new IllegalStateException(
                        "Aeron clustered-cache receiver exceeded the maximum sender identity count: %s"
                                .formatted(MAX_TRACKED_SENDERS));
                this.failClosed("Aeron clustered-cache receiver rejected an unbounded sender set", overflow);
                return false;
            }
            if (!this.lastSequenceBySender.isEmpty()) {
                final IllegalStateException lateSender = new IllegalStateException(
                        "Aeron clustered-cache receiver observed an unknown sender after the join boundary: %s"
                                .formatted(sender));
                this.failClosed(
                        "Aeron clustered-cache receiver requires re-synchronization for a late sender",
                        lateSender);
                return false;
            }
            if (sequence != 0) {
                /* A receiver may join after a sender has already emitted
                 * frames. Invalidate before accepting that sender's first
                 * observed sequence; the wipe covers every invalidation that
                 * preceded the baseline, so no gap can leave stale cache data.
                 * Health stays refused until the wipe and baseline complete. */
                this.joinInvalidationInProgress = true;
                try {
                    this.messageAcceptor.invalidateAll();
                } catch (final RuntimeException failure) {
                    try {
                        this.failClosed("Aeron clustered-cache receiver could not invalidate before joining a sender", failure);
                    } finally {
                        this.joinInvalidationInProgress = false;
                    }
                    return false;
                }
                this.lastSequenceBySender.put(sender, sequence);
                this.lastSeenNanos.put(sender, System.nanoTime());
                this.observedSenders.add(sender);
                this.cursorsDirty = true;
                this.joinInvalidationInProgress = false;
                return true;
            }
            this.lastSequenceBySender.put(sender, sequence);
            this.lastSeenNanos.put(sender, System.nanoTime());
            this.observedSenders.add(sender);
            this.cursorsDirty = true;
            return true;
        }
        if (previous == Long.MAX_VALUE || sequence != previous + 1) {
            final IllegalStateException gap = new IllegalStateException(
                    "Aeron clustered-cache invalidation sequence gap from sender %s: expected %s after %s, received %s".formatted(sender, (previous == Long.MAX_VALUE ? "overflow" : previous + 1), previous, sequence));
            this.gaps.increment();
            this.failClosed("Aeron clustered-cache receiver failed closed", gap);
            return false;
        }
        this.lastSequenceBySender.put(sender, sequence);
        this.lastSeenNanos.put(sender, System.nanoTime());
        this.observedSenders.add(sender);
        this.cursorsDirty = true;
        return true;
    }

        /// Counts quorum senders with post-start traffic inside the freshness window.
    ///
    /// Only observed senders count: persisted cursors and rebase entries prove
    /// nothing about current reachability. A sender that proved liveness and
    /// then went silent stops counting once its deadline passes, at which
    /// point the per-sender staleness checks above fail the receiver closed.
    ///
    /// @param now current `System.nanoTime()` reading
    /// @return number of observed senders heard from within the freshness window
    private int freshObservedSenders(final long now) {
        int fresh = 0;
        for (final var observed : this.observedSenders) {
            final Long lastSeen = this.lastSeenNanos.get(observed);
            if (lastSeen != null && now - lastSeen < this.freshnessTimeoutNanos) {
                fresh++;
            }
        }
        return fresh;
    }

        /// Finds a previously-heard sender whose silence exceeds the freshness
    /// deadline, or `null` when every tracked sender is fresh.
    ///
    /// A sender that never sent again after a re-baseline leaves no entry, so
    /// only senders with observed traffic are held to the deadline.
    ///
    /// @param now current `System.nanoTime()` reading
    /// @return the silent sender, or `null` when all tracked senders are fresh
    private AeronClusteredCacheMessageCodec.SenderId silentSender(final long now) {
        for (final var seen : this.lastSeenNanos.entrySet()) {
            if (now - seen.getValue() >= this.freshnessTimeoutNanos) {
                return seen.getKey();
            }
        }
        return null;
    }

        /// Returns a stable copy of every tracked sender cursor.
    ///
    /// Sender continuity is never evicted for space: dropping a cursor would
    /// let that sender advance while unseen and later return with an
    /// unverifiable baseline. The receiver rejects a sender set beyond the
    /// hard bound instead, so this snapshot remains complete.
    private Map<AeronClusteredCacheMessageCodec.SenderId, Long> cursorsSnapshot() {
        return new ConcurrentHashMap<>(this.lastSequenceBySender);
    }

        /// Flushes dirty cursors at most once per flush interval. Failures only
    /// delay durability: the next interval retries, and disposal flushes again.
    private void flushCursorsIfDue(final long now) {
        if (!this.cursorsDirty || now - this.lastFlushNanos < CURSOR_FLUSH_INTERVAL_NANOS) {
            return;
        }
        try {
            this.cursorStore.store(this.cursorsSnapshot());
            this.lastFlushNanos = now;
            this.cursorsDirty = false;
        } catch (final RuntimeException failure) {
            LOGGER.log(System.Logger.Level.WARNING, "Aeron clustered-cache cursor flush failed; retrying", failure);
        }
    }

        /// Persists the cursors one last time without failing disposal.
    private void flushCursorsAtDispose() {
        try {
            this.cursorStore.store(this.cursorsSnapshot());
        } catch (final RuntimeException failure) {
            LOGGER.log(System.Logger.Level.WARNING, "Aeron clustered-cache cursor flush on disposal failed",
                    failure);
        }
    }

        /// Stops the polling loop and releases the subscription. The wait is bounded;
    /// when delivery is blocked, the subscription remains owned by this receiver
    /// and a later call retries the stop instead of closing it beneath the polling
    /// thread.
    @Override
    public void dispose() {
        final Thread worker;
        final CountDownLatch currentStopped;
        synchronized (this) {
            if (this.disposed && this.agentThread == null) {
                return;
            }
            this.disposed = true;
            this.running = false;
            worker = this.agentThread;
            currentStopped = this.stopped;
        }
        /* Persist what was applied so a restart validates its first sequence
         * against this cursor instead of accepting anything. Best-effort: a
         * failure only risks a conservative extra invalidation on restart. */
        this.flushCursorsAtDispose();
        if (worker == null) {
            /* Never started: no subscription exists, but close through the resource
             * owner so a receiver-only provider does not retain an unbound lifecycle
             * forever. If a sender already owns the publication this call is a no-op
             * for the shared client and the sender remains responsible for closing it. */
            RuntimeException closeFailure = null;
            try {
                this.resources.closeSubscription();
            } catch (final RuntimeException failure) {
                closeFailure = failure;
            }
            try {
                this.releaseSequence.run();
            } catch (final RuntimeException releaseFailure) {
                if (closeFailure == null) closeFailure = releaseFailure;
                else if (closeFailure != releaseFailure) closeFailure.addSuppressed(releaseFailure);
            }
            if (this.hmacSecret != null) Arrays.fill(this.hmacSecret, (byte) 0);
            if (this.previousHmacSecret != null) Arrays.fill(this.previousHmacSecret, (byte) 0);
            if (closeFailure != null) {
                throw closeFailure;
            }
            return;
        }
        if (worker == Thread.currentThread()) {
            throw new IllegalStateException("Aeron clustered-cache receiver cannot dispose itself");
        }
        worker.interrupt();
        boolean stoppedInTime;
        try {
            stoppedInTime = currentStopped == null || currentStopped.await(5_000L, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while closing Aeron clustered-cache receiver", failure);
        }
        /* The latch is counted down from the polling loop's finally block. At
         * that point the subscription is no longer being polled; the Java thread
         * may remain alive for the tiny interval in which run() returns, which is
         * not a resource race and must not turn normal disposal into a timeout. */
        if (!stoppedInTime) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "Aeron clustered-cache receiver did not stop; subscription remains open for retry");
            throw new IllegalStateException("Aeron clustered-cache receiver did not stop before disposal timeout");
        }
        this.resources.closeSubscription();
        synchronized (this) {
            if (this.agentThread == worker) {
                this.agentThread = null;
                this.stopped = null;
                this.subscription = null;
            }
        }
        this.releaseSequence.run();
        if (this.hmacSecret != null) Arrays.fill(this.hmacSecret, (byte) 0);
        if (this.previousHmacSecret != null) Arrays.fill(this.previousHmacSecret, (byte) 0);
        LOGGER.log(System.Logger.Level.DEBUG, "Disposed Aeron clustered-cache receiver: received=%s, selfSkipped=%s, heartbeats=%s, malformed=%s, gaps=%s".formatted(this.received.sum(), this.selfSkipped.sum(), this.heartbeats.sum(), this.malformed.sum(), this.gaps.sum()));
    }
}
