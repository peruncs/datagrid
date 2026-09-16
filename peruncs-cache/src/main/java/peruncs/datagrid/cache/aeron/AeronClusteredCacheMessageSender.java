package peruncs.datagrid.cache.aeron;

import io.aeron.ConcurrentPublication;
import io.aeron.Publication;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.typing.Disposable;
import peruncs.datagrid.cache.types.TimestampsRegionUpdateMessage;

import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/// Publishes clustered-cache invalidations on one Aeron publication.
///
/// This sender is synchronous and fails the local cache operation when the
/// invalidation cannot be accepted. A listener callback acquires the shared
/// sequence lock and offers the frame, waiting until the publication accepts
/// it, retrying transient back pressure and reconnection with an idle strategy
/// for at most the configured publish timeout. That one budget also bounds the
/// wait for the sequence lock when another sender of the same node identity
/// holds it, so a competing sender cannot queue indefinitely. A closed or
/// exhausted publication fails immediately, and a timeout fails with
/// [CacheEntryListenerException]. Nothing is silently dropped.
///
/// An idle sender publishes a payload-less heartbeat on its own daemon thread
/// at the configured interval. Heartbeats consume the same per-sender
/// sequence as invalidations, so a receiver can tell a quiet sender from a
/// lost one and mark itself stale when no frame of any kind arrives. A
/// heartbeat never blocks, never fails the local operation, and never
/// touches the offer-retry counters: when the sequence lock is contested or
/// the publication is not connected, that beat is simply skipped.
///
/// Observability: published, heartbeat, and offer-retry counters are
/// package-private test seams; the public surface is the dispose debug log.
/// The sequence is shared per node identity when `node-id` is configured, and
/// per provider otherwise, so receivers never see false gaps when several
/// providers share one node id.
public final class AeronClusteredCacheMessageSender
        implements CacheEntryCreatedListener<Object, Object>, CacheEntryUpdatedListener<Object, Object>, Disposable {
    private static final System.Logger LOGGER =
            System.getLogger(AeronClusteredCacheMessageSender.class.getName());
        /// A scratch buffer larger than this is released after the frame is offered.
    private static final int MAX_RETAINED_SCRATCH_BYTES = 64 * 1024;
    private static final long DISPOSE_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(5L);
    /* A saturated configured timeout must not overflow the timed tryLock; the
     * lock is only ever held for another sender's offer, so a ten-year cap is
     * indistinguishable from "wait until the deadline". */
    private static final long MAX_LOCK_WAIT_NANOS = TimeUnit.DAYS.toNanos(3_650L);
    private final AeronClusteredCacheResources resources;
    private final byte[] senderId;
    private final AeronClusteredCacheSenderSequence.SequenceLease sequence;
    private final ReentrantLock sequenceLock;
    private final Runnable releaseSequence;
    private final long publishTimeoutNanos;
    private final long heartbeatIntervalNanos;
    private final int maxPayloadBytes;
    private final byte[] hmacSecret;
    /* Every publication is serialized by sequenceLock, so one stateful idle
     * strategy and one reusable native buffer are sufficient. */
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final LongAdder published = new LongAdder();
    private final LongAdder heartbeats = new LongAdder();
    private final LongAdder offerRetries = new LongAdder();
    private volatile Thread heartbeatThread;
    private volatile boolean heartbeatStopped;
    private final Object lifecycleMonitor = new Object();
    private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
    /* Lock order is deliberately one-way: a publish admission takes
     * lifecycleMonitor, then releases it before taking sequenceLock; the first
     * publication lookup takes sequenceLock and then the resources monitor. A
     * disposal holds lifecycleMonitor only while waiting for admitted callbacks,
     * and resources.closePublication() never calls back into this sender. This
     * avoids a lifecycle/sequence/resources cycle while retaining one contiguous
     * sequence-and-offer critical section for shared node identities. The
     * sequence lock is taken with a bounded tryLock so a competing sender cannot
     * queue behind it indefinitely. */
    private UnsafeBuffer scratch;
    /* The publication is looked up once and cached; the resources monitor is
     * only entered on the first publish of this sender. */
    private volatile ConcurrentPublication publication;
    private volatile boolean disposed;
    private int inFlightPublishes;
    private boolean closing;

    private AeronClusteredCacheMessageSender(
            final AeronClusteredCacheResources resources,
            final byte[] senderId,
            final AeronClusteredCacheSenderSequence.SequenceLease sequence,
            final ReentrantLock sequenceLock,
            final Runnable releaseSequence,
            final long publishTimeoutNanos,
            final long heartbeatIntervalNanos,
            final int maxPayloadBytes,
            final byte[] hmacSecret
    ) {
        this.resources = resources;
        this.senderId = Objects.requireNonNull(senderId, "senderId").clone();
        if (this.senderId.length != Long.BYTES * 2) {
            throw new IllegalArgumentException("sender id must be exactly 16 bytes");
        }
        this.sequence = sequence;
        this.sequenceLock = sequenceLock;
        this.releaseSequence = releaseSequence;
        this.publishTimeoutNanos = publishTimeoutNanos;
        if (heartbeatIntervalNanos < 1) {
            throw new IllegalArgumentException(
                    "heartbeatIntervalNanos must be positive: %s".formatted(heartbeatIntervalNanos));
        }
        this.heartbeatIntervalNanos = heartbeatIntervalNanos;
        this.maxPayloadBytes = maxPayloadBytes;
        if (hmacSecret != null && hmacSecret.length < AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES) {
            throw new IllegalArgumentException("HMAC secret must contain at least %s bytes".formatted(
                    AeronClusteredCacheConfiguration.MIN_HMAC_SECRET_BYTES));
        }
        this.hmacSecret = hmacSecret == null ? null : hmacSecret.clone();
        final int maxFramePayloadBytes = Integer.MAX_VALUE -
                AeronClusteredCacheMessageCodec.HEADER_LENGTH - AeronClusteredCacheMessageCodec.CRC_LENGTH -
                (hmacSecret == null ? 0 : AeronClusteredCacheMessageCodec.HMAC_LENGTH);
        if (maxPayloadBytes < 1 || maxPayloadBytes > maxFramePayloadBytes) {
            throw new IllegalArgumentException("maxPayloadBytes is outside the supported frame size");
        }
    }

        /// Creates the sender that turns timestamp cache events into cluster messages.
    /// The heartbeat thread starts with the sender and stops on disposal.
    ///
    /// @param resources             shared Aeron resources for this node
    /// @param senderId              sender identity used so the node ignores its own frames
    /// @param sequence              sequence source shared by every sender of this identity
    /// @param publishTimeoutNanos   maximum time to wait for the publication to accept a frame
    /// @param heartbeatIntervalNanos how often an idle sender publishes a heartbeat
    /// @param maxPayloadBytes       maximum accepted payload size
    /// @param hmacSecret            HMAC secret signing every frame, or `null` for unsigned frames
    /// @return timestamp-cache sender
    static AeronClusteredCacheMessageSender New(
            final AeronClusteredCacheResources resources,
            final byte[] senderId,
            final AeronClusteredCacheSenderSequence.SequenceLease sequence,
            final ReentrantLock sequenceLock,
            final Runnable releaseSequence,
            final long publishTimeoutNanos,
            final long heartbeatIntervalNanos,
            final int maxPayloadBytes,
            final byte[] hmacSecret
    ) {
        final AeronClusteredCacheMessageSender sender = new AeronClusteredCacheMessageSender(resources, senderId,
                sequence, sequenceLock, releaseSequence, publishTimeoutNanos, heartbeatIntervalNanos,
                maxPayloadBytes, hmacSecret);
        sender.startHeartbeats();
        return sender;
    }

        /// Starts the daemon heartbeat thread. Best-effort by design: a missed
    /// beat only delays a receiver's freshness signal, it never fails anything.
    private void startHeartbeats() {
        final Thread worker = Thread.ofVirtual()
                .name("eclipse-datagrid-cache-heartbeat")
                .unstarted(this::heartbeatLoop);
        this.heartbeatThread = worker;
        worker.start();
    }

        /// Publishes one heartbeat per interval until disposal.
    private void heartbeatLoop() {
        while (!this.heartbeatStopped && !this.disposed) {
            try {
                TimeUnit.NANOSECONDS.sleep(this.heartbeatIntervalNanos);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
            if (this.heartbeatStopped || this.disposed) {
                break;
            }
            try {
                this.trySendHeartbeat();
            } catch (final RuntimeException failure) {
                /* Heartbeats are best-effort liveness signals. A failure here
                 * must never kill the heartbeat thread or the sender; the
                 * publish path reports transport failures on its own. */
                LOGGER.log(System.Logger.Level.DEBUG, "Aeron clustered-cache heartbeat skipped", failure);
            }
        }
    }

        /// Offers one heartbeat without waiting. The sequence is consumed only
    /// when the publication accepts the frame, so a skipped beat leaves no gap.
    private void trySendHeartbeat() {
        final AeronClusteredCacheResources resources = this.resources;
        if (resources == null || resources.failure() != null) {
            return;
        }
        if (!this.sequenceLock.tryLock()) {
            return;
        }
        try {
            /* Disposal stops the heartbeat thread first, but a beat already
             * past the sleep may still be queued behind a publish; never let
             * it connect a transport the disposal just released. */
            if (this.heartbeatStopped || this.disposed) {
                return;
            }
            final ConcurrentPublication publication;
            try {
                publication = this.ensurePublication();
            } catch (final RuntimeException unavailable) {
                return;
            }
            final long heartbeatSequence = this.sequence.current();
            if (heartbeatSequence == Long.MAX_VALUE) {
                return;
            }
            final UnsafeBuffer buffer = this.scratchFor(0, this.hmacSecret != null);
            final int length = AeronClusteredCacheMessageCodec.encodeHeartbeat(
                    buffer, this.senderId, heartbeatSequence, this.hmacSecret);
            if (publication.offer(buffer, 0, length) > 0) {
                this.sequence.advance();
                this.heartbeats.increment();
            }
        } finally {
            this.sequenceLock.unlock();
        }
    }

        /// Stops the heartbeat thread without failing disposal when it lingers.
    private void stopHeartbeats() {
        this.heartbeatStopped = true;
        final Thread worker = this.heartbeatThread;
        if (worker != null && worker != Thread.currentThread()) {
            worker.interrupt();
            try {
                worker.join(TimeUnit.SECONDS.toMillis(2L));
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

        /// Returns the publish deadline, saturating at [Long#MAX_VALUE] so an
    /// extreme configured timeout cannot overflow the addition and fail
    /// immediately. This mirrors `ReplicationRetry` deadline semantics; it
    /// lives here because the cache module must not depend on cluster types.
    private static long saturatingDeadline(final long timeoutNanos) {
        final long now = System.nanoTime();
        try {
            return Math.addExact(now, timeoutNanos);
        } catch (final ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

        /// Returns the remaining wait budget without overflowing a saturated deadline.
    private static long remainingNanos(final long deadline) {
        if (deadline == Long.MAX_VALUE) return Long.MAX_VALUE;
        final long remaining = deadline - System.nanoTime();
        return Math.max(remaining, 0L);
    }

        /// Serializes and publishes each event in order.
    ///
    /// @param events cache events to publish
    private void handleEvents(final Iterable<CacheEntryEvent<?, ?>> events)
            throws CacheEntryListenerException {
        for (final CacheEntryEvent<?, ?> event : events) {
            final TimestampsRegionUpdateMessage message;
            try {
                message = TimestampsRegionUpdateMessage.fromEvent(event);
            } catch (final RuntimeException failure) {
                throw new CacheEntryListenerException("Failed to serialize clustered-cache message", failure);
            }
            this.publish(message);
        }
    }

        /// Publishes created entries to the cluster.
    @Override
    public void onCreated(final Iterable<CacheEntryEvent<?, ?>> events)
            throws CacheEntryListenerException {
        this.handleEvents(events);
    }

        /// Publishes updated entries to the cluster.
    @Override
    public void onUpdated(final Iterable<CacheEntryEvent<?, ?>> events)
            throws CacheEntryListenerException {
        this.handleEvents(events);
    }

    private void publish(final TimestampsRegionUpdateMessage message) {
        final int payloadLength;
        try {
            payloadLength = AeronClusteredCachePayloadCodec.encodedLength(Objects.requireNonNull(message, "message"));
        } catch (final RuntimeException failure) {
            throw new CacheEntryListenerException("Failed to serialize clustered-cache message", failure);
        }
        if (payloadLength > this.maxPayloadBytes) {
            throw new CacheEntryListenerException(
                    "Aeron clustered-cache payload exceeds the configured limit of %s".formatted(this.maxPayloadBytes));
        }
        final RuntimeException terminal = this.failure.get();
        if (terminal != null) {
            throw new CacheEntryListenerException("Aeron clustered-cache sender has failed", terminal);
        }
        boolean admitted = false;
        UnsafeBuffer buffer = null;
        try {
            synchronized (this.lifecycleMonitor) {
                if (this.disposed || this.closing) {
                    throw new CacheEntryListenerException("Aeron clustered-cache sender is disposed",
                            new IllegalStateException("disposed"));
                }
                this.inFlightPublishes++;
                admitted = true;
            }
            final RuntimeException resourceFailure = this.resources.failure();
            if (resourceFailure != null) {
                throw new CacheEntryListenerException(
                        "Aeron clustered-cache transport has failed", resourceFailure);
            }

            /* The sequence lock is process-wide for a configured node id, so a
             * competing sender can hold it for the whole offer timeout. Bound
             * the wait by the same publish deadline instead of queueing
             * indefinitely behind it. */
            final long deadline = saturatingDeadline(this.publishTimeoutNanos);
            if (!this.acquireSequenceLock(deadline)) {
                throw new CacheEntryListenerException(
                        "Aeron clustered-cache publish did not acquire the shared sequence within %s ms".formatted(this.publishTimeoutNanos / 1_000_000L));
            }
            try {
                buffer = this.scratchFor(payloadLength, this.hmacSecret != null);
                /* The payload is serialized straight into the scratch frame;
                 * no per-message payload array is allocated. Do not consume a
                 * sequence until the publication has accepted the frame.
                 * A timed-out offer is not visible to receivers. */
                AeronClusteredCachePayloadCodec.encodeInto(
                        message, buffer, AeronClusteredCacheMessageCodec.payloadOffset(0));
                final ConcurrentPublication publication = this.ensurePublication();
                final long sequence = this.sequence.current();
                if (sequence == Long.MAX_VALUE) {
                    throw new CacheEntryListenerException("Aeron clustered-cache sender sequence exhausted");
                }
                final int length = AeronClusteredCacheMessageCodec.encodeFrame(
                        buffer, this.senderId, sequence,
                        AeronClusteredCacheMessageCodec.TYPE_INVALIDATION, payloadLength, this.hmacSecret);
                this.offer(publication, buffer, length, deadline);
                this.sequence.advance();
            } finally {
                if (buffer != null && buffer.capacity() > MAX_RETAINED_SCRATCH_BYTES) {
                    this.releaseScratch();
                }
                this.sequenceLock.unlock();
            }
        } catch (final CacheEntryListenerException failure) {
            throw failure;
        } catch (final RuntimeException failure) {
            throw new CacheEntryListenerException(
                    "Failed to publish Aeron clustered-cache invalidation", failure);
        } finally {
            if (admitted) {
                synchronized (this.lifecycleMonitor) {
                    this.inFlightPublishes--;
                    if (this.inFlightPublishes == 0) {
                        this.lifecycleMonitor.notifyAll();
                    }
                }
            }
        }
    }

        /// Acquires the shared sequence lock within the publish deadline.
    ///
    /// @param deadline publish deadline in nanoseconds
    /// @return `true` when the lock was acquired
    private boolean acquireSequenceLock(final long deadline) {
        try {
            return this.sequenceLock.tryLock(
                    Math.min(remainingNanos(deadline), MAX_LOCK_WAIT_NANOS), TimeUnit.NANOSECONDS);
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new CacheEntryListenerException(
                    "Aeron clustered-cache publish was interrupted while waiting for the shared sequence", interrupted);
        }
    }

        /// Returns the sender-owned off-heap buffer that fits the given payload,
    /// growing (and releasing the previous) buffer when needed.
    private UnsafeBuffer scratchFor(final int payloadLength, final boolean signed) {
        final int required = AeronClusteredCacheMessageCodec.HEADER_LENGTH +
                AeronClusteredCacheMessageCodec.CRC_LENGTH +
                (signed ? AeronClusteredCacheMessageCodec.HMAC_LENGTH : 0) + payloadLength;
        UnsafeBuffer buffer = this.scratch;
        if (buffer == null || buffer.capacity() < required) {
            this.releaseScratch();
            buffer = new UnsafeBuffer(XMemory.allocateDirectNative(required));
            this.scratch = buffer;
        }
        return buffer;
    }

        /// Releases the sender-owned off-heap scratch buffer.
    private void releaseScratch() {
        final UnsafeBuffer buffer = this.scratch;
        if (buffer != null) {
            this.scratch = null;
            XMemory.deallocateDirectByteBuffer(buffer.byteBuffer());
        }
    }

    private ConcurrentPublication ensurePublication() {
        ConcurrentPublication publication = this.publication;
        if (publication == null) {
            /* The resources monitor guards the single addPublication: without it,
             * concurrent listener threads could each create and leak a
             * publication. */
            synchronized (this.resources) {
                publication = this.publication;
                if (publication == null) {
                    try {
                        publication = this.resources.publication();
                    } catch (final RuntimeException failure) {
                        throw new CacheEntryListenerException(
                                "Aeron clustered-cache publication is unavailable", failure);
                    }
                    this.publication = publication;
                }
            }
        }
        return publication;
    }

        /// Offers a frame, waiting for connection and back pressure up to the
    /// publish deadline. The caller is a synchronous JCache listener, so this
    /// method either succeeds or throws; it never returns without publishing.
    ///
    /// @param publication publication to offer on
    /// @param buffer      encoded frame
    /// @param length      encoded frame length
    /// @param deadline    shared publish deadline in nanoseconds
    private void offer(final ConcurrentPublication publication, final UnsafeBuffer buffer, final int length,
                       final long deadline) {
        try {
            while (true) {
                final RuntimeException resourceFailure = this.resources.failure();
                if (resourceFailure != null) {
                    throw new CacheEntryListenerException(
                            "Aeron clustered-cache transport has failed", resourceFailure);
                }
                final RuntimeException senderFailure = this.failure.get();
                if (senderFailure != null) {
                    throw new CacheEntryListenerException("Aeron clustered-cache sender has failed", senderFailure);
                }
                if (Thread.currentThread().isInterrupted()) {
                    throw new CacheEntryListenerException(
                            "Aeron clustered-cache publish was interrupted", new InterruptedException());
                }
                final long result = publication.offer(buffer, 0, length);
                if (result > 0) {
                    this.idleStrategy.reset();
                    this.published.increment();
                    return;
                }
                if (result == Publication.CLOSED) {
                    final CacheEntryListenerException terminal =
                            new CacheEntryListenerException("Aeron clustered-cache publication is closed");
                    this.failure.compareAndSet(null, terminal);
                    throw terminal;
                }
                if (result == Publication.MAX_POSITION_EXCEEDED) {
                    final CacheEntryListenerException terminal = new CacheEntryListenerException(
                            "Aeron clustered-cache publication reached its maximum position");
                    this.failure.compareAndSet(null, terminal);
                    throw terminal;
                }
                if (result != Publication.NOT_CONNECTED && result != Publication.BACK_PRESSURED &&
                    result != Publication.ADMIN_ACTION) {
                    final CacheEntryListenerException terminal = new CacheEntryListenerException(
                            "Aeron clustered-cache publication returned an unknown offer result: %s".formatted(result));
                    this.failure.compareAndSet(null, terminal);
                    throw terminal;
                }
                this.offerRetries.increment();
                if (System.nanoTime() >= deadline) {
                    throw new CacheEntryListenerException(
                            "Aeron clustered-cache publication did not accept the invalidation within %s ms (result=%s)".formatted((this.publishTimeoutNanos / 1_000_000L), result));
                }
                /* idle(0): a positive count means "work was done" and resets the
                 * strategy without parking, which would busy-spin for the whole
                 * publish timeout. */
                this.idleStrategy.idle(0);
            }
        } catch (final IllegalArgumentException failure) {
            throw new CacheEntryListenerException("Aeron clustered-cache frame is invalid", failure);
        }
    }

        /// Disposes the sender, waiting for in-flight publishes to quiesce first.
    ///
    /// The wait is bounded: when publishes do not drain before the disposal
    /// timeout, disposal fails and resets so it can be retried — the sender
    /// is not left half-closed. Only a fully completed disposal releases the
    /// scratch buffer and the shared sequence lease. Disposal is idempotent.
    @Override
    public void dispose() {
        /* Stop the heartbeat first so no new sequence is consumed while the
         * in-flight publishes drain. */
        this.stopHeartbeats();
        boolean quiescent = false;
        boolean completed = false;
        try {
            synchronized (this.lifecycleMonitor) {
                if (this.disposed) return;
                this.closing = true;
                final long deadline = saturatingDeadline(DISPOSE_TIMEOUT_NANOS);
                try {
                    while (this.inFlightPublishes != 0) {
                        final long remaining = remainingNanos(deadline);
                        if (remaining <= 0L) {
                            throw new IllegalStateException(
                                    "Aeron clustered-cache sender did not stop before disposal timeout");
                        }
                        TimeUnit.NANOSECONDS.timedWait(this.lifecycleMonitor, remaining);
                    }
                    quiescent = true;
                    /* Keep the handle until the shared resource owner confirms close. If
                     * closePublication() fails, retry without leaking this sender's buffer. */
                    this.resources.closePublication();
                    this.publication = null;
                    this.disposed = true;
                    completed = true;
                } catch (final InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while closing Aeron clustered-cache sender", failure);
                }
            }
        } finally {
            if (quiescent) this.releaseScratch();
            if (completed) {
                /* The sender is single-use; once the publication is closed no
                 * operation can legitimately need the HMAC key again. Wipe the
                 * owned copy so a long-lived provider does not retain credentials
                 * after its transport has been disposed. */
                if (this.hmacSecret != null) Arrays.fill(this.hmacSecret, (byte) 0);
                AeronClusteredCacheMessageCodec.clearThreadLocalAuthenticationState();
                this.releaseSequence.run();
            }
            if (!completed) {
                synchronized (this.lifecycleMonitor) {
                    this.closing = false;
                }
            }
        }
        LOGGER.log(System.Logger.Level.DEBUG, "Disposed Aeron clustered-cache sender: published=%s, heartbeats=%s, offerRetries=%s".formatted(this.published.sum(), this.heartbeats.sum(), this.offerRetries.sum()));
    }

        /// Package-private test seam for the published counter.
    long published() {
        return this.published.sum();
    }

        /// Package-private test seam for the heartbeat counter.
    long heartbeats() {
        return this.heartbeats.sum();
    }

        /// Package-private test seam for the offer-retry counter.
    long offerRetries() {
        return this.offerRetries.sum();
    }

        /// Package-private test seam for native scratch ownership.
    int scratchBufferCount() {
        return this.scratch == null ? 0 : 1;
    }

        /// Returns whether this single-use sender has completed disposal.
    boolean isDisposed() {
        return this.disposed;
    }
}
