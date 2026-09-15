package peruncs.datagrid.cache.clustered.aeron.types;

import io.aeron.ConcurrentPublication;
import io.aeron.Publication;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.serializer.Serializer;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.typing.Disposable;
import peruncs.datagrid.cache.clustered.types.TimestampsRegionUpdateMessage;

import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Publishes clustered-cache invalidations on one Aeron publication.
 *
 * <p>This sender is synchronous and fails the local cache operation when the
 * invalidation cannot be accepted. A listener callback offers the frame and waits
 * until the publication accepts it, retrying transient back pressure and reconnection
 * with an idle strategy for at most the configured publish timeout. A closed
 * or exhausted publication fails immediately, and a timeout fails with
 * {@link CacheEntryListenerException}. Nothing is silently dropped.</p>
 *
 * <p>Observability: published and offer-retry counters are package-private
 * test seams; the public surface is the dispose debug log. The sequence is
 * shared per node identity when {@code node-id} is configured, and per
 * provider otherwise, so receivers never see false gaps when several providers
 * share one node id.</p>
 */
public abstract class AeronClusteredCacheMessageSender
        implements CacheEntryCreatedListener<Object, Object>, CacheEntryUpdatedListener<Object, Object>, Disposable {
    private static final System.Logger LOGGER =
            System.getLogger(AeronClusteredCacheMessageSender.class.getName());
    /** A scratch buffer larger than this is released after the frame is offered. */
    private static final int MAX_RETAINED_SCRATCH_BYTES = 64 * 1024;
    private static final long DISPOSE_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(5L);
    private final AeronClusteredCacheResources resources;
    private final byte[] senderId;
    private final AeronClusteredCacheSenderSequence.SequenceLease sequence;
    private final Object sequenceLock;
    private final Runnable releaseSequence;
    private final Serializer<byte[]> serializer;
    private final long publishTimeoutNanos;
    private final int maxPayloadBytes;
    /* Every publication is serialized by sequenceLock, so one stateful idle
     * strategy and one reusable native buffer are sufficient. */
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final LongAdder published = new LongAdder();
    private final LongAdder offerRetries = new LongAdder();
    private final Object lifecycleMonitor = new Object();
    private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
    /* Lock order is deliberately one-way: a publish admission takes
     * lifecycleMonitor, then releases it before taking sequenceLock; the first
     * publication lookup takes sequenceLock and then the resources monitor. A
     * disposal holds lifecycleMonitor only while waiting for admitted callbacks,
     * and resources.closePublication() never calls back into this sender. This
     * avoids a lifecycle/sequence/resources cycle while retaining one contiguous
     * sequence-and-offer critical section for shared node identities. */
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
            final Object sequenceLock,
            final Runnable releaseSequence,
            final Serializer<byte[]> serializer,
            final long publishTimeoutNanos,
            final int maxPayloadBytes
    ) {
        this.resources = resources;
        this.senderId = java.util.Objects.requireNonNull(senderId, "senderId").clone();
        if (this.senderId.length != Long.BYTES * 2) {
            throw new IllegalArgumentException("sender id must be exactly 16 bytes");
        }
        this.sequence = sequence;
        this.sequenceLock = sequenceLock;
        this.releaseSequence = releaseSequence;
        this.serializer = serializer;
        this.publishTimeoutNanos = publishTimeoutNanos;
        this.maxPayloadBytes = maxPayloadBytes;
    }

    /**
     * Creates the sender that turns timestamp cache events into cluster messages.
     *
     * @param resources           shared Aeron resources for this node
     * @param senderId            sender identity used so the node ignores its own frames
     * @param sequence            sequence source shared by every sender of this identity
     * @param serializer          serializer shared with the receiver
     * @param publishTimeoutNanos maximum time to wait for the publication to accept a frame
     * @param maxPayloadBytes     maximum accepted serialized payload size
     * @return timestamp-cache sender
     */
    static AeronClusteredCacheMessageSender UpdateTimestamps(
            final AeronClusteredCacheResources resources,
            final byte[] senderId,
            final AeronClusteredCacheSenderSequence.SequenceLease sequence,
            final Object sequenceLock,
            final Runnable releaseSequence,
            final Serializer<byte[]> serializer,
            final long publishTimeoutNanos,
            final int maxPayloadBytes
    ) {
        return new UpdateTimestamps(resources, senderId, sequence, sequenceLock, releaseSequence, serializer,
                publishTimeoutNanos, maxPayloadBytes);
    }

    /**
     * Returns the publish deadline, saturating at {@link Long#MAX_VALUE} so an
     * extreme configured timeout cannot overflow the addition and fail
     * immediately.
     */
    private static long saturatingDeadline(final long timeoutNanos) {
        final long now = System.nanoTime();
        try {
            return Math.addExact(now, timeoutNanos);
        } catch (final ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    /** Returns the remaining wait budget without overflowing a saturated deadline. */
    private static long remainingNanos(final long deadline) {
        if (deadline == Long.MAX_VALUE) return Long.MAX_VALUE;
        final long remaining = deadline - System.nanoTime();
        return Math.max(remaining, 0L);
    }

    /** Converts one cache event into a cluster update message. */
    protected abstract TimestampsRegionUpdateMessage createMessage(CacheEntryEvent<?, ?> event);

    /** Serializes and publishes each event in order. */
    protected void handleEvents(final Iterable<CacheEntryEvent<?, ?>> events)
            throws CacheEntryListenerException {
        for (final CacheEntryEvent<?, ?> event : events) {
            final byte[] payload;
            try {
                payload = this.serializer.serialize(this.createMessage(event));
            } catch (final Exception failure) {
                throw new CacheEntryListenerException("Failed to serialize clustered-cache message", failure);
            }
            this.publish(payload);
        }
    }

    private void publish(final byte[] payload) {
        if (payload == null || payload.length > this.maxPayloadBytes) {
            throw new CacheEntryListenerException(
                    "Aeron clustered-cache payload exceeds the configured limit of " + this.maxPayloadBytes);
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

            synchronized (this.sequenceLock) {
                try {
                    buffer = this.scratchFor(payload.length);
                    /* Do not consume a sequence until the publication has accepted the
                     * frame. A timed-out offer is not visible to receivers. */
                    final ConcurrentPublication publication = this.ensurePublication();
                    final long sequence = this.sequence.current();
                    if (sequence == Long.MAX_VALUE) {
                        throw new CacheEntryListenerException("Aeron clustered-cache sender sequence exhausted");
                    }
                    final int length = AeronClusteredCacheMessageCodec.encode(
                            buffer, this.senderId, sequence, payload);
                    this.offer(publication, buffer, length);
                    this.sequence.advance();
                } finally {
                    if (buffer != null && buffer.capacity() > MAX_RETAINED_SCRATCH_BYTES) {
                        this.releaseScratch();
                    }
                }
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

    /**
     * Returns the sender-owned off-heap buffer that fits the given payload,
     * growing (and releasing the previous) buffer when needed.
     */
    private UnsafeBuffer scratchFor(final int payloadLength) {
        final int required = AeronClusteredCacheMessageCodec.HEADER_LENGTH + payloadLength;
        UnsafeBuffer buffer = this.scratch;
        if (buffer == null || buffer.capacity() < required) {
            this.releaseScratch();
            buffer = new UnsafeBuffer(XMemory.allocateDirectNative(required));
            this.scratch = buffer;
        }
        return buffer;
    }

    /** Releases the sender-owned off-heap scratch buffer. */
    private void releaseScratch() {
        final UnsafeBuffer buffer = this.scratch;
        if (buffer != null) {
            this.scratch = null;
            XMemory.deallocateDirectByteBuffer(buffer.byteBuffer());
        }
    }

    /** Releases the scratch buffer after publication quiescence. */
    private void releaseAllScratch() {
        this.releaseScratch();
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

    /**
     * Offers a frame, waiting for connection and back pressure up to the
     * publish timeout. The caller is a synchronous JCache listener, so this
     * method either succeeds or throws; it never returns without publishing.
     */
    private void offer(final ConcurrentPublication publication, final UnsafeBuffer buffer, final int length) {
        final long deadline = saturatingDeadline(this.publishTimeoutNanos);
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
                            "Aeron clustered-cache publication returned an unknown offer result: " + result);
                    this.failure.compareAndSet(null, terminal);
                    throw terminal;
                }
                this.offerRetries.increment();
                if (System.nanoTime() >= deadline) {
                    throw new CacheEntryListenerException(
                            "Aeron clustered-cache publication did not accept the invalidation within " +
                            (this.publishTimeoutNanos / 1_000_000L) + " ms (result=" + result + ")");
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

    @Override
    public void dispose() {
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
            if (quiescent) this.releaseAllScratch();
            if (completed) this.releaseSequence.run();
            if (!completed) {
                synchronized (this.lifecycleMonitor) {
                    this.closing = false;
                }
            }
        }
        LOGGER.log(System.Logger.Level.DEBUG, "Disposed Aeron clustered-cache sender: published=" +
                                              this.published.sum() + ", offerRetries=" + this.offerRetries.sum());
    }

    /** Package-private test seam for the published counter. */
    long published() {
        return this.published.sum();
    }

    /** Package-private test seam for the offer-retry counter. */
    long offerRetries() {
        return this.offerRetries.sum();
    }

    /** Package-private test seam for native scratch ownership. */
    int scratchBufferCount() {
        return this.scratch == null ? 0 : 1;
    }

    /** Returns whether this single-use sender has completed disposal. */
    boolean isDisposed() {
        return this.disposed;
    }

    /** Converts timestamp cache events into cluster update messages. */
    private static final class UpdateTimestamps extends AeronClusteredCacheMessageSender
            implements CacheEntryCreatedListener<Object, Object>, CacheEntryUpdatedListener<Object, Object> {
        private UpdateTimestamps(
                final AeronClusteredCacheResources resources,
                final byte[] senderId,
                final AeronClusteredCacheSenderSequence.SequenceLease sequence,
                final Object sequenceLock,
                final Runnable releaseSequence,
                final Serializer<byte[]> serializer,
                final long publishTimeoutNanos,
                final int maxPayloadBytes
        ) {
            super(resources, senderId, sequence, sequenceLock, releaseSequence, serializer,
                    publishTimeoutNanos, maxPayloadBytes);
        }

        @Override
        public void onCreated(final Iterable<CacheEntryEvent<?, ?>> events)
                throws CacheEntryListenerException {
            this.handleEvents(events);
        }

        @Override
        public void onUpdated(final Iterable<CacheEntryEvent<?, ?>> events)
                throws CacheEntryListenerException {
            this.handleEvents(events);
        }

        @Override
        protected TimestampsRegionUpdateMessage createMessage(final CacheEntryEvent<?, ?> event) {
            return TimestampsRegionUpdateMessage.fromEvent(event);
        }
    }
}
