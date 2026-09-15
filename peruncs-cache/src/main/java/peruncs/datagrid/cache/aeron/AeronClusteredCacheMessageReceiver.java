package peruncs.datagrid.cache.aeron;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.eclipse.serializer.typing.Disposable;
import peruncs.datagrid.cache.types.ClusteredCacheMessageAcceptor;
import peruncs.datagrid.cache.types.TimestampsRegionUpdateMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
/// A malformed or undecodable frame stops this receiver and is exposed
/// through [#failure()]. A volatile broadcast cannot prove that a bad
/// frame was harmless or reconstruct a missing invalidation; continuing would
/// make the local cache permanently stale. The same fail-closed rule applies
/// when a valid message cannot be applied or a sender sequence gap is found.
///
/// A receiver is single-use: after [#dispose()] it cannot be started again.
/// Create a new receiver from the provider when a new lifecycle is needed.
///
/// Observability: [#isRunning()] is the programmatic health surface,
/// and received/self-skipped/malformed counters are reported in the dispose
/// debug log; polling failures are retained through [#failure()].
public final class AeronClusteredCacheMessageReceiver implements Disposable {
    private static final System.Logger LOGGER =
            System.getLogger(AeronClusteredCacheMessageReceiver.class.getName());
    private static final int FRAGMENT_LIMIT = 10;
    /* A volatile invalidation stream is trusted only up to a bounded number of
     * sender identities.  Without a cap, an untrusted or misconfigured peer can
     * force an unbounded HashMap allocation simply by changing its node id. */
    private static final int MAX_TRACKED_SENDERS = 1_024;
    private static final String ROLE_NAME = "eclipse-datagrid-cache-invalidation-aeron";

    private final AeronClusteredCacheResources resources;
    private final byte[] senderId;
    private final Runnable releaseSequence;
    private final ClusteredCacheMessageAcceptor messageAcceptor;
    private final int maxPayloadBytes;
    private final FragmentAssembler assembler = new FragmentAssembler(this::onFragment);
    private final BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final LongAdder received = new LongAdder();
    private final LongAdder selfSkipped = new LongAdder();
    private final LongAdder malformed = new LongAdder();
    private final LongAdder gaps = new LongAdder();
    /* Accessed only on the polling thread. */
    private final Map<AeronClusteredCacheMessageCodec.SenderId, Long> lastSequenceBySender = new HashMap<>();
    private final AtomicReference<RuntimeException> agentFailure = new AtomicReference<>();
    private volatile Subscription subscription;
    private volatile Thread agentThread;
    private volatile boolean disposed;
    private volatile boolean running;
    private volatile CountDownLatch stopped;

        /// Creates a receiver for one provider.
    ///
    /// @param resources       shared Aeron resources for this node
    /// @param senderId        sender identity used to ignore this node's frames
    /// @param messageAcceptor target for accepted invalidations
    /// @param maxPayloadBytes maximum accepted payload size
    AeronClusteredCacheMessageReceiver(
            final AeronClusteredCacheResources resources,
            final byte[] senderId,
            final Runnable releaseSequence,
            final ClusteredCacheMessageAcceptor messageAcceptor,
            final int maxPayloadBytes
    ) {
        this.resources = Objects.requireNonNull(resources, "resources");
        this.senderId = Objects.requireNonNull(senderId, "senderId").clone();
        this.releaseSequence = Objects.requireNonNull(releaseSequence, "releaseSequence");
        this.messageAcceptor = Objects.requireNonNull(messageAcceptor, "messageAcceptor");
        if (senderId.length != Long.BYTES * 2) {
            throw new IllegalArgumentException("sender id must be exactly 16 bytes");
        }
        if (maxPayloadBytes < 1 || maxPayloadBytes > Integer.MAX_VALUE -
                AeronClusteredCacheMessageCodec.HEADER_LENGTH - AeronClusteredCacheMessageCodec.CRC_LENGTH) {
            throw new IllegalArgumentException("maxPayloadBytes is outside the supported frame size");
        }
        this.maxPayloadBytes = maxPayloadBytes;
    }

        /// Starts the polling loop and its daemon thread.
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
            this.subscription = this.resources.subscription();
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

    /// Returns whether the receiver is currently consuming invalidations.
    ///
    /// Reports `false` before [#start()] and after disposal, and also
    /// after a terminal transport failure so a node serving stale timestamps
    /// is observable.
    ///
    /// @return `true` while the receiver is running
    public boolean isRunning() {
        return this.running && !this.disposed;
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
    private void run() {
        try {
            while (this.running) {
                final RuntimeException resourceFailure = this.resources.failure();
                if (resourceFailure != null) {
                    this.agentFailure.compareAndSet(null, new IllegalStateException(
                            "Aeron clustered-cache receiver transport has failed", resourceFailure));
                    this.running = false;
                    break;
                }
                final Subscription current = this.subscription;
                final int work = current == null ? 0 : current.poll(this.assembler, FRAGMENT_LIMIT);
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
        }
    }

        /// Package-private test seams for the counters.
    long received() {
        return this.received.sum();
    }

    long selfSkipped() {
        return this.selfSkipped.sum();
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
         * and the payload bounds; the CRC is computed once, not per accessor. */
        final AeronClusteredCacheMessageCodec.ValidatedFrame frame;
        try {
            frame = AeronClusteredCacheMessageCodec.validate(
                    buffer, offset, length, this.maxPayloadBytes, this.senderId);
        } catch (final RuntimeException failure) {
            this.malformed.increment();
            this.failClosed("Malformed Aeron clustered-cache frame of %s bytes".formatted(length), failure);
            return;
        }
        if (frame.self()) {
            this.selfSkipped.increment();
            return;
        }
        if (!this.acceptSequence(frame.sender(), frame.sequence())) {
            return;
        }

        final TimestampsRegionUpdateMessage message;
        try {
            final byte[] payload = AeronClusteredCacheMessageCodec.payloadOf(
                    buffer, offset, frame.payloadLength());
            message = AeronClusteredCachePayloadCodec.decode(payload);
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
    }

        /// Stops delivery while retaining the first terminal cause for health checks.
    private void failClosed(final String message, final RuntimeException failure) {
        this.agentFailure.compareAndSet(null, failure);
        this.running = false;
        LOGGER.log(System.Logger.Level.ERROR, message, failure);
    }

        /// Tracks the per-sender sequence. A gap means this volatile broadcast lost
    /// an invalidation (or the sender identity was reused); the receiver therefore
    /// fails closed instead of continuing with a cache that cannot be reconciled.
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
        if (previous == null && this.lastSequenceBySender.size() >= MAX_TRACKED_SENDERS) {
            final IllegalStateException overflow = new IllegalStateException(
                    "Aeron clustered-cache receiver exceeded the maximum sender identity count: %s".formatted(MAX_TRACKED_SENDERS));
            this.failClosed("Aeron clustered-cache receiver rejected an unbounded sender set", overflow);
            return false;
        }
        if (previous != null && (previous == Long.MAX_VALUE || sequence != previous + 1)) {
            final IllegalStateException gap = new IllegalStateException(
                    "Aeron clustered-cache invalidation sequence gap from sender %s: expected %s after %s, received %s".formatted(sender, (previous == Long.MAX_VALUE ? "overflow" : previous + 1), previous, sequence));
            this.gaps.increment();
            this.failClosed("Aeron clustered-cache receiver failed closed", gap);
            return false;
        }
        this.lastSequenceBySender.put(sender, sequence);
        return true;
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
        LOGGER.log(System.Logger.Level.DEBUG, "Disposed Aeron clustered-cache receiver: received=%s, selfSkipped=%s, malformed=%s, gaps=%s".formatted(this.received.sum(), this.selfSkipped.sum(), this.malformed.sum(), this.gaps.sum()));
    }
}
