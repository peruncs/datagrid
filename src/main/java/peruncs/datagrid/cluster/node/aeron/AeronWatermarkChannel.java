package peruncs.datagrid.cluster.node.aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReaderWatermark;
import peruncs.datagrid.cluster.storage.aeron.config.AeronRetryPolicy;
import peruncs.datagrid.cluster.storage.types.ReplicationRetry;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/// Small, latest-value Aeron control stream carrying durable reader progress to
/// the writer. Missing or superseded messages are safe: retention requires the
/// latest watermark from every active reader and watermarks are monotonic.
///
/// The worker idle pacing and the close-wait pacing come from the configured
/// [AeronRetryPolicy], so a deployment with slow or distant peers can widen
/// parks instead of burning CPU on Agrona's hard-coded defaults.
final class AeronWatermarkChannel implements AutoCloseable {
    private final Receiver receiver;
    private final long closeTimeoutNanos;
    private final AeronRetryPolicy retryPolicy;
    private final Aeron aeron;
    private final byte[] bufferA = new byte[AeronReaderWatermark.ENCODED_LENGTH];
    private final byte[] bufferB = new byte[AeronReaderWatermark.ENCODED_LENGTH];
    /* Agrona's zero-capacity buffer avoids retaining a heap byte[]; wrap the
     * latest caller-owned encoding immediately before each offer. */
    private final UnsafeBuffer sendBuffer = new UnsafeBuffer();
    private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread worker;
    private Publication publication;
    private Subscription subscription;
    private byte[] pending;
    private byte[] sending;
    private byte[] reusable = this.bufferA;
    private boolean closed;
    /* Set after an offer observes no subscriber. Close can discard that value
     * immediately; waiting for the configured flush timeout cannot make an
     * absent subscriber receive it. */
    private boolean subscriberAbsent;
    /* Read unsynchronized by available(); every mutation holds the monitor,
     * so volatile is the only additional visibility needed. */
    private volatile boolean closing;
    private AeronWatermarkChannel(
            final Aeron aeron,
            final Publication publication,
            final Subscription subscription,
            final Receiver receiver,
            final long closeTimeoutNanos,
            final AeronRetryPolicy retryPolicy
    ) {
        if ((publication == null) == (subscription == null))
            throw new IllegalArgumentException("exactly one Aeron watermark endpoint is required");
        if (subscription != null) Objects.requireNonNull(receiver, "receiver");
        if (publication != null && receiver != null)
            throw new IllegalArgumentException("writer watermark channels cannot have a receiver");
        if (closeTimeoutNanos <= 0) throw new IllegalArgumentException("closeTimeoutNanos must be positive");
        this.aeron = Objects.requireNonNull(aeron, "aeron");
        this.publication = publication;
        this.subscription = subscription;
        this.receiver = receiver;
        this.closeTimeoutNanos = closeTimeoutNanos;
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.worker = Thread.ofPlatform().daemon().name("eclipse-datagrid-aeron-watermarks").unstarted(this::run);
        this.worker.start();
    }

    static AeronWatermarkChannel writer(
            final Aeron aeron, final String channel, final int streamId, final Receiver receiver,
            final long closeTimeoutNanos, final AeronRetryPolicy retryPolicy) {
        Objects.requireNonNull(aeron, "aeron");
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(receiver, "receiver");
        final Subscription subscription = aeron.addSubscription(channel, streamId);
        try {
            return new AeronWatermarkChannel(aeron, null, subscription, receiver, closeTimeoutNanos, retryPolicy);
        } catch (final RuntimeException | Error failure) {
            try {
                subscription.close();
            } catch (final RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    static AeronWatermarkChannel writer(
            final Aeron aeron, final String channel, final int streamId, final Receiver receiver,
            final long closeTimeoutNanos) {
        return writer(aeron, channel, streamId, receiver, closeTimeoutNanos, AeronRetryPolicy.Default());
    }

    static AeronWatermarkChannel writer(
            final Aeron aeron, final String channel, final int streamId, final Receiver receiver) {
        return writer(aeron, channel, streamId, receiver, TimeUnit.SECONDS.toNanos(5));
    }

    static AeronWatermarkChannel reader(
            final Aeron aeron, final String channel, final int streamId, final long closeTimeoutNanos,
            final AeronRetryPolicy retryPolicy) {
        Objects.requireNonNull(aeron, "aeron");
        Objects.requireNonNull(channel, "channel");
        final Publication publication = aeron.addPublication(channel, streamId);
        try {
            return new AeronWatermarkChannel(aeron, publication, null, null, closeTimeoutNanos, retryPolicy);
        } catch (final RuntimeException | Error failure) {
            try {
                publication.close();
            } catch (final RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    static AeronWatermarkChannel reader(
            final Aeron aeron, final String channel, final int streamId, final long closeTimeoutNanos) {
        return reader(aeron, channel, streamId, closeTimeoutNanos, AeronRetryPolicy.Default());
    }

    static AeronWatermarkChannel reader(final Aeron aeron, final String channel, final int streamId) {
        return reader(aeron, channel, streamId, TimeUnit.SECONDS.toNanos(5));
    }

    private static RuntimeException append(final RuntimeException current, final RuntimeException additional) {
        if (additional == null) return current;
        if (current == null) return additional;
        if (current != additional) current.addSuppressed(additional);
        return current;
    }

        /// Copies an encoded watermark into channel-owned storage, replacing an older
    /// unsent value with the latest durable progress. The caller may reuse or
    /// mutate its array as soon as this method returns.
    synchronized void publish(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length != AeronReaderWatermark.ENCODED_LENGTH)
            throw new IllegalArgumentException("Aeron watermark encoding must contain exactly %s bytes".formatted(AeronReaderWatermark.ENCODED_LENGTH));
        this.requireOpen();
        this.discardPendingForReplacement();
        if (this.reusable == null && this.sending != null) {
            this.reusable = this.sending == this.bufferA ? this.bufferB : this.bufferA;
        }
        if (this.reusable == null) {
            throw new IllegalStateException("Aeron watermark channel has no encoding buffer");
        }
        this.pending = this.reusable;
        this.reusable = null;
        System.arraycopy(encoded, 0, this.pending, 0, encoded.length);
    }

        /// Encodes the latest watermark into a channel-owned hand-off buffer.
    synchronized void publishEncoded(
            final UUID readerId, final UUID clusterId, final UUID storeGeneration,
            final long writerEpoch, final long recordingId, final long sequence,
            final long position) {
        this.requireOpen();
        if (this.pending == null) {
            if (this.reusable == null) {
                /* Both fixed buffers can only be occupied when the worker is offering
                 * one and the other is queued. In that case the queued value is the
                 * only safe target to replace. */
                if (this.sending != null) this.reusable = this.sending == this.bufferA ? this.bufferB : this.bufferA;
            }
            this.pending = this.reusable;
            this.reusable = null;
        }
        if (this.pending == null) throw new IllegalStateException("Aeron watermark channel has no encoding buffer");
        AeronReaderWatermark.encodeInto(this.pending, readerId, clusterId, storeGeneration,
                writerEpoch, recordingId, sequence, position);
    }

    boolean available() {
        return this.running.get() && !this.closing && this.failure.get() == null;
    }

        /// Rejects publishes once the channel has failed, stopped, or started closing.
    ///
    /// Callers hold the monitor so the `closing` flag is observed atomically
    /// with the pending-buffer hand-off that follows.
    private void requireOpen() {
        final RuntimeException terminal = this.failure.get();
        if (terminal != null) throw new IllegalStateException("Aeron watermark channel failed", terminal);
        if (!this.running.get() || this.closing)
            throw new IllegalStateException("Aeron watermark channel is closed or closing");
    }

    RuntimeException failure() {
        return this.failure.get();
    }

    private void run() {
        final IdleStrategy idle = this.retryPolicy.idleStrategy();
        try {
            while (this.running.get()) {
                int work = 0;
                if (this.subscription != null) {
                    work += this.subscription.poll((buffer, offset, length, header) ->
                            this.receiver.accept(buffer, offset, length), 16);
                }
                if (this.publication != null) {
                    final byte[] value;
                    synchronized (this) {
                        value = this.pending;
                        if (value != null) {
                            this.pending = null;
                            this.sending = value;
                        }
                    }
                    if (value != null) {
                        this.sendBuffer.wrap(value);
                        final long result = this.publication.offer(this.sendBuffer);
                        if (result > 0) {
                            synchronized (this) {
                                this.sending = null;
                                this.subscriberAbsent = false;
                                /* The publication has copied the frame. Do not retain the
                                 * last watermark in the reusable heap buffer between offers. */
                                Arrays.fill(value, (byte) 0);
                                this.reclaimSentBuffer(value);
                            }
                            work++;
                        } else if (result == Publication.NOT_CONNECTED && this.aeron.isClosed()) {
                            /* The client is gone: no subscriber can ever arrive.
                             * Fail the worker so transport supervision observes
                             * the dead client instead of spinning silently. */
                            throw new IllegalStateException(
                                    "Aeron watermark client is closed with an unsent position");
                        } else if (result == Publication.NOT_CONNECTED ||
                                   result == Publication.BACK_PRESSURED ||
                                   result == Publication.ADMIN_ACTION) {
                            /* No receiver yet, a slow receiver, or an admin hold:
                             * keep the latest value and retry on the next poll.
                             * Retaining across NOT_CONNECTED is what lets a
                             * subscriber that arrives later — a writer that
                             * starts after the reader published — still receive
                             * the durable cursor with no further transaction
                             * and no reader restart. Close discards a value
                             * that is still unconnected (see [#close]), so
                             * retaining here never fails a clean shutdown. */
                            synchronized (this) {
                                this.sending = null;
                                this.subscriberAbsent = result == Publication.NOT_CONNECTED;
                                if (this.pending == null) this.pending = value;
                                else this.reclaimSentBuffer(value);
                            }
                        } else if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                            throw new IllegalStateException(
                                    "Aeron watermark publication became terminal: %s".formatted(result));
                        } else {
                            throw new IllegalStateException("unknown Aeron watermark offer result: %s".formatted(result));
                        }
                    }
                }
                idle.idle(work);
            }
        } catch (final RuntimeException failure) {
            this.failure.compareAndSet(null, failure);
        } catch (final Error failure) {
            this.failure.compareAndSet(null,
                    new IllegalStateException("Aeron watermark worker failed", failure));
            throw failure;
        } finally {
            this.running.set(false);
        }
    }

    /// Shuts the watermark worker down, flushing the last durable position first.
    ///
    /// A bounded wait lets a pending watermark publish; if a subscriber exists
    /// and it still cannot flush, closing fails instead of silently dropping
    /// the reader's final boundary. A value with no subscriber even now is
    /// discarded — nobody will ever receive it. An interrupted or timed-out
    /// close resets so it can be retried, and in-flight values left behind
    /// fail the close rather than vanishing.
    @Override
    public void close() {
        final RuntimeException initialFailure;
        synchronized (this) {
            if (this.closed) return;
            if (this.closing) throw new IllegalStateException("Aeron watermark channel is already closing");
            this.closing = true;
            initialFailure = this.failure.get();
        }
        RuntimeException closeFailure = initialFailure;
        final long deadline = ReplicationRetry.deadlineNanos(this.closeTimeoutNanos);
        try {
            if (this.publication != null) {
                final IdleStrategy idle = this.retryPolicy.idleStrategy();
                while (true) {
                    final boolean pending;
                    final boolean alive;
                    synchronized (this) {
                        pending = this.pending != null;
                        alive = this.worker.isAlive();
                    }
                    final boolean subscriberAbsent;
                    synchronized (this) {
                        subscriberAbsent = this.subscriberAbsent;
                    }
                    final boolean disconnected = !this.publication.isConnected();
                    if (!pending || subscriberAbsent || disconnected || this.failure.get() != null || !alive ||
                        ReplicationRetry.expired(deadline)) break;
                    idle.idle(0);
                }
                /* No verdict here: a value still pending after the wait may yet
                 * be discarded by the final arbitration below when nobody
                 * subscribes. Only the post-join state decides. */
            }
            synchronized (this) {
                this.running.set(false);
                this.worker.interrupt();
            }
            try {
                final long remainingNanos = ReplicationRetry.remainingNanos(deadline);
                final long remainingMillis = Math.max(1L,
                        TimeUnit.NANOSECONDS.toMillis(remainingNanos));
                this.worker.join(remainingMillis);
            } catch (final InterruptedException failure) {
                Thread.currentThread().interrupt();
                synchronized (this) {
                    this.closing = false;
                }
                throw new IllegalStateException("interrupted while closing Aeron watermark channel", failure);
            }
            if (this.worker.isAlive()) {
                synchronized (this) {
                    this.closing = false;
                }
                throw new IllegalStateException("Aeron watermark channel did not stop");
            }
            synchronized (this) {
                /* One final offer arbitrates a value still pending after the
                 * wait: delivered and back-pressured keep their existing
                 * meaning, while NOT_CONNECTED discards — nobody is there to
                 * receive it, and failing close for such a boundary would make
                 * every unsubscribed shutdown noisy. (Publication connection
                 * state cannot arbitrate: an established but subscriberless
                 * publication still reports connected.) */
                if (this.pending != null && this.publication != null) {
                    this.sendBuffer.wrap(this.pending);
                    final long result = this.publication.offer(this.sendBuffer);
                    if (result > 0 || result == Publication.NOT_CONNECTED) {
                        if (result <= 0) Arrays.fill(this.pending, (byte) 0);
                        this.pending = null;
                    }
                }
                if (this.pending != null) {
                    closeFailure = append(closeFailure, new IllegalStateException(
                            "Aeron watermark channel could not flush its last durable reader position"));
                }
                if (this.sending != null) {
                    closeFailure = append(closeFailure, new IllegalStateException(
                            "Aeron watermark channel stopped with an in-flight value"));
                }
                this.pending = null;
                this.sending = null;
                this.reusable = null;
                if (this.publication != null) {
                    try {
                        this.publication.close();
                        this.publication = null;
                    } catch (final RuntimeException failure) {
                        closeFailure = append(closeFailure, failure);
                    }
                }
                if (this.subscription != null) {
                    try {
                        this.subscription.close();
                        this.subscription = null;
                    } catch (final RuntimeException failure) {
                        closeFailure = append(closeFailure, failure);
                    }
                }
                if (this.publication == null && this.subscription == null) {
                    Arrays.fill(this.bufferA, (byte) 0);
                    Arrays.fill(this.bufferB, (byte) 0);
                    this.closed = true;
                }
                this.closing = false;
            }
            if (closeFailure != null) throw closeFailure;
        } catch (final RuntimeException failure) {
            synchronized (this) {
                this.closing = false;
            }
            throw failure;
        }
    }

    private boolean isReusableBuffer(final byte[] value) {
        return value == this.bufferA || value == this.bufferB;
    }

    private void discardPendingForReplacement() {
        if (this.isReusableBuffer(this.pending) && this.pending != this.sending) {
            this.reusable = this.pending;
        }
        this.pending = null;
    }

    private void reclaimSentBuffer(final byte[] value) {
        if (this.reusable == null && this.isReusableBuffer(value)) {
            /* The just-completed buffer is the only buffer whose ownership is
             * definitely back with the channel. Reusing the opposite buffer is
             * incorrect when it is already queued as pending: the next encode would
             * overwrite bytes while the worker is still offering them. */
            this.reusable = value;
        }
    }

        /// Returns whether the worker and both Aeron endpoints have been closed.
    synchronized boolean isClosed() {
        return this.closed;
    }

    @FunctionalInterface
    interface Receiver {
        void accept(DirectBuffer buffer, int offset, int length);
    }

}
