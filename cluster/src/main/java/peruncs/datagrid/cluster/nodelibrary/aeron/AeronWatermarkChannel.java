package peruncs.datagrid.cluster.nodelibrary.aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronAuthenticatedWatermark;
import peruncs.datagrid.cluster.storage.types.ReplicationRetry;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/// Small, latest-value Aeron control stream carrying durable reader progress to
/// the writer. Missing or superseded messages are safe: retention requires the
/// latest watermark from every active reader and watermarks are monotonic.
final class AeronWatermarkChannel implements AutoCloseable {
    private final Receiver receiver;
    private final long closeTimeoutNanos;
    private final byte[] bufferA = new byte[AeronAuthenticatedWatermark.ENCODED_LENGTH];
    private final byte[] bufferB = new byte[AeronAuthenticatedWatermark.ENCODED_LENGTH];
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
    private boolean closing;
    private AeronWatermarkChannel(
            final Publication publication,
            final Subscription subscription,
            final Receiver receiver,
            final long closeTimeoutNanos
    ) {
        if ((publication == null) == (subscription == null))
            throw new IllegalArgumentException("exactly one Aeron watermark endpoint is required");
        if (subscription != null && receiver == null)
            throw new NullPointerException("receiver");
        if (publication != null && receiver != null)
            throw new IllegalArgumentException("writer watermark channels cannot have a receiver");
        if (closeTimeoutNanos <= 0) throw new IllegalArgumentException("closeTimeoutNanos must be positive");
        this.publication = publication;
        this.subscription = subscription;
        this.receiver = receiver;
        this.closeTimeoutNanos = closeTimeoutNanos;
        this.worker = new Thread(this::run, "eclipse-datagrid-aeron-watermarks");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    static AeronWatermarkChannel writer(
            final Aeron aeron, final String channel, final int streamId, final Receiver receiver,
            final long closeTimeoutNanos) {
        Objects.requireNonNull(aeron, "aeron");
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(receiver, "receiver");
        final Subscription subscription = aeron.addSubscription(channel, streamId);
        try {
            return new AeronWatermarkChannel(null, subscription, receiver, closeTimeoutNanos);
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
            final Aeron aeron, final String channel, final int streamId, final Receiver receiver) {
        return writer(aeron, channel, streamId, receiver, java.util.concurrent.TimeUnit.SECONDS.toNanos(5));
    }

    static AeronWatermarkChannel reader(
            final Aeron aeron, final String channel, final int streamId, final long closeTimeoutNanos) {
        Objects.requireNonNull(aeron, "aeron");
        Objects.requireNonNull(channel, "channel");
        final Publication publication = aeron.addPublication(channel, streamId);
        try {
            return new AeronWatermarkChannel(publication, null, null, closeTimeoutNanos);
        } catch (final RuntimeException | Error failure) {
            try {
                publication.close();
            } catch (final RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    static AeronWatermarkChannel reader(final Aeron aeron, final String channel, final int streamId) {
        return reader(aeron, channel, streamId, java.util.concurrent.TimeUnit.SECONDS.toNanos(5));
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
        if (encoded == null) throw new NullPointerException("encoded");
        if (encoded.length != AeronAuthenticatedWatermark.ENCODED_LENGTH)
            throw new IllegalArgumentException("Aeron watermark encoding must contain exactly %s bytes".formatted(AeronAuthenticatedWatermark.ENCODED_LENGTH));
        final RuntimeException terminal = this.failure.get();
        if (terminal != null) throw new IllegalStateException("Aeron watermark channel failed", terminal);
        if (!this.running.get() || this.closing)
            throw new IllegalStateException("Aeron watermark channel is closed or closing");
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
            final long position, final byte[] secret) {
        final RuntimeException terminal = this.failure.get();
        if (terminal != null) throw new IllegalStateException("Aeron watermark channel failed", terminal);
        if (!this.running.get() || this.closing)
            throw new IllegalStateException("Aeron watermark channel is closed or closing");
        if (this.pending != null && !this.isReusableBuffer(this.pending)) this.pending = null;
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
        AeronAuthenticatedWatermark.signEncodedInto(this.pending, readerId, clusterId, storeGeneration,
                writerEpoch, recordingId, sequence, position, secret);
    }

    boolean available() {
        return this.running.get() && this.failure.get() == null;
    }

    RuntimeException failure() {
        return this.failure.get();
    }

    private void run() {
        final IdleStrategy idle = new BackoffIdleStrategy(1, 10, 1, 1_000_000);
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
                                /* The publication has copied the frame. Do not retain the
                                 * HMAC tag in the reusable heap buffer between offers. */
                                Arrays.fill(value, (byte) 0);
                                this.reclaimSentBuffer(value);
                            }
                            work++;
                        } else if (result == Publication.NOT_CONNECTED || result == Publication.BACK_PRESSURED ||
                                   result == Publication.ADMIN_ACTION) {
                            synchronized (this) {
                                this.sending = null;
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
    /// A bounded wait lets a pending watermark publish; if it cannot flush,
    /// closing fails instead of silently dropping the reader's final
    /// boundary. An interrupted or timed-out close resets so it can be
    /// retried, and in-flight values left behind fail the close rather than
    /// vanishing.
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
                final IdleStrategy idle = new BackoffIdleStrategy(1, 10, 1, 1_000_000);
                while (true) {
                    final boolean pending;
                    final boolean alive;
                    synchronized (this) {
                        pending = this.pending != null;
                        alive = this.worker.isAlive();
                    }
                    if (!pending || this.failure.get() != null || !alive || ReplicationRetry.expired(deadline)) break;
                    idle.idle(0);
                }
                final boolean pending;
                synchronized (this) {
                    pending = this.pending != null;
                }
                if (pending) {
                    closeFailure = append(closeFailure, new IllegalStateException(
                            "Aeron watermark channel could not flush its last durable reader position"));
                }
            }
            synchronized (this) {
                this.running.set(false);
                this.worker.interrupt();
            }
            try {
                final long remainingNanos = ReplicationRetry.remainingNanos(deadline);
                final long remainingMillis = Math.max(1L,
                        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remainingNanos));
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
                if (this.pending != null || this.sending != null) {
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
