package peruncs.datagrid.cluster.node.aeron;

import io.aeron.Aeron;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReaderWatermark;
import peruncs.datagrid.cluster.storage.aeron.reader.CursorSnapshot;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

/// Owns the reader-watermark channel of one transport: the writer-side
/// fan-in that feeds the retention quorum, and the reader-side progress
/// publication.
///
/// The channel is created once per transport in the role-appropriate mode.
/// A reader may publish its cursor while the writer is still recovering its
/// recording, so the writer keeps one deferred value per configured reader
/// until the durable writer boundary exists instead of losing the only
/// acknowledgement. Malformed, stale, or future watermarks are counted and
/// rejected without killing delivery of later valid progress.
final class WatermarkFanIn {
    private static final System.Logger LOGGER = System.getLogger(WatermarkFanIn.class.getName());

    private final Supplier<Aeron> aeron;
    private final AeronSettings settings;
    private final BooleanSupplier retentionSupported;
    /// Live retention controller on the writer, or `null` before creation.
    private final Supplier<AeronArchiveRetention> retention;
    private final BooleanSupplier writerReady;
    /* Scratch accumulator for watermark-frame CRC checks. The writer
     * watermark channel owns one worker thread that runs every decode,
     * so the scratch is confined to that thread for the channel's life. */
    private final CRC32C crcScratch = new CRC32C();
    private final AtomicLong rejectedWatermarks = new AtomicLong();
    /* A reader can publish its last durable cursor while the writer is still
     * recovering its Archive recording. Keep one watermark value per reader
     * until the writer boundary exists instead of dropping that acknowledgement. */
    private final ConcurrentHashMap<UUID, AeronReaderWatermark> deferredWatermarks = new ConcurrentHashMap<>();
    private volatile AeronWatermarkChannel channel;

    WatermarkFanIn(
            final Supplier<Aeron> aeron,
            final AeronSettings settings,
            final BooleanSupplier retentionSupported,
            final Supplier<AeronArchiveRetention> retention,
            final BooleanSupplier writerReady
    ) {
        this.aeron = aeron;
        this.settings = settings;
        this.retentionSupported = retentionSupported;
        this.retention = retention;
        this.writerReady = writerReady;
    }

        /// Reports whether the writer-side watermark channel is receiving.
    ///
    /// @return `true` when the channel exists and has an available image
    boolean available() {
        final AeronWatermarkChannel current = this.channel;
        return current != null && current.available();
    }

        /// Reports whether the channel has been created.
    ///
    /// @return `true` when the channel exists
    boolean hasChannel() {
        return this.channel != null;
    }

        /// Returns the current watermark or retention failure, or `null`.
    ///
    /// @return channel failure, or `null` while healthy
    RuntimeException channelFailure() {
        final AeronArchiveRetention controller = this.retention.get();
        final RuntimeException retentionFailure = controller == null ? null : controller.failure();
        if (retentionFailure != null) return retentionFailure;
        final AeronWatermarkChannel current = this.channel;
        return current == null ? null : current.failure();
    }

        /// Discards deferred reader watermarks during full teardown.
    void discardDeferred() {
        this.deferredWatermarks.clear();
    }

        /// Publishes one reader progress watermark, or drops it when no
    /// channel exists.
    ///
    /// @param snapshot    last resolved reader boundary
    /// @param recordingId recording the boundary belongs to
    void publish(final CursorSnapshot snapshot, final long recordingId) {
        final AeronWatermarkChannel current = this.channel;
        if (current == null) return;
        current.publishEncoded(this.settings.identity().nodeId(), this.settings.clusterId(),
                this.settings.identity().storeGeneration(),
                this.settings.epoch(), recordingId, snapshot.sequence(), snapshot.position());
    }

        /// Creates the channel once, in the role-appropriate mode.
    ///
    /// The transport creates the retention controller before this call on
    /// the writer, so the fan-in can wire its callbacks to it directly.
    void ensure() {
        if (this.channel != null) return;
        if (this.settings.role().isWriter()) {
            if (!this.retentionSupported.getAsBoolean()) return;
            final AeronArchiveRetention controller = this.retention.get();
            this.channel = AeronWatermarkChannel.writer(this.aeron.get(),
                    this.settings.channels().watermark(), this.settings.watermarkStreamId(),
                    (encoded, offset, length) ->
                    {
                        try {
                            final AeronReaderWatermark watermark =
                                    AeronReaderWatermark.decode(this.crcScratch, encoded, offset, length);
                            /* A reader may publish its cursor while the writer is still
                             * recovering its recording. Keep one value per
                             * configured reader instead of losing the only acknowledgement. */
                            if (!this.writerReady.getAsBoolean()) {
                                if (this.settings.retentionReaders().contains(watermark.readerId()) &&
                                    watermark.clusterId().equals(this.settings.clusterId()) &&
                                    watermark.storeGeneration().equals(this.settings.identity().storeGeneration()) &&
                                    watermark.writerEpoch() == this.settings.epoch()) {
                                    this.deferredWatermarks.put(watermark.readerId(), watermark);
                                }
                                return;
                            }
                            this.deferredWatermarks.merge(watermark.readerId(), watermark,
                                    (previous, next) -> next.sequence() >= previous.sequence() ? next : previous);
                            this.drainDeferred();
                            } catch (final RuntimeException rejected) {
                                /* Reject one malformed, stale, or future
                                 * watermark without killing delivery of later valid progress. */
                                this.noteRejection("Aeron reader watermark", rejected);
                            }
                    }, this.settings.watermarkCloseTimeoutNanos(),
                    this.settings.replication().retryPolicy());
        } else {
            /* A reader always publishes progress: the stream is latest-value
             * fire-and-forget, an unreceived value is retained for retry
             * until a subscriber connects (or discarded at close if none
             * ever does, so shutdown stays clean), and the writer records
             * only quorum members — a watermark from a reader outside the
             * writer's retention list is rejected there. Publication must
             * therefore not depend on this reader's local retention list:
             * the documented setup names the quorum on the writer only. */
            this.channel = AeronWatermarkChannel.reader(this.aeron.get(),
                    this.settings.channels().watermark(), this.settings.watermarkStreamId(),
                    this.settings.watermarkCloseTimeoutNanos(),
                    this.settings.replication().retryPolicy());
        }
    }

        /// Replays reader progress received during writer recovery.
    void drainDeferred() {
        if (this.deferredWatermarks.isEmpty() || !this.writerReady.getAsBoolean()) {
            return;
        }
        final AeronArchiveRetention controller = this.retention.get();
        if (controller == null) return;
        for (final var entry : this.deferredWatermarks.entrySet()) {
            try {
                if (controller.offerReaderWatermark(entry.getValue())) {
                    this.deferredWatermarks.remove(entry.getKey(), entry.getValue());
                }
            } catch (final IllegalArgumentException rejected) {
                this.deferredWatermarks.remove(entry.getKey(), entry.getValue());
                this.noteRejection("deferred Aeron reader watermark", rejected);
            } catch (final RuntimeException failure) {
                /* The mailbox retains the newest value after a transient
                 * retention failure or a full worker queue. */
                this.noteRejection("deferred Aeron reader watermark", failure);
            }
        }
    }

        /// Stops the writer-side watermark worker before any writer/retention
    /// resource is closed. The worker invokes retention callbacks and must
    /// never race a transport shutdown while the writer monitor is being
    /// dismantled.
    ///
    /// @return the close failure, or `null` on success
    RuntimeException closeChannel() {
        final AeronWatermarkChannel current = this.channel;
        if (current == null) return null;
        try {
            current.close();
            this.channel = null;
            return null;
        } catch (final RuntimeException failure) {
            if (current.isClosed()) this.channel = null;
            return failure;
        }
    }

    private void noteRejection(final String source, final RuntimeException failure) {
        final long count = this.rejectedWatermarks.incrementAndGet();
        if ((count & (count - 1)) == 0) {
            LOGGER.log(levelFor(failure), "Rejected %s count=%s".formatted(source, count), failure);
        }
    }

        /// Rejections caused by an interrupt are shutdown noise, not data
    /// problems: close interrupts the watermark worker mid-wait (retention
    /// hands off through a blocking {@code Future#get}), and the surviving
    /// interrupted-state signal must not surface as a warning that suggests a
    /// reader progress loss. Real rejections still warn.
    static System.Logger.Level levelFor(final RuntimeException failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException) return DEBUG;
        }
        return WARNING;
    }
}
