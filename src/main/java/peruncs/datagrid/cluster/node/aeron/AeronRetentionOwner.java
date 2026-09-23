package peruncs.datagrid.cluster.node.aeron;

import peruncs.datagrid.cluster.node.replication.ReplicationLogRetention;
import peruncs.datagrid.cluster.storage.ReplicationCursor;

/// Owns Archive retention for one transport: the retention controller, the
/// reader-watermark fan-in that feeds it, and the degraded/unsupported views.
///
/// Without a configured reader set and an embedded Archive there is nothing
/// safe to delete, so retention reports unsupported and history is preserved
/// rather than risking an unacknowledged purge. The watermark channel is
/// stopped before any writer or retention resource during close, because its
/// worker invokes retention callbacks and must never race the transport
/// shutdown while the writer monitor is being dismantled.
final class AeronRetentionOwner {
    private final AeronTransport facade;
    private ReplicationLogRetention retention;

    AeronRetentionOwner(final AeronTransport facade) {
        this.facade = facade;
    }

    private AeronSettings settings() {
        return this.facade.settings();
    }

    private AeronTransportShared shared() {
        return this.facade.shared();
    }

    /// Returns the Archive retention controller.
    ///
    /// Without a configured reader set and an embedded Archive there is
    /// nothing safe to delete, so retention reports unsupported and history
    /// is preserved rather than risking an unacknowledged purge.
    ///
    /// @return retention controller
    ReplicationLogRetention retention() {
        final AeronTransportShared shared = shared();
        synchronized (shared) {
            shared.ensureOpen();
            if (this.retention != null) return this.retention;
            if (!this.retentionSupported()) {
                /* Retention without a configured reader set and an embedded Archive
                 * cannot prove that every reader has crossed the requested boundary. */
                this.retention = new ReplicationLogRetention() {
                    @Override
                    public boolean isSupported() {
                        return false;
                    }

                    @Override
                    public MaintenanceResult deleteThrough(final ReplicationCursor cursor) {
                        throw new UnsupportedOperationException(
                                "Aeron Archive retention requires an embedded writer and configured retention readers");
                    }

                    @Override
                    public void close() {
                    }
                };
                return this.retention;
            }
            final AeronArchiveRetention created = this.newRetentionController();
            /* Publish the controller before runtime startup because the writer-side
             * watermark setup consults this field.  If startup fails, however, do not
             * leave a closed/broken controller cached for the next retention() call. */
            this.retention = created;
            try {
                this.facade.runtimeOwner().ensure();
                return created;
            } catch (final RuntimeException | Error failure) {
                this.retention = null;
                try {
                    created.close();
                } catch (final RuntimeException | Error closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }
    }

    private AeronArchiveRetention newRetentionController() {
        final AeronWriterTransport writer = this.facade.writerTransport();
        final AeronRuntimeOwner runtime = this.facade.runtimeOwner();
        return new AeronArchiveRetention(settings().archivePolicy().retentionReaders(),
                () ->
                {
                    if (!writer.writerReady())
                        throw new IllegalStateException("Aeron writer must be running before retention maintenance");
                }, new AeronArchiveRetention.RecordingPositions(
                runtime::getStartPosition,
                runtime::getStopPosition,
                runtime::getRecordingPosition),
                writer::writerRecordingId,
                writer::writerBoundary, writer::purgeWithWritesPaused,
                settings().topology().clusterId(), settings().topology().identity().storeGeneration(),
                settings().topology().epoch(), settings().replication()::termLength,
                settings().archivePolicy()::segmentFileLength,
                shared().watermarks()::available,
                settings().topology().directories().checkpointPath().resolveSibling(
                        "%s.retention".formatted(settings().topology().directories().checkpointPath().getFileName())),
                AeronArchiveRetention.DEFAULT_OPERATION_TIMEOUT_MILLIS);
    }

    /// Reports whether this node can run Archive retention.
    ///
    /// @return `true` when a writer with an embedded Archive has retention readers
    boolean retentionSupported() {
        return !settings().archivePolicy().retentionReaders().isEmpty() &&
               settings().topology().role().isWriter() && !settings().archivePolicy().externalArchive();
    }

    /// Returns the live retention controller for the watermark fan-in.
    ///
    /// @return retention controller, or `null` before creation
    AeronArchiveRetention liveRetention() {
        return (AeronArchiveRetention) this.retention;
    }

    /// Reports whether a retention controller exists.
    ///
    /// @return `true` while retention exists
    boolean hasRetention() {
        return this.retention != null;
    }

    /// Creates the watermark channel once the runtime exists.
    ///
    /// The retention controller is created before the channel on a writer, so
    /// the fan-in can wire its callbacks to it directly.
    void ensureWatermarkChannel() {
        if (shared().watermarks().hasChannel()) return;
        if (settings().topology().role().isWriter() && this.retentionSupported() && this.retention == null) {
            this.retention = this.newRetentionController();
        }
        shared().watermarks().ensure();
    }

    /// Replays reader progress received during writer recovery.
    void drainDeferredWatermarks() {
        shared().watermarks().drainDeferred();
    }

    /// Stops the writer-side watermark worker before any writer/retention resource
    /// is closed. The worker invokes retention callbacks and must never race a
    /// transport shutdown while the writer monitor is being dismantled.
    ///
    /// @return the close failure, or `null` on success
    RuntimeException closeWatermarkChannel() {
        return shared().watermarks().closeChannel();
    }

    /// Closes the retention controller as an ordered transport close stage.
    void closeRetentionStage() {
        this.retention.close();
        this.retention = null;
    }

    /// Closes retention during failed startup cleanup, reporting instead of throwing.
    ///
    /// @return the close failure, or `null` on success or when absent
    RuntimeException closeQuietly() {
        if (this.retention == null) return null;
        try {
            this.retention.close();
            this.retention = null;
            return null;
        } catch (final RuntimeException retentionFailure) {
            return retentionFailure;
        }
    }
}
