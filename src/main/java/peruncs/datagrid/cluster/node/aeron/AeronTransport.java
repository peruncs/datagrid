package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.datagrid.cluster.node.CloseSequencer;
import peruncs.datagrid.cluster.node.NodeRole;
import peruncs.datagrid.cluster.node.NodeSettingsSource;
import peruncs.datagrid.cluster.node.backup.BackupMetadata;
import peruncs.datagrid.cluster.node.replication.*;
import peruncs.datagrid.cluster.storage.ReplicationCursor;
import peruncs.datagrid.cluster.storage.binary.ReplicationApplier;
import peruncs.datagrid.cluster.storage.binary.ReplicationPublisher;
import peruncs.datagrid.cluster.storage.binary.StorageBinaryDataReceiver;
import peruncs.datagrid.cluster.storage.io.AtomicFileWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/// Creates the Aeron cluster replication transport.
///
/// The created transport owns its embedded MediaDriver/Archive runtime, its
/// writer, its reader, and its retention controller through dedicated owners
/// and releases them from [#close()] in dependency order; individual readers
/// and writers never close the runtime. The facade itself holds no
/// mutable transport state beyond composing those owners and one small shared
/// lifecycle state.
public final class AeronTransport implements ClusterReplicationTransport {
    private final AeronSettings settings;
    /// Shared lifecycle state synchronized by every owner; also the monitor.
    private final AeronTransportShared shared;
    private final AeronRuntimeOwner runtimeOwner;
    private final AeronWriterTransport writerTransport;
    private final AeronReaderTransport readerTransport;
    private final AeronRetentionOwner retentionOwner;
    /* Guards health-view replacement only: health probes must never queue
     * behind reader replacement/disposal on the reader slot lock or behind
     * writer startup on the shared monitor. */
    private final Object healthLock = new Object();
    private AeronHealth health;
    /// Ordered retryable close stages, built once; each readiness check reads live fields.
    private final CloseSequencer closeSequencer;

    /// Creates a transport with settings read from the node properties.
    ///
    /// @param properties node configuration
    public AeronTransport(final NodeSettingsSource properties) {
        this.settings = AeronSettings.fromEnvironment(properties);
        final Path leaseDirectoryPath = leaseDirectory(properties);
        validateLeaseDirectory(this.settings, leaseDirectoryPath, properties);
        this.shared = new AeronTransportShared(new AeronArchiveCapacity(this.settings));
        this.runtimeOwner = new AeronRuntimeOwner(this);
        this.writerTransport = new AeronWriterTransport(this, leaseDirectoryPath,
                properties.writerLeaseStalenessMillis());
        this.readerTransport = new AeronReaderTransport(this);
        this.retentionOwner = new AeronRetentionOwner(this);
        this.shared.installWatermarks(new WatermarkCollector(this.runtimeOwner::aeron, this.settings,
                this.retentionOwner::retentionSupported, this.retentionOwner::liveRetention,
                this.writerTransport::writerReady));
        this.closeSequencer = new CloseSequencer(this.closeStages());
        /* Probe metadata durability before any synchronized runtime startup path. */
        this.verifyMetadataStorage();
    }

    AeronSettings settings() {
        return this.settings;
    }

    AeronTransportShared shared() {
        return this.shared;
    }

    AeronRuntimeOwner runtimeOwner() {
        return this.runtimeOwner;
    }

    AeronWriterTransport writerTransport() {
        return this.writerTransport;
    }

    AeronReaderTransport readerTransport() {
        return this.readerTransport;
    }

    AeronRetentionOwner retentionOwner() {
        return this.retentionOwner;
    }

    /* Package-private forked-test seam. Keeping the owner structure hidden
     * avoids adding a production lifecycle interface solely for fault injection. */
    static void stopDriverForTest(final ClusterReplicationTransport transport) {
        if (!(transport instanceof AeronTransport aeronTransport))
            throw new IllegalArgumentException("transport was not created by the Aeron provider");
        aeronTransport.runtimeOwner().stopDriver();
    }

    /// Rejects a lease directory the Aeron runtime recreates or overwrites.
    ///
    /// The MediaDriver recreates its directory on every start, so a lease
    /// stored inside the driver, archive, or checkpoint tree is deleted on the
    /// next startup and the writer would appear fenced. Failing at wiring time
    /// is clearer than a lost lease on the first write.
    ///
    /// @param settings       resolved Aeron settings
    /// @param leaseDirectory resolved lease directory, or `null` when unset
    private static void validateLeaseDirectory(final AeronSettings settings, final Path leaseDirectory,
                                               final NodeSettingsSource properties) {
        final boolean productionWriter = settings.productionMode() && settings.topology().role() == NodeRole.WRITER;
        if (leaseDirectory == null) {
            if (productionWriter) {
                throw new IllegalArgumentException("production writer requires a pre-provisioned shared lease directory");
            }
            return;
        }
        if (productionWriter) {
            if (!Boolean.parseBoolean(properties.replicationProperty("ECLIPSE_DATAGRID_AERON_SHARED_LEASE_FILESYSTEM"))) {
                throw new IllegalArgumentException(
                        "ECLIPSE_DATAGRID_AERON_SHARED_LEASE_FILESYSTEM=true is required for a production writer");
            }
            validateSharedLeaseFilesystem(leaseDirectory);
        }
        if (overlaps(leaseDirectory, settings.topology().directories().aeronDirectory()) ||
            overlaps(leaseDirectory, settings.topology().directories().archiveDirectory()) ||
            overlaps(leaseDirectory, settings.topology().directories().checkpointPath())) {
            throw new IllegalArgumentException(
                    "writer lease directory must not overlap the Aeron driver, archive, or checkpoint paths: lease=%s, driver=%s, archive=%s, checkpoint=%s".formatted(
                            leaseDirectory, settings.topology().directories().aeronDirectory(), settings.topology().directories().archiveDirectory(), settings.topology().directories().checkpointPath()));
        }
    }

    /* FileStore.type is only a fail-closed local-filesystem filter, not proof
     * that two hosts mounted the same export or honor distributed locks. */
    static void validateSharedLeaseFilesystem(final Path directory) {
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("production writer lease directory must be pre-provisioned: " + directory);
        }
        try {
            AtomicFileWriter.ensureNoSymbolicLinks(directory);
            final String type = Files.getFileStore(directory).type();
            if (!"nfs4".equalsIgnoreCase(type)) {
                throw new IllegalArgumentException("production writer lease requires a supported shared filesystem (nfs4); found "
                        + type + " at " + directory);
            }
            AtomicFileWriter.verify(directory.resolve(".datagrid-lease-probe"));
        } catch (final IOException failure) {
            throw new IllegalStateException("production writer lease filesystem validation failed: " + directory, failure);
        }
    }

    private static boolean overlaps(final Path left, final Path right) {
        return left.startsWith(right) || right.startsWith(left);
    }

    /// Resolves the shared directory holding the writer fencing lease.
    ///
    /// The lease must live where every writer of one cluster/generation can
    /// see it, so it shares the backup volume: the one filesystem this
    /// topology already treats as shared across machines. A writer without a
    /// configured shared directory cannot fence, so the directory is required
    /// for the writer role and absent for readers.
    ///
    /// @param properties node configuration
    /// @return lease directory, or `null` when no shared backup volume is configured
    private static Path leaseDirectory(final NodeSettingsSource properties) {
        final String configured = properties.replicationProperty(
                NodeSettingsSource.Env.EnvKeys.BACKUP_PATH);
        return configured == null || configured.isBlank()
                ? null
                : Paths.get(configured).toAbsolutePath().normalize();
    }

    private static Throwable appendFailure(final Throwable current,
                                           final Throwable additional) {
        if (additional == null) return current;
        /* Checked failures are normalized so callers always hold a
         * runtime-typed aggregate; the shared sequencer owns the
         * aggregation order and the Error-priority rule. */
        final Throwable normalized = additional instanceof RuntimeException runtime
                ? runtime : new IllegalStateException("Aeron transport resource close failed", additional);
        return CloseSequencer.append(current, normalized);
    }

    private void verifyMetadataStorage() {
        final Path parent = this.settings.topology().directories().checkpointPath().toAbsolutePath().getParent();
        if (parent == null) throw new IllegalArgumentException("Aeron checkpoint path must have a parent directory");
        if (Files.isSymbolicLink(this.settings.topology().directories().checkpointPath())) {
            throw new IllegalArgumentException(
                    "Aeron checkpoint path must not be a symbolic link: %s".formatted(this.settings.topology().directories().checkpointPath()));
        }
        /* Apply the same production permission policy before the capability probe
         * creates any temporary metadata files.  Otherwise a non-POSIX filesystem
         * would pass this early probe and fail only later during runtime startup. */
        AeronRuntime.ensurePrivateDirectory(parent, this.settings.productionMode());
        try {
            AtomicFileWriter.verify(this.settings.topology().directories().checkpointPath());
        } catch (final IOException failure) {
            throw new IllegalStateException(
                    "Aeron replication metadata storage does not support atomic replacement", failure);
        }
    }

    @Override
    public String id() {
        return "aeron";
    }

    @Override
    public BackupMetadata.Identity configuredBackupIdentity() {
        return new BackupMetadata.Identity(
                this.settings.topology().clusterId(), this.settings.topology().identity().storeGeneration(),
                this.settings.topology().epoch(), this.settings.topology().recordingId());
    }

    /// Returns the replication publisher for the transport's single replication stream.
    ///
    /// The returned handle carries dictionaries and lifecycle control
    /// only: data publication is deliberately unavailable through it and
    /// flows exclusively through the persistence-target factory, where
    /// local Store acceptance and checkpoint fencing are one serialized
    /// operation. A second stream name is rejected — one transport owns
    /// exactly one stream.
    ///
    /// @param streamName logical stream name, claimed on first use
    /// @return replication publisher
    @Override
    public ReplicationPublisher distributor(final String streamName) {
        return this.writerTransport.distributor(streamName);
    }

    @Override
    public UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
            final String streamName, final ReplicationPublisher distributor,
            final Supplier<StorageConnection> writerStorage) {
        return this.writerTransport.persistenceTargetFactory(streamName, distributor, writerStorage);
    }

    @Override
    public ReplicationApplier client(
            final StorageBinaryDataReceiver receiver,
            final String streamName,
            final CommitAppliedListener cursorListener,
            final ReplicationCursor startingCursor,
            final boolean commitPosition
    ) {
        return this.readerTransport.client(receiver, streamName, cursorListener, startingCursor, commitPosition);
    }

    @Override
    public ReplicationPositionProvider positionProvider(final String streamName) {
        return this.writerTransport.positionProvider(streamName);
    }

    @Override
    public ReplicationLogRetention retention() {
        return this.retentionOwner.retention();
    }

    @Override
    public ReplicationHealth health(
            final StorageControllerAdapter storage,
            final ReplicationApplier client
    ) {
        this.shared.ensureOpen();
        /* Health-view replacement uses its own lock, so a health probe never
         * queues behind reader replacement/disposal on the reader lock or
         * behind writer startup on the shared transport monitor. */
        synchronized (this.healthLock) {
            if (this.health == null || !this.health.matches(storage, client)) {
                if (this.health != null) this.health.close();
                this.health = new AeronHealth(
                        storage,
                        client,
                        this.shared::closed,
                        () -> this.runtimeOwner.driverFailure() != null,
                        () -> this.shared.capacity().available(this.settings.replication().maxTransactionBytes()),
                        this.writerTransport::writerReady,
                        () -> this.settings.topology().role().isWriter(),
                        this.writerTransport::writerCheckpointState,
                        this.shared.capacity()::usableSpaceBytes,
                        () -> this.writerTransport.writerBoundary().position(),
                        () -> this.writerTransport.writerBoundary().sequence(),
                        this.readerTransport::appliedSequence,
                        () -> this.shared.watermarks().channelFailure() != null);
            }
            return this.health;
        }
    }

    /// Releases resources after a failed runtime start, best effort.
    ///
    /// Writer-side watermark delivery calls back into this transport, so that
    /// worker is stopped before the writer/retention resources it could
    /// re-enter. The runtime is closed last, only once the channel and
    /// retention are fully gone.
    ///
    /// @return aggregated close failure, or `null` on success
    Throwable closeRuntimeQuietly() {
        Throwable failure = null;
        /* Writer-side watermark delivery calls back into this transport. Stop that
         * worker first so it cannot enter writer/retention startup while the close
         * is in progress. Reader-side channels remain open until the reader exits so
         * its final cursor publication is not turned into a spurious failure. */
        if (!this.readerTransport.occupied() && this.settings.topology().role().isWriter() &&
            this.shared.watermarks().hasChannel()) {
            failure = this.retentionOwner.closeWatermarkChannel();
        }
        failure = appendFailure(failure, this.retentionOwner.closeWatermarkChannel());
        failure = appendFailure(failure, this.retentionOwner.closeQuietly());
        if (!this.shared.watermarks().hasChannel() && !this.retentionOwner.hasRetention() &&
            this.runtimeOwner.isStarted()) {
            failure = appendFailure(failure, this.runtimeOwner.closeQuietly());
        }
        if (!this.shared.watermarks().hasChannel() && !this.retentionOwner.hasRetention() &&
            !this.runtimeOwner.isStarted()) {
            this.shared.watermarks().discardDeferred();
        }
        return failure;
    }

    /// Shuts the transport down in dependency order: reader, write
    /// coordinator, writer, watermark channel, retention, the shared Aeron
    /// runtime, and finally the writer fencing lease.
    ///
    /// The lease is released only after the publication path is fully
    /// gone, so no successor can steal the token while this writer can
    /// still offer. Each stage runs only after the previous one is fully
    /// gone, and a stage that fails leaves the transport retryable instead
    /// of torn down beneath live threads — closing the runtime early would
    /// turn a retryable abort into a use-after-close. All failures are
    /// aggregated and thrown together. Never call this from inside a reader
    /// delivery callback.
    @Override
    public void close() {
        this.shared.ensureNotDeliveryCallback();
        synchronized (this.shared) {
            if (this.shared.closed()) return;
            if (this.shared.closing()) {
                throw new IllegalStateException("Aeron transport close is already in progress");
            }
            /* A lease held without any other resource means writer startup
             * failed after acquisition; it still shuts down on the full
             * path below, never leaking its file and heartbeat thread. */
            if (!this.readerTransport.occupied() && !this.writerTransport.hasWriter() &&
                !this.writerTransport.hasCoordinator() && !this.runtimeOwner.isStarted() &&
                !this.retentionOwner.hasRetention() && !this.shared.watermarks().hasChannel() &&
                !this.writerTransport.hasLease()) {
                this.shared.watermarks().discardDeferred();
                this.shared.closed(true);
                return;
            }
            this.shared.closing(true);
        }

        final Throwable failure = this.closeSequencer.close();
        if (failure != null) {
            this.shared.closing(false);
            if (failure instanceof Error error) throw error;
            if (failure instanceof RuntimeException r)
                throw r;
            throw new IllegalStateException("failed to close Aeron transport", failure);
        }
        synchronized (this.shared) {
            this.writerTransport.clearDistributor();
            this.shared.clearStreamClaim();
            this.shared.watermarks().discardDeferred();
            this.shared.closed(true);
            this.shared.closing(false);
        }
    }

    /// Declares the ordered release stages exactly as the dependency graph
    /// requires them: reader, coordinator, writer, watermark channel,
    /// retention, shared runtime, and finally the writer fencing lease.
    ///
    /// A stage is skipped when its resource is already gone, so a retry
    /// after a failed stage resumes without repeating completed work. The
    /// lease is released only after the publication path is fully stopped,
    /// so no successor can steal the token while this writer can still
    /// offer.
    private List<CloseSequencer.Stage> closeStages() {
        return List.of(
                CloseSequencer.stage("reader",
                        this.readerTransport::occupied,
                        this.readerTransport::disposeReader),
                /* The coordinator owns every pending transaction and must resolve or
                 * preserve its fence before the Archive wrapper stops the recording.
                 * Closing the raw writer first can publish an ABORT beneath a Store
                 * write that still owns the coordinator. */
                CloseSequencer.stage("coordinator",
                        () -> !this.readerTransport.occupied() && this.writerTransport.hasCoordinator(),
                        this.writerTransport::closeCoordinatorStage),
                CloseSequencer.stage("writer",
                        () -> !this.readerTransport.occupied() && !this.writerTransport.hasCoordinator() &&
                              this.writerTransport.hasWriter(),
                        this.writerTransport::closeWriterStage),
                /* A reader can publish its final durable cursor from the polling
                 * thread. Stop that thread before flushing and closing the watermark
                 * publication. Likewise, keep the writer-side receiver alive until
                 * publication shutdown has completed. */
                CloseSequencer.stage("watermark",
                        () -> !this.readerTransport.occupied() && !this.writerTransport.hasWriter() &&
                              !this.writerTransport.hasCoordinator() && this.shared.watermarks().hasChannel(),
                        () -> {
                            final RuntimeException watermarkFailure = this.retentionOwner.closeWatermarkChannel();
                            if (watermarkFailure != null) throw watermarkFailure;
                        }),
                CloseSequencer.stage("retention",
                        () -> !this.readerTransport.occupied() && !this.writerTransport.hasWriter() &&
                              !this.writerTransport.hasCoordinator() &&
                              !this.shared.watermarks().hasChannel() && this.retentionOwner.hasRetention(),
                        this.retentionOwner::closeRetentionStage),
                /* Do not close the shared runtime while a writer or coordinator still
                 * owns the publication. A failed abort/stop is retryable; closing
                 * Aeron here would turn that retry into a use-after-close and leak
                 * the unresolved sequence. */
                CloseSequencer.stage("runtime",
                        () -> !this.readerTransport.occupied() && !this.writerTransport.hasWriter() &&
                              !this.writerTransport.hasCoordinator() &&
                              !this.retentionOwner.hasRetention() && !this.shared.watermarks().hasChannel() &&
                              this.runtimeOwner.isStarted(),
                        this.runtimeOwner::close),
                CloseSequencer.stage("writer-lease",
                        () -> !this.readerTransport.occupied() && !this.writerTransport.hasWriter() &&
                              !this.writerTransport.hasCoordinator() &&
                              !this.runtimeOwner.isStarted(),
                        this.writerTransport::closeLeaseStage)
        );
    }
}
