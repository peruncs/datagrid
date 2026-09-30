package peruncs.cluster.node.backup;

import org.eclipse.store.storage.types.StorageController;
import peruncs.cluster.api.*;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.node.CloseSequencer;
import peruncs.cluster.node.StorageNodeControl;
import peruncs.cluster.storage.binary.ReplicationApplier;

import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

import static java.lang.System.Logger.Level.INFO;
import static org.eclipse.serializer.util.X.notNull;

/// Coordinates backup work while exposing node health.
///
/// A backup stops the reader at a resolved boundary while it creates the
/// backup. Callers must not close the storage while either
/// operation is active. Readiness requires an actively reading,
/// failure-free reader; health additionally tolerates the intentional reader
/// stop a running backup performs, so a normal backup cycle does not flap the
/// health endpoint.
///
/// @since 1.0
public final class BackupNodeManager implements StorageNodeControl, BackupNodeControl, AutoCloseable {
        /// Creates a backup manager for the supplied collaborators.
    ///
    /// @param storageBackupTaskExecutor backup task executor
    /// @param dataClient                replication data client
    /// @param storageController         storage controller
    /// @param storageSizeBytes     supplier for the latest disk-space snapshot
    /// @param replicationEnabled        whether this node has replication
    /// @return backup manager
    public static BackupNodeManager create(
            final StorageBackupTaskExecutor storageBackupTaskExecutor,
            final ReplicationApplier dataClient,
            final StorageController storageController,
            final LongSupplier storageSizeBytes,
            final boolean replicationEnabled
    ) {
        return new BackupNodeManager(
                notNull(storageBackupTaskExecutor),
                notNull(dataClient),
                notNull(storageController),
                notNull(storageSizeBytes),
                replicationEnabled
        );
    }

    private static final System.Logger LOGGER = System.getLogger(BackupNodeManager.class.getName());

    private final StorageBackupTaskExecutor tasks;
    private final ReplicationApplier dataClient;
    private final StorageController storageController;
    private final LongSupplier storageSizeBytes;
    private final boolean replicationEnabled;
    private boolean clientDisposed;

    private BackupNodeManager(
            final StorageBackupTaskExecutor storageBackupTaskExecutor,
            final ReplicationApplier dataClient,
            final StorageController storageController,
            final LongSupplier storageSizeBytes,
            final boolean replicationEnabled
    ) {
        this.tasks = storageBackupTaskExecutor;
        this.dataClient = dataClient;
        this.storageController = storageController;
        this.storageSizeBytes = storageSizeBytes;
        this.replicationEnabled = replicationEnabled;
    }

    /// Borrows this manager as the storage control view; the manager keeps
    /// implementing [StorageNodeControl] directly, so composition costs
    /// nothing and the backup role gains no extra operations.
    @Override
    public StorageNodeControl storage() {
        return this;
    }

    @Override
    public boolean isWriter() {
        return false;
    }

    @Override
    public ReplicationStatus replicationStatus() {
        if (!this.replicationEnabled) return ReplicationStatus.notConfigured();
        final long applied = this.dataClient.currentSequence();
        final ReplicationState state = this.isHealthy() ? ReplicationState.LIVE
                : this.isReady() ? ReplicationState.STARTING : ReplicationState.FAILED;
        return new ReplicationStatus(state, Math.max(-1L, applied), -1L, -1L, -1L, -1L,
                Math.max(-1L, applied));
    }

    @Override
    public CompletableFuture<BackupInfo> createStorageBackup(final BackupSlot slot) {
        return this.tasks.runBackup(slot);
    }

    /// Returns the latest backup outcome without changing replication health.
    public BackupStatus backupStatus() {
        final Throwable failure = this.tasks.backupFailure();
        final Throwable maintenanceFailure = this.tasks.maintenanceFailure();
        return new BackupStatus(failure != null, message(failure), this.tasks.lastSuccessEpochMillis(),
                maintenanceFailure != null, message(maintenanceFailure));
    }

    private static String message(final Throwable failure) {
        if (failure == null) return "";
        return failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
    }

    @Override
    public boolean isHealthy() {
        /* A stopped reader is not healthy: without it the node silently
         * stops replicating. An intentional stop while a backup holds the
         * single-flight lock is the one exemption, so every backup cycle
         * does not flap the health endpoint. Maintenance failures after a
         * durable backup are exposed through the task executor and
         * intentionally do not make the node unhealthy. */
        return this.isOperational() && (this.dataClient.isRunning() || this.tasks.isBackupExecuting());
    }

    @Override
    public boolean isReady() throws NodeException {
        /* Readiness is stricter than health: the reader must actually be
         * running, not merely stopped for an intentional backup. */
        return this.isOperational() && this.dataClient.isRunning();
    }

    @Override
    public boolean isRunningStorageChecks() {
        return this.tasks.isRunningChecks();
    }

    @Override
    public long readStorageSizeBytes() throws NodeException {
        return this.storageSizeBytes.getAsLong();
    }

    @Override
    public void startStorageChecks() {
        this.tasks.runChecks();
    }

    /// Closes the backup executor and the reader client, aggregating
    /// failures. The first call disposes everything it can; later calls
    /// retry the reader disposal while the executor still reports an
    /// active backup, so a failed close never permanently strands the
    /// client while a backup holds it.
    @Override
    public void close() {
        LOGGER.log(INFO, "Closing BackupNodeManager.");
        Throwable failure = null;
        try {
            this.tasks.close();
        } catch (final Throwable closeFailure) {
            failure = closeFailure;
        }
        if (!this.clientDisposed && !this.tasks.isRunningBackup()) {
            try {
                this.dataClient.dispose();
                this.clientDisposed = true;
            } catch (final Throwable closeFailure) {
                failure = CloseSequencer.append(failure, closeFailure);
            }
        } else if (!this.clientDisposed) {
            failure = CloseSequencer.append(failure, new IllegalStateException(
                    "backup reader remains active; data client disposal is deferred to a later close"));
        }
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException runtime) throw runtime;
    }

    private boolean isStorageAvailable() {
        return this.storageController.isRunning() && !this.storageController.isStartingUp();
    }

    /// Reports the conditions shared by health and readiness.
    ///
    /// Health adds tolerance for the intentional reader stop a running
    /// backup performs; readiness additionally requires the reader to be
    /// running. This keeps the two truth tables from drifting apart.
    private boolean isOperational() {
        return this.isStorageAvailable()
                && this.dataClient.failure() == null;
    }
}
