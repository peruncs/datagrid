package peruncs.cluster.node;

import peruncs.cluster.node.backup.BackupNodeManager;
import peruncs.cluster.node.backup.FilesystemVolumeBackupBackend;
import peruncs.cluster.node.backup.StorageBackupBackend;
import peruncs.cluster.node.backup.StorageBackupManager;
import peruncs.cluster.node.backup.StorageBackupTaskExecutor;
import peruncs.cluster.node.NodeCollaborators.LazyHolder;
import peruncs.cluster.storage.ReplicationPosition;

import java.util.function.Supplier;

/// The lazily created backup services of one node: backend, backup manager, task executor and the
/// backup-node manager.
///
/// They are created on first use and need the replication client, retention and the running Store,
/// all of which the owning [NodeCollaborators] publishes later; the group therefore reaches them
/// through its owner instead of receiving them at construction.
final class BackupCollaborators {
    final LazyHolder<StorageBackupBackend> backend;
    final LazyHolder<StorageBackupManager> manager;
    final LazyHolder<StorageBackupTaskExecutor> taskExecutor;
    final LazyHolder<BackupNodeManager> nodeManager;
    private final NodeCollaborators owner;

    /// Creates the group.
    ///
    /// @param owner             collaborators the services depend on
    /// @param configuredBackend backend override, or `null` for the filesystem backend
    BackupCollaborators(final NodeCollaborators owner, final StorageBackupBackend configuredBackend) {
        this.owner = owner;
        this.backend = LazyHolder.of(() -> configuredBackend != null ? configuredBackend
                : FilesystemVolumeBackupBackend.create(owner.nodeConfig.backup(), owner.nodeConfig.operations()));
        this.manager = LazyHolder.of(this::createManager);
        this.taskExecutor = LazyHolder.of(this::createTaskExecutor);
        this.nodeManager = LazyHolder.of(this::createNodeManager);
    }

    private StorageBackupManager createManager() {
        final Supplier<ReplicationPosition> position = this.owner.replication.applier.get()::position;
        return StorageBackupManager.create(
                this.owner.clusterStorageManager,
                this.owner.nodeConfig.backup().kept(),
                this.backend.get(),
                position,
                this.owner.replication.applier.get(),
                this.owner.replication.retention.get(),
                this.owner.nodeConfig.operations());
    }

    private StorageBackupTaskExecutor createTaskExecutor() {
        return StorageBackupTaskExecutor.create(this.owner.clusterStorageManager, this.manager.get(),
                this.owner.nodeConfig.backup().closeTimeout().toMillis(), this.owner.nodeConfig.operations());
    }

    private BackupNodeManager createNodeManager() {
        return BackupNodeManager.create(
                this.taskExecutor.get(),
                this.owner.replication.applier.get(),
                this.owner.clusterStorageManager,
                this.owner.getStorageUsageGauge()::readUsedDiskSpaceBytes,
                this.owner.hasReplicationMark());
    }
}
