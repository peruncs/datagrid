package peruncs.datagrid.cluster.node;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import peruncs.datagrid.cluster.node.backup.BackupNodeManager;
import peruncs.datagrid.cluster.node.backup.StorageBackupBackend;
import peruncs.datagrid.cluster.node.backup.StorageBackupManager;
import peruncs.datagrid.cluster.node.backup.StorageBackupTaskExecutor;
import peruncs.datagrid.cluster.node.replication.*;
import peruncs.datagrid.cluster.node.store.StorageDiskSpaceReader;
import peruncs.datagrid.cluster.node.store.StorageNodeHealthCheck;
import peruncs.datagrid.cluster.node.store.StorageTaskExecutor;
import peruncs.datagrid.cluster.storage.types.ObjectGraphUpdateHandler;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataMerger;

import java.util.function.Supplier;

/// Immutable pre-start configuration of one cluster node.
///
/// Collaborators left `null` are created by the node from the properties
/// provider; an embedder supplies one only to substitute an implementation
/// or to pre-wire a test double.
///
/// @param backupBackend backup archive backend
/// @param storageTaskExecutor storage executor
/// @param storageBackupTaskExecutor backup executor
/// @param replicationTransport cluster replication transport
/// @param dataMerger binary data merger
/// @param dataMessageAppliedListener post-consumption listener
/// @param storedReplicationCursorManager persisted cursor manager
/// @param storageBackupManager backup manager
/// @param rootSupplier Store root supplier
/// @param graphUpdateHandler object-graph update handler
/// @param embeddedStorageFoundation embedded Store foundation
/// @param backupNodeManager backup node manager
/// @param dataClient binary data client
/// @param dataDistributor binary data distributor
/// @param healthCheck node health check
/// @param propertiesProvider node properties provider
/// @param storageDiskSpaceReader disk-space reader
/// @param storageNodeManager storage node manager
/// @param positionProvider replication position provider
/// @param replicationRetention replication-log retention policy
record NodeConfiguration(
        StorageBackupBackend backupBackend,
        StorageTaskExecutor storageTaskExecutor,
        StorageBackupTaskExecutor storageBackupTaskExecutor,
        ClusterReplicationTransport replicationTransport,
        StorageBinaryDataMerger dataMerger,
        DataMessageAppliedListener dataMessageAppliedListener,
        StoredReplicationCursorManager storedReplicationCursorManager,
        StorageBackupManager storageBackupManager,
        Supplier<Object> rootSupplier,
        ObjectGraphUpdateHandler graphUpdateHandler,
        EmbeddedStorageFoundation<?> embeddedStorageFoundation,
        BackupNodeManager backupNodeManager,
        StorageBinaryDataClient dataClient,
        StorageBinaryDataDistributor dataDistributor,
        StorageNodeHealthCheck healthCheck,
        NodeLibraryPropertiesProvider propertiesProvider,
        StorageDiskSpaceReader storageDiskSpaceReader,
        StorageNodeManager storageNodeManager,
        ReplicationPositionProvider positionProvider,
        ReplicationLogRetention replicationRetention) {
}
