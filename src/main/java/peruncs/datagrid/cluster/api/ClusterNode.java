package peruncs.datagrid.cluster.api;

import peruncs.datagrid.cluster.node.ClusterFoundation;
import peruncs.datagrid.cluster.node.StorageNodeControl;
import peruncs.datagrid.cluster.node.replication.ReplicationHealth;
import peruncs.datagrid.cluster.node.replication.ReplicationMetrics;
import peruncs.datagrid.cluster.node.store.ClusterStorageManager;

import java.util.Objects;

/// One owned cluster-node lifecycle and its guarded Store.
///
/// Closing this object closes every Store, Aeron, backup, and worker resource
/// it created. Borrowed controls are never exposed.
///
/// @param <T> root type
public final class ClusterNode<T> implements AutoCloseable {
    private final ClusterFoundation foundation;
    private final ClusterStore<T> store;

    private ClusterNode(final ClusterFoundation foundation, final ClusterStore<T> store) {
        this.foundation = foundation;
        this.store = store;
    }

    /// Opens and starts a node from immutable options.
    ///
    /// @param <T> root type
    /// @param options immutable node options
    /// @return the started node and its owned Store view
    @SuppressWarnings("unchecked")
    public static <T> ClusterNode<T> open(final NodeOptions<T> options) {
        Objects.requireNonNull(options, "options");
        final ClusterFoundation.Builder builder = ClusterFoundation.New()
                .setRootSupplier(options.rootSupplier()::get)
                .setEnableAsyncDistribution(options.asynchronousDistribution());
        final ClusterFoundation foundation = builder.build();
        try {
            final ClusterStorageManager<T> storage =
                    (ClusterStorageManager<T>) foundation.startStorageManager();
            return new ClusterNode<>(foundation, new ClusterStore<>(storage));
        } catch (final RuntimeException | Error failure) {
            try {
                foundation.close();
            } catch (final Throwable closeFailure) {
                if (closeFailure != failure) failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /// Returns the guarded Store owned by this node.
    ///
    /// @return the Store view guarded by this node's lifecycle
    public ClusterStore<T> store() {
        return this.store;
    }

    /// Starts periodic storage checks.
    public void startStorageChecks() {
        this.control().startStorageChecks();
    }

    /// Creates a scheduled-slot backup on a backup-reader node.
    public void createScheduledBackup() {
        this.foundation.backupNodeManager().createStorageBackup(false);
    }

    /// Creates a manually retained backup on a backup-reader node.
    public void createManualBackup() {
        this.foundation.backupNodeManager().createStorageBackup(true);
    }

    /// Returns one immutable status snapshot.
    ///
    /// @return the current role, readiness, replication, and storage metrics
    public NodeStatus status() {
        final StorageNodeControl control = this.control();
        final ReplicationMetrics metrics = control.replicationMetrics();
        return new NodeStatus(control.isWriter(), control.isReady(), control.isHealthy(),
                control.isRunningStorageChecks(), control.readStorageSizeBytes(), metrics.transport(),
                map(metrics.state()), metrics.currentSequence(), metrics.latestSequence(),
                metrics.archiveUsableSpaceBytes(), metrics.writerDurablePosition(),
                metrics.writerDurableSequence(), metrics.appliedSequence());
    }

    private StorageNodeControl control() {
        try {
            return this.foundation.storageNodeManager();
        } catch (final IllegalStateException notStorageRole) {
            return this.foundation.backupNodeManager();
        }
    }

    private static ReplicationState map(final ReplicationHealth.State state) {
        return switch (state) {
            case STARTING -> ReplicationState.STARTING;
            case REPLAYING -> ReplicationState.REPLAYING;
            case LIVE -> ReplicationState.LIVE;
            case DEGRADED_ARCHIVE -> ReplicationState.DEGRADED;
            case RESEED_REQUIRED -> ReplicationState.RESEED_REQUIRED;
            case FAILED -> ReplicationState.FAILED;
        };
    }

    @Override
    public void close() {
        this.foundation.close();
    }
}
