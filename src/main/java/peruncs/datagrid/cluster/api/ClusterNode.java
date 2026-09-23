package peruncs.datagrid.cluster.api;

import peruncs.datagrid.cluster.node.NodeAssembly;
import peruncs.datagrid.cluster.node.NodeRole;
import peruncs.datagrid.cluster.node.StorageNodeControl;
import peruncs.datagrid.cluster.node.replication.ReplicationMetrics;
import peruncs.datagrid.cluster.node.store.ClusterStorageManager;

import java.util.Objects;
import java.util.OptionalLong;

/// One owned cluster-node lifecycle and its guarded Store.
///
/// Closing this object closes every Store, Aeron, backup, and worker resource
/// it created. Borrowed controls are never exposed.
///
/// @param <T> root type
public final class ClusterNode<T> implements AutoCloseable {
    private final NodeAssembly assembly;
    private final ClusterStore<T> store;
    private final NodeRole role;

    private ClusterNode(final NodeAssembly assembly, final ClusterStore<T> store, final NodeRole role) {
        this.assembly = assembly;
        this.store = store;
        this.role = role;
    }

    /// Opens and starts a node from immutable options.
    ///
    /// Role and identity come from the environment:
    /// `ECLIPSE_DATAGRID_REPLICATION_ROLE` (`writer`, `reader`,
    /// `backup-reader`, or unset for an unreplicated node),
    /// `ECLIPSE_DATAGRID_AERON_CLUSTER_ID`, `ECLIPSE_DATAGRID_AERON_NODE_ID`,
    /// and `ECLIPSE_DATAGRID_AERON_STORE_GENERATION` are required on every
    /// replicated node; production nodes additionally require an explicit
    /// `ECLIPSE_DATAGRID_AERON_WIRE_NONCE` and the trusted-network and
    /// shared-lease acknowledgements.
    ///
    /// Startup fails closed: when durable local state cannot be reconciled
    /// with the Archive, or any required setting is missing or inconsistent,
    /// this method throws before any Store, Aeron, or background-thread
    /// resource escapes, and everything already created is closed.
    ///
    /// @param <T> root type
    /// @param options immutable node options
    /// @return the started node and its owned Store view
    @SuppressWarnings("unchecked")
    public static <T> ClusterNode<T> open(final NodeOptions<T> options) {
        Objects.requireNonNull(options, "options");
        final NodeAssembly.Builder builder = NodeAssembly.create()
                .setRootSupplier(options.rootSupplier()::get);
        final NodeAssembly assembly = builder.build();
        try {
            final ClusterStorageManager<T> storage =
                    (ClusterStorageManager<T>) assembly.startStorageManager();
            return new ClusterNode<>(assembly, new ClusterStore<>(storage), assembly.nodeRole());
        } catch (final RuntimeException | Error failure) {
            try {
                assembly.close();
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
    ///
    /// This operation is valid for writer and reader nodes. A backup-reader
    /// does not run the ordinary storage-check scheduler.
    public void startStorageChecks() {
        this.control().startStorageChecks();
    }

    /// Stops a backup-reader at a durable boundary and creates a scheduled-slot backup.
    ///
    /// Only a backup-reader may call this method; other roles are rejected.
    /// One backup runs at a time: a concurrent request is rejected as busy
    /// (map it to a conflict response). Replication pauses at a resolved
    /// cursor, resumes after the snapshot finishes, and remains failed
    /// closed if resume fails, so a failed backup never silently restarts
    /// reading from an ambiguous position.
    ///
    /// @throws peruncs.datagrid.cluster.errors.NodeException if the backup or
    /// the resume fails
    public void createScheduledBackup() {
        this.assembly.backupNodeManager().createStorageBackup(false);
    }

    /// Stops a backup-reader at a durable boundary and creates a retained manual backup.
    ///
    /// Same role, single-flight, and stop/resume rules as
    /// [#createScheduledBackup()]; the manual slot is retained beyond the
    /// scheduled retention sweep.
    ///
    /// @throws peruncs.datagrid.cluster.errors.NodeException if the backup or
    /// the resume fails
    public void createManualBackup() {
        this.assembly.backupNodeManager().createStorageBackup(true);
    }

    /// Returns one immutable status snapshot.
    ///
    /// @return the current role, readiness, and replication metrics
    public NodeStatus status() {
        final StorageNodeControl control = this.control();
        return new NodeStatus(control.isWriter(), control.isReady(), control.isHealthy(),
                control.isRunningStorageChecks(), control.readStorageSizeBytes(),
                replication(control));
    }

    private StorageNodeControl control() {
        return switch (this.role) {
            case WRITER, READER -> this.assembly.storageNodeManager();
            case BACKUP_READER -> this.assembly.backupNodeManager().storage();
        };
    }

    /// Translates the internal raw metrics into the exported status view.
    ///
    /// A node configured without replication reports no metrics at all: the
    /// internal `none` transport marker becomes an absent [ReplicationStatus]
    /// instead of a record full of placeholder values.
    private static ReplicationStatus replication(final StorageNodeControl control) {
        final ReplicationMetrics metrics = control.replicationMetrics();
        if (metrics.transport() == null || metrics.transport().isBlank()
            || "none".equalsIgnoreCase(metrics.transport())) {
            return null;
        }
        return new ReplicationStatus(metrics.state(), metrics.currentSequence(), metrics.latestSequence(),
                present(metrics.archiveUsableSpaceBytes()), present(metrics.writerDurablePosition()),
                present(metrics.writerDurableSequence()), present(metrics.appliedSequence()));
    }

    /// Maps the internal negative-is-unknown sentinel to absence.
    private static OptionalLong present(final long value) {
        return value < 0L ? OptionalLong.empty() : OptionalLong.of(value);
    }

    @Override
    public void close() {
        this.assembly.close();
    }
}
