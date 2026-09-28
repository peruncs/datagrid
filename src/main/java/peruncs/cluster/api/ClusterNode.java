package peruncs.cluster.api;

import peruncs.cluster.errors.BackupBusyException;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.errors.WrongRoleException;
import peruncs.cluster.node.NodeAssembly;
import peruncs.cluster.node.NodeRole;
import peruncs.cluster.node.StorageNodeControl;
import peruncs.cluster.node.replication.ReplicationMetrics;

import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/// One owned cluster-node lifecycle and its guarded Store.
///
/// Closing this object closes every Store, Aeron, backup, and worker resource
/// it created. Borrowed controls are never exposed.
///
/// @param <T> root type
public final class ClusterNode<T> implements AutoCloseable {
    private final NodeAssembly assembly;
    private final ClusterStorageManager<T> storage;
    private final NodeRole role;

    private ClusterNode(final NodeAssembly assembly, final ClusterStorageManager<T> storage, final NodeRole role) {
        this.assembly = assembly;
        this.storage = storage;
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
    /// `ECLIPSE_DATAGRID_AERON_WIRE_NONCE` and the shared-lease acknowledgement.
    /// The replication protocol has no node authentication or transport
    /// encryption, by design.
    ///
    /// Startup fails closed: when durable local state cannot be reconciled
    /// with the Archive, or any required setting is missing or inconsistent,
    /// this method throws before any Store, Aeron, or background-thread
    /// resource escapes, and everything already created is closed.
    ///
    /// @param <T> root type
    /// @param options immutable node options
    /// @return the started node and its owned Store view
    /// @throws NodeException if settings, filesystem state, or a node resource prevents startup
    /// @throws ReseedRequiredException if local data cannot be reconciled with the writer history
    public static <T> ClusterNode<T> open(final NodeOptions<T> options) {
        Objects.requireNonNull(options, "options");
        final NodeAssembly.Builder builder = NodeAssembly.create()
                .setRootSupplier(options.rootSupplier()::get);
        /* General embedding hooks: a custom Store foundation (tuning, custom
         * type handlers, backup setup — the live file provider is always
         * node-derived) and a programmatic settings source. */
        if (options.embeddedStorageFoundation() != null) {
            builder.setEmbeddedStorageFoundation(options.embeddedStorageFoundation());
        }
        if (options.nodeSettingsSource() != null) {
            builder.setNodeSettingsSource(options.nodeSettingsSource());
        }
        return open(builder);
    }

    @SuppressWarnings("unchecked")
    private static <T> ClusterNode<T> open(final NodeAssembly.Builder builder) {
        final NodeAssembly assembly = builder.build();
        try {
            /* NodeOptions<T> is the typed boundary: the root supplier's type
             * fixes the intended manager root type; existing disk data must
             * also satisfy the documented schema/root contract. The supplier
             * alone cannot prove a deserialized payload's type, and the
             * assembly stores Supplier<Object> because it is role-generic, so
             * this single cast is where the application's type meets the
             * assembly's erasure. */
            final ClusterStorageManager<T> storage =
                    (ClusterStorageManager<T>) assembly.startStorageManager();
            return new ClusterNode<>(assembly, storage, assembly.nodeRole());
        } catch (final RuntimeException | Error failure) {
            try {
                assembly.close();
            } catch (final Throwable closeFailure) {
                if (closeFailure != failure) failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /// Returns the guarded, Store-compatible storage manager owned by this node.
    ///
    /// The manager is a drop-in [org.eclipse.store.storage.types.StorageManager]:
    /// common Store interfaces remain available under the documented role,
    /// root, coordination, and lifecycle restrictions. Reads and mutations on
    /// the graph join [GraphBoundary]; its `shutdown()` performs the complete
    /// node teardown, as does closing this node.
    ///
    /// @return the guarded Store facade owned by this node's lifecycle
    public ClusterStorageManager<T> storageManager() {
        return this.storage;
    }

    /// Starts periodic storage checks.
    ///
    /// This operation is valid for writer and reader nodes. A backup-reader
    /// does not run the ordinary storage-check scheduler.
    public void startStorageChecks() {
        this.control().startStorageChecks();
    }

    /// Creates a backup asynchronously at a resolved replication boundary.
    ///
    /// A concurrent request completes exceptionally with [BackupBusyException].
    /// The future completes with the published backup details or the operation
    /// failure. Cancelling the future does not cancel the backup task.
    ///
    /// @param slot scheduled or retained manual slot
    /// @return completion of the backup
    /// @throws WrongRoleException unless this is a backup-reader
    public CompletableFuture<BackupInfo> createBackup(final BackupSlot slot) {
        Objects.requireNonNull(slot, "slot");
        if (this.role != NodeRole.BACKUP_READER) {
            throw new WrongRoleException("createBackup is only available on a backup-reader");
        }
        return this.assembly.backupNodeManager().createStorageBackup(slot);
    }

    /// Returns one immutable status snapshot.
    ///
    /// This is a production-node API: a development node owns no role
    /// manager, so status (and [startStorageChecks]) is rejected for it
    /// with [WrongRoleException].
    ///
    /// @return the current role, readiness, and replication metrics
    public NodeStatus status() {
        final StorageNodeControl control = this.control();
        return new NodeStatus(control.isWriter(), control.isReady(), control.isHealthy(),
                control.isRunningStorageChecks(), control.readStorageSizeBytes(),
                replication(control), this.role == NodeRole.BACKUP_READER
                        ? this.assembly.backupNodeManager().backupStatus() : BackupStatus.empty());
    }

    private StorageNodeControl control() {
        return switch (this.role) {
            case WRITER, READER -> this.assembly.storageNodeManager();
            case BACKUP_READER -> this.assembly.backupNodeManager().storage();
        };
    }

    /// Translates the internal raw metrics into the exported status view.
    ///
    /// A node configured without replication reports an explicit
    /// `NOT_CONFIGURED` state with empty metric boundaries.
    private static ReplicationStatus replication(final StorageNodeControl control) {
        final ReplicationMetrics metrics = control.replicationMetrics();
        if (metrics == null) {
            return new ReplicationStatus(ReplicationState.NOT_CONFIGURED,
                    OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(),
                    new ReplicationStatus.WriterDurableBoundary(OptionalLong.empty(), OptionalLong.empty()),
                    OptionalLong.empty());
        }
        return new ReplicationStatus(metrics.state(), present(metrics.currentSequence()), present(metrics.latestSequence()),
                present(metrics.archiveUsableSpaceBytes()),
                new ReplicationStatus.WriterDurableBoundary(
                        present(metrics.writerDurablePosition()), present(metrics.writerDurableSequence())),
                present(metrics.appliedSequence()));
    }

    /// Maps the internal negative-is-unknown sentinel to absence.
    private static OptionalLong present(final long value) {
        return value < 0L ? OptionalLong.empty() : OptionalLong.of(value);
    }

    /// Closes the node and every resource it created.
    ///
    /// Equivalent to `storageManager().shutdown()`: the manager's shutdown
    /// triggers the same complete, ordered, idempotent teardown. A close
    /// invoked from inside a [GraphBoundary] section is rejected — unwind the
    /// section first.
    @Override
    public void close() {
        this.assembly.close();
    }
}
