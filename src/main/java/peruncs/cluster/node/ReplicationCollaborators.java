package peruncs.cluster.node;

import peruncs.cluster.errors.NodeException;
import peruncs.cluster.node.NodeCollaborators.LazyHolder;
import peruncs.cluster.node.aeron.AeronTransport;
import peruncs.cluster.node.replication.ClusterReplicationTransport;
import peruncs.cluster.node.replication.ReplicationLogRetention;
import peruncs.cluster.node.replication.ReplicationPositionProvider;
import peruncs.cluster.storage.binary.ReplicationApplier;
import peruncs.cluster.storage.binary.StorageBinaryDataMerger;
import peruncs.cluster.storage.binary.TypeDictionaryOutbox;
import org.eclipse.store.storage.types.StorageConnection;

import java.util.concurrent.atomic.AtomicBoolean;

/// The lazily created replication services of one node, and the writer-side state shared with its
/// persistence target.
///
/// The transport, position provider, retention, reader client and merger depend on each other and on
/// the running Store, which the owning [NodeCollaborators] publishes after start; they are created on
/// first use through the owner.
final class ReplicationCollaborators {
    final LazyHolder<ClusterReplicationTransport> transport;
    final LazyHolder<ReplicationPositionProvider> positionProvider;
    final LazyHolder<ReplicationLogRetention> retention;
    final LazyHolder<ReplicationApplier> applier;
    final LazyHolder<StorageBinaryDataMerger> merger;
    /// Type dictionaries the writer re-sends with its next transaction.
    final TypeDictionaryOutbox outbox = new TypeDictionaryOutbox();
    /// Set while the Store starts, so the writes that open it are not replicated.
    final AtomicBoolean distributionIgnored = new AtomicBoolean();
    private final NodeCollaborators owner;

    /// Creates the group.
    ///
    /// @param owner                collaborators the services depend on
    /// @param configuredTransport transport override, or `null` for the configured transport
    ReplicationCollaborators(final NodeCollaborators owner, final ClusterReplicationTransport configuredTransport) {
        this.owner = owner;
        this.transport = LazyHolder.of(() -> configuredTransport != null ? configuredTransport
                : this.createTransport());
        this.positionProvider = LazyHolder.of(() -> this.transport.get().positionProvider());
        this.retention = LazyHolder.of(() -> this.transport.get().retention());
        this.merger = LazyHolder.of(this::createMerger);
        this.applier = LazyHolder.of(() -> this.transport.get().clientFromMark(
                this.merger.get(), this.transport.get().replicationMark()));
    }

    private ClusterReplicationTransport createTransport() {
        return switch (this.owner.nodeConfig.replicationTransport()) {
            case NONE -> ClusterReplicationTransport.noOp();
            case AERON -> new AeronTransport(this.owner.nodeConfig);
        };
    }

    private StorageBinaryDataMerger createMerger() {
        final StorageConnection replicationStorage = this.owner.embeddedStorageManager != null
                ? this.owner.embeddedStorageManager
                : this.owner.clusterStorageManager;
        if (replicationStorage == null) {
            throw new NodeException(
                    "cannot create the replication merger before embedded storage has started");
        }
        final var config = this.owner.nodeConfig;
        final var graph = this.owner.graphCoordinator;
        final var defaults = StorageBinaryDataMerger.Configuration.create(
                this.owner.getEmbeddedStorageFoundation().getConnectionFoundation(),
                replicationStorage, graph::write, graph);
        return StorageBinaryDataMerger.create(new StorageBinaryDataMerger.Configuration(
                defaults.foundation(),
                defaults.storage(),
                defaults.graphUpdater(),
                config.timeouts().mergerCache().toMillis(),
                config.limits().applyQueueBytes(),
                config.limits().applyQueueMaxBytes(),
                config.limits().bufferPoolRetainedBytes(),
                config.timeouts().applyBudget().toMillis(),
                defaults.disposeOrderlyTimeoutMs(),
                defaults.disposeInterruptTimeoutMs(),
                config.limits().maxValidatedIndexObjects(),
                config.timeouts().indexRefresh().toMillis(),
                graph));
    }
}
