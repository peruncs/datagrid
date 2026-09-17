package peruncs.datagrid.cluster.node.replication;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.datagrid.cluster.node.backup.BackupMetadata;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataReceiver;

import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/// Aeron replication transport for one Data Grid cluster instance.
///
/// The transport supplies the distributor, reader, position, health, and
/// retention implementations; [#noOp()] covers nodes with replication
/// disabled.
public interface ClusterReplicationTransport extends AutoCloseable {
        /// Returns the configured identity used to filter compatible backups.
    ///
    /// @return configured backup identity, or an unknown identity when the
    /// transport has no persistent generation
    default BackupMetadata.Identity configuredBackupIdentity() {
        return BackupMetadata.Identity.unknown();
    }
        /// Creates a transport that performs no replication.
    ///
    /// @return disabled transport
    static ClusterReplicationTransport noOp() {

        return new ClusterReplicationTransport() {

            private final ReplicationCursor cursor = new ReplicationCursor("none", null, -1, "");

            @Override
            public String id() {
                return "none";
            }

            @Override
            public StorageBinaryDataDistributor distributor(final String streamName, final boolean asynchronous) {
                return StorageBinaryDataDistributor.NoOp();
            }

            @Override
            public StorageBinaryDataClient client(
                    final StorageBinaryDataReceiver receiver,
                    final String streamName,
                    final AfterDataMessageConsumedListener cursorListener,
                    final ReplicationCursor startingCursor,
                    final boolean commitPosition
            ) {
                return StorageBinaryDataClient.NoOp(startingCursor);
            }

            @Override
            public ReplicationPositionProvider positionProvider(final String streamName) {
                return new ReplicationPositionProvider() {
                    public void init() {
                    }

                    public ReplicationCursor latest() {
                        return cursor;
                    }

                    public void close() {
                    }
                };
            }

            @Override
            public ReplicationLogRetention retention() {
                return new ReplicationLogRetention() {
                    public MaintenanceResult deleteThrough(final ReplicationCursor ignored) {
                        return new MaintenanceResult(MaintenanceResult.Status.NOTHING_TO_DELETE, -1, "no replication log");
                    }

                    public void close() {
                    }
                };
            }

            @Override
            public UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
                    final String streamName,
                    final StorageBinaryDataDistributor distributor,
                    final Supplier<StorageConnection> writerStorage
            ) {
                /* No replication: the local target is the whole story. */
                return delegate -> delegate;
            }

            @Override
            public ReplicationHealth health(
                    final StorageControllerAdapter storage,
                    final StorageBinaryDataClient client
            ) {
                return new ReplicationHealth() {
                    public boolean isReady() {
                        return storage.isReady();
                    }

                    public boolean isHealthy() {
                        return storage.isReady();
                    }

                    public void init() {
                        // No-op transport has no health state to initialize.
                    }

                    public void close() {
                        // No-op transport holds no health resources.
                    }
                };
            }

            @Override
            public void close() {
            }
        };
    }

        /// Returns the stable provider id, `aeron` or `none`.
    ///
    /// @return provider id
    String id();

        /// Creates a writer-side binary distributor for the named logical stream.
    /// Implementations may reject direct data publication when local Store
    /// acceptance must be coordinated; use [#persistenceTargetFactory(String, StorageBinaryDataDistributor)] for that transaction boundary.
    ///
    /// @param streamName   logical stream name
    /// @param asynchronous whether publication may be asynchronous
    /// @return binary distributor
    StorageBinaryDataDistributor distributor(String streamName, boolean asynchronous);

        /// Creates a reader-side client starting at the supplied durable cursor.
    ///
    /// The client delivers complete binaries and type dictionaries to the
    /// receiver directly; the receiver stays owned by its creator, which also
    /// disposes it.
    ///
    /// @param receiver       destination for received binaries and dictionaries
    /// @param streamName     logical stream name
    /// @param cursorListener callback after data is applied
    /// @param startingCursor durable starting cursor
    /// @param commitPosition whether reader positions are committed
    /// @return binary data client
    StorageBinaryDataClient client(
            StorageBinaryDataReceiver receiver,
            String streamName,
            AfterDataMessageConsumedListener cursorListener,
            ReplicationCursor startingCursor,
            boolean commitPosition
    );

        /// Returns the provider's latest published position used for backup/bootstrap.
    ///
    /// @param streamName logical stream name
    /// @return position provider
    ReplicationPositionProvider positionProvider(String streamName);

        /// Returns a provider-specific, safe log-retention controller.
    ///
    /// @return retention controller
    ReplicationLogRetention retention();

        /// Creates health state independent of any provider client implementation.
    ///
    /// @param storage storage readiness view
    /// @param client  reader client
    /// @return health view
    ReplicationHealth health(
            StorageControllerAdapter storage,
            StorageBinaryDataClient client
    );

    /// Creates a target wrapper without writer-side index validation.
    ///
    /// Convenience overload for tests and transports whose graph has no index
    /// policy to enforce; production wiring supplies the writer storage and
    /// uses [#persistenceTargetFactory(String, StorageBinaryDataDistributor, Supplier)].
    ///
    /// @param streamName  logical stream name
    /// @param distributor binary distributor
    /// @return target factory without index validation
    default UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
            final String streamName,
            final StorageBinaryDataDistributor distributor
    ) {
        return this.persistenceTargetFactory(streamName, distributor, () -> null);
    }

    /// Creates a target wrapper for coordinated publication.
    ///
    /// The wrapper owns the Store transaction boundary: local acceptance and
    /// publication are one operation, coordinated by the transport. Writer
    /// roles additionally validate the graph against the index policy before
    /// every distributed write, using the supplied writer storage connection.
    ///
    /// @param streamName   logical stream name
    /// @param distributor  binary distributor
    /// @param writerStorage supplies the writer's storage connection at write
    ///                      time, or `null` before it exists; implementations
    ///                      use it for pre-publication validation
    /// @return target factory
    UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
            String streamName,
            StorageBinaryDataDistributor distributor,
            Supplier<StorageConnection> writerStorage
    );

    @Override
    void close();

        /// Small view that avoids making the transport depend on Store internals.
    interface StorageControllerAdapter {
                /// Reports whether local storage is ready.
        ///
        /// @return `true` when ready
        boolean isReady();
    }
}
