package peruncs.cluster.node.replication;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.cluster.node.backup.BackupMetadata;
import peruncs.cluster.storage.ReplicationCursor;
import peruncs.cluster.storage.binary.ReplicationApplier;
import peruncs.cluster.storage.binary.ReplicationPublisher;
import peruncs.cluster.storage.binary.StorageBinaryDataReceiver;

import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/// Replication transport for one Data Grid cluster instance.
///
/// The transport supplies the publisher, reader, position, health, and
/// retention implementations; [#noOp()] covers nodes with replication
/// disabled. This is an intentional domain port, not a broker compatibility
/// layer: the neutral node package uses it to avoid a dependency on Aeron and
/// to represent the supported `none` role without nulls. Aeron is currently
/// the only production implementation.
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
    /// @return the shared disabled transport
    static ClusterReplicationTransport noOp() {
        return NoOp.INSTANCE;
    }

        /// Shared stateless transport for nodes with replication disabled.
    final class NoOp implements ClusterReplicationTransport {
        private static final NoOp INSTANCE = new NoOp();

        private NoOp() {
        }

        @Override
        public String id() {
            return "none";
        }

        @Override
        public ReplicationPublisher distributor(final String streamName) {
            return ReplicationPublisher.noOp();
        }

        @Override
        public ReplicationApplier client(
                final StorageBinaryDataReceiver receiver,
                final String streamName,
                final CommitAppliedListener cursorListener,
                final ReplicationCursor startingCursor
        ) {
            return ReplicationApplier.noOp(startingCursor);
        }

        @Override
        public ReplicationPositionProvider positionProvider(final String streamName) {
            return new ReplicationPositionProvider() {
                public void init() {
                }

                public ReplicationCursor latest() {
                    return ReplicationCursor.NONE;
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
                final ReplicationPublisher distributor,
                final Supplier<StorageConnection> writerStorage
        ) {
            /* No replication: the local target is the whole story. */
            return delegate -> delegate;
        }

        @Override
        public ReplicationHealth health(
                final StorageControllerAdapter storage,
                final ReplicationApplier client
        ) {
            return new ReplicationHealth() {
                public boolean isReady() {
                    return storage.isReady();
                }

                public boolean isHealthy() {
                    return storage.isReady();
                }

                public void close() {
                    /* The stateless transport holds no health resources. */
                }
            };
        }

        @Override
        public void close() {
        }
    }

        /// Returns the stable provider id, `aeron` or `none`.
    ///
    /// @return provider id
    String id();

        /// Creates a writer-side replication publisher for the named logical stream.
    /// Implementations may reject direct data publication when local Store
    /// acceptance must be coordinated; use [#persistenceTargetFactory(String, ReplicationPublisher)] for that transaction boundary.
    ///
    /// @param streamName logical stream name
    /// @return replication publisher
    ReplicationPublisher distributor(String streamName);

        /// Creates a reader-side replayer starting at the supplied durable cursor.
    ///
    /// The replayer delivers complete binaries and type dictionaries to the
    /// receiver directly; the receiver stays owned by its creator, which also
    /// disposes it.
    ///
    /// @param receiver       destination for received binaries and dictionaries
    /// @param streamName     logical stream name
    /// @param cursorListener callback after data is applied
    /// @param startingCursor durable starting cursor
    /// @return replication applier
    ReplicationApplier client(
            StorageBinaryDataReceiver receiver,
            String streamName,
            CommitAppliedListener cursorListener,
            ReplicationCursor startingCursor
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

        /// Creates health state independent of any transport implementation.
    ///
    /// @param storage storage readiness view
    /// @param client      replication applier
    /// @return health view
    ReplicationHealth health(
            StorageControllerAdapter storage,
            ReplicationApplier client
    );

    /// Creates a target wrapper without writer-side index validation.
    ///
    /// Convenience overload for tests and transports whose graph has no index
    /// policy to enforce; production wiring supplies the writer storage and
    /// uses [#persistenceTargetFactory(String, ReplicationPublisher, Supplier)].
    ///
    /// @param streamName  logical stream name
    /// @param distributor replication publisher
    /// @return target factory without index validation
    default UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
            final String streamName,
            final ReplicationPublisher distributor
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
    /// @param distributor  replication publisher
    /// @param writerStorage supplies the writer's storage connection at write
    ///                      time, or `null` before it exists; implementations
    ///                      use it for pre-publication validation
    /// @return target factory
    UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
            String streamName,
            ReplicationPublisher distributor,
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
