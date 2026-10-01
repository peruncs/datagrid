package peruncs.cluster.node.replication;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.cluster.node.backup.BackupMetadata;
import peruncs.cluster.storage.ReplicationPosition;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;
import peruncs.cluster.storage.binary.ReplicationApplier;
import peruncs.cluster.storage.binary.TypeDictionaryOutbox;
import peruncs.cluster.storage.binary.StorageBinaryDataReceiver;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/// Replication transport for one PerunCS Cluster node.
///
/// The transport supplies the publisher, reader, position, health, and
/// retention implementations; [#noOp()] covers nodes with replication
/// disabled. This is an intentional domain port, not a broker compatibility
/// layer: the neutral node package uses it to avoid a dependency on Aeron and
/// to represent the supported `none` role without nulls. Aeron is currently
/// the only production implementation.
public interface ClusterReplicationTransport extends AutoCloseable {
    /// Registers transport-owned roots before the Store foundation starts.
    ///
    /// @param foundation Store foundation
    default void registerPersistentRoots(final EmbeddedStorageFoundation<?> foundation) {
    }

    /// Returns the stable Store mark registered by this transport.
    ///
    /// @return transport-owned replication mark, or `null` for standalone nodes
    default ReplicationMark replicationMark() {
        return null;
    }

    /// Updates the mark immediately before its enclosing Store commit is serialized.
    ///
    /// @param mark mark included in the next Store transaction
    default void prepareReplicationCommit(final ReplicationMark mark) {
    }

    /// Cancels a mark reservation if the Store commit fails before target preparation.
    default void cancelReplicationCommit(final ReplicationMark mark) {
    }

    /// Retries a locally accepted Store COMMIT whose initial Aeron offer timed out.
    default void retryPendingCommit() {
    }

    /// Returns the retry cadence for a pending Store COMMIT.
    default Duration pendingCommitRetryInterval() {
        return Duration.ofSeconds(1);
    }

    /// Persists the first writer mark or validates the mark loaded from Store.
    default void ensureWriterMark(final StorageConnection storage) {
    }

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
        public ReplicationApplier clientFromMark(
                final StorageBinaryDataReceiver receiver,
                final ReplicationMark startingMark
        ) {
            return ReplicationApplier.noOp();
        }

        @Override
        public ReplicationPositionProvider positionProvider() {
            return new ReplicationPositionProvider() {
                public void init() {
                }

                public ReplicationPosition latest() {
                    return ReplicationPosition.NONE;
                }

                public void close() {
                }
            };
        }

        @Override
        public ReplicationLogRetention retention() {
            return new ReplicationLogRetention() {
                public MaintenanceResult deleteThrough(final ReplicationPosition ignored) {
                    return new MaintenanceResult(MaintenanceResult.Status.NOTHING_TO_DELETE, -1, "no replication log");
                }

                public void close() {
                }
            };
        }

        @Override
        public UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
                final TypeDictionaryOutbox outbox,
                final BooleanSupplier distributionEnabled,
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

    /// Starts a reader at the boundary stored in its replication mark.
    ///
    /// The replayer delivers complete binaries and type dictionaries to the
    /// receiver directly; the receiver stays owned by its creator, which also
    /// disposes it.
    ///
    /// @param receiver destination for received binaries and dictionaries
    /// @param startingMark Store-resident reader boundary
    /// @return replication applier
    ReplicationApplier clientFromMark(
            StorageBinaryDataReceiver receiver,
            ReplicationMark startingMark
    );

    /// Returns the provider's latest published position used for backup/bootstrap.
    ///
    /// @return position provider
    ReplicationPositionProvider positionProvider();

    /// Returns a provider-specific, safe log-retention controller.
    ///
    /// @return retention controller
    ReplicationLogRetention retention();

    /// Runs one bounded retention pass at the latest durable writer position.
    ///
    /// @return maintenance result, or a no-op result when retention is unsupported
    default ReplicationLogRetention.MaintenanceResult maintainRetention() {
        final ReplicationLogRetention retention = retention();
        return retention.isSupported()
                ? retention.deleteThrough(positionProvider().latest())
                : new ReplicationLogRetention.MaintenanceResult(
                        ReplicationLogRetention.MaintenanceResult.Status.NOTHING_TO_DELETE,
                        -1L, "retention is unsupported");
    }

    /// Creates health state independent of any transport implementation.
    ///
    /// @param storage storage readiness view
    /// @param client      replication applier
    /// @return health view
    ReplicationHealth health(
            StorageControllerAdapter storage,
            ReplicationApplier client
    );

    /// Creates a target wrapper for coordinated publication.
    ///
    /// The wrapper owns the Store transaction boundary: local acceptance and
    /// publication are one operation, coordinated by the transport. Writer
    /// roles additionally validate the graph against the index policy before
    /// every distributed write, using the supplied writer storage connection.
    ///
    /// @param outbox       receives the exported type dictionaries
    /// @param distributionEnabled whether writes are currently replicated; `false` while the
    ///                     node bootstraps its Store
    /// @param writerStorage supplies the writer's storage connection at write
    ///                      time, or `null` before it exists; implementations
    ///                      use it for pre-publication validation
    /// @return target factory
    UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
            TypeDictionaryOutbox outbox,
            BooleanSupplier distributionEnabled,
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
