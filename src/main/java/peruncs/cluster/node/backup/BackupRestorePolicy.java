package peruncs.cluster.node.backup;

import peruncs.cluster.storage.io.AtomicFileWriter;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.node.replication.ClusterReplicationTransport;
import peruncs.cluster.node.replication.ReplicationPositionProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/// Selects a compatible seed for a node with no local Store.
///
/// Existing local Store state is left in place. A backup is only a seed for a
/// missing reader Store; the Store-resident replication mark controls resume.
public final class BackupRestorePolicy {
    private static final System.Logger LOGGER = System.getLogger(BackupRestorePolicy.class.getName());

    private final ClusterReplicationTransport transport;
    private final ReplicationPositionProvider positionProvider;
    private final RestoreActions actions;
    private final boolean ownAuthoritativeStore;

    public BackupRestorePolicy(
            final ClusterReplicationTransport transport,
            final ReplicationPositionProvider positionProvider,
            final RestoreActions actions,
            final boolean ownAuthoritativeStore
    ) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.positionProvider = Objects.requireNonNull(positionProvider, "positionProvider");
        this.actions = Objects.requireNonNull(actions, "actions");
        this.ownAuthoritativeStore = ownAuthoritativeStore;
    }

    /// Actions needed to install a backup seed.
    ///
    /// @param storageParentPath   resolves the Store parent
    /// @param deleteDirectory     removes the old Store image
    public record RestoreActions(Supplier<Path> storageParentPath, Consumer<Path> deleteDirectory) {
        /// Validates the restore callbacks.
        public RestoreActions {
            Objects.requireNonNull(storageParentPath, "storageParentPath");
            Objects.requireNonNull(deleteDirectory, "deleteDirectory");
        }
    }

    /// Resolves the backup identity this node restores as.
    ///
    /// The transport configuration is authoritative for stable cluster and
    /// Store-generation identity. A live position may fill an epoch or
    /// recording that was unknown during wiring, but never erases configured
    /// values. Aeron nodes fail closed if cluster/generation identity remains
    /// unavailable.
    ///
    /// @return best available node backup identity
    /// @throws ReseedRequiredException when an Aeron node lacks required identity
    BackupMetadata.Identity configuredIdentity() {
        BackupMetadata.Identity provider = this.transport.configuredBackupIdentity();
        if (!this.ownAuthoritativeStore) {
            try {
                provider = provider.fillUnknowns(BackupMetadata.Identity.of(this.positionProvider.latest()));
            } catch (final NodeException unavailable) {
                /* Only a typed 'no boundary yet' is tolerated; a programming error must surface. */
                LOGGER.log(System.Logger.Level.DEBUG,
                        "Replication provider reports no backup identity; using configured identity",
                        unavailable);
            }
        }
        if (this.transport.replicationMark() != null &&
            (provider.clusterId() == null || provider.storeGeneration() == null)) {
            throw new ReseedRequiredException(
                    "Aeron backup selection requires configured cluster and Store-generation identity");
        }
        return provider;
    }

    /// Installs the newest compatible backup when no local Store exists.
    ///
    /// @param storageRootPath Store root directory
    /// An existing local Store is always retained, and the backup volume is not touched
    /// at all: a read-only or unavailable cold-backup volume must not stop a node that
    /// already has its Store.
    ///
    /// @param backendSupplier supplies the backup backend; called only when the Store is missing
    /// @return `true` when a backup was installed
    /// @throws NodeException when no compatible backup exists or restore fails
    public boolean restoreLatestBackupIfRequired(
            final Path storageRootPath,
            final Supplier<? extends StorageBackupBackend> backendSupplier
    ) {
        final boolean storageExists = !isMissingOrEmpty(storageRootPath);
        if (storageExists) {
            LOGGER.log(System.Logger.Level.INFO,
                    "Existing local storage found; retaining it and its Store replication mark");
            return false;
        }
        final StorageBackupBackend backend = backendSupplier.get();
        final BackupMetadata.Identity configured = this.configuredIdentity();
        final BackupMetadata selected = backend.findLatestCompatibleBackup(configured);
        if (selected == null) {
            if (this.ownAuthoritativeStore && backend.containsBackups()) {
                throw new ReseedRequiredException(
                        "writer Store is absent or empty; the shared reader backup volume cannot provide an authoritative writer image");
            }
            if (backend.containsBackups()) {
                throw new NodeException(
                        "No backup on the shared volume is compatible with this node %s; refusing to install an unrelated image"
                                .formatted(configured));
            }
            return false;
        }
        if (this.ownAuthoritativeStore) {
            throw new ReseedRequiredException(
                    "writer Store is absent or empty; a shared reader backup cannot replace the authoritative writer image");
        }
        this.restoreBackup(backend, selected, storageRootPath);
        return true;
    }

    /// Installs a selected backup as a reader seed.
    ///
    /// @param backend         backup backend
    /// @param selected        selected backup metadata
    /// @param storageRootPath Store root directory
    private void restoreBackup(
            final StorageBackupBackend backend,
            final BackupMetadata selected,
            final Path storageRootPath
    ) {
        try {
            backend.restoreBackup(this.actions.storageParentPath().get(), selected);
        } catch (final RuntimeException | Error failure) {
            /* A partial extraction must not be mistaken for a valid Store. */
            try {
                this.actions.deleteDirectory().accept(storageRootPath);
            } catch (final RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private static boolean isMissingOrEmpty(final Path directory) {
        return AtomicFileWriter.isMissingOrEmptyStore(directory);
    }
}
