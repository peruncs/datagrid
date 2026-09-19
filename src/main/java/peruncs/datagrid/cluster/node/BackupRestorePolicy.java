package peruncs.datagrid.cluster.node;

import peruncs.datagrid.cluster.node.aeron.ReseedRequiredException;
import peruncs.datagrid.cluster.node.backup.BackupMetadata;
import peruncs.datagrid.cluster.node.backup.StorageBackupBackend;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.node.replication.ReplicationPositionProvider;
import peruncs.datagrid.cluster.node.replication.StoredReplicationCursorManager;
import peruncs.datagrid.cluster.storage.types.ReplicationCursor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/// Decides whether a node restores a shared-volume backup, and installs the
/// selected image together with its durable cursor.
///
/// Compatibility by cluster, Store generation, epoch, and recording is
/// validated before anything is deleted or installed, so a backup from an
/// unrelated generation on a shared volume can never overwrite valid local
/// storage. A local cursor from another transport or Store generation is
/// never mixed with local files; it is discarded together with the Store
/// before extraction. An equal-sequence cursor must also point at the same
/// provider position. A local cursor ahead of the newest compatible backup
/// is retained because restoring an older image would lose data.
final class BackupRestorePolicy {
    private static final System.Logger LOGGER = System.getLogger(BackupRestorePolicy.class.getName());

    private final ClusterReplicationTransport transport;
    private final ReplicationPositionProvider positionProvider;
    private final Supplier<StoredReplicationCursorManager> cursorManager;
    private final Supplier<Path> storageParentPath;
    private final Consumer<Path> deleteDirectory;
    private final Runnable closeCursorManager;
    private final Runnable deleteOffsetFile;

    BackupRestorePolicy(
            final ClusterReplicationTransport transport,
            final ReplicationPositionProvider positionProvider,
            final Supplier<StoredReplicationCursorManager> cursorManager,
            final Supplier<Path> storageParentPath,
            final Consumer<Path> deleteDirectory,
            final Runnable closeCursorManager,
            final Runnable deleteOffsetFile
    ) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.positionProvider = Objects.requireNonNull(positionProvider, "positionProvider");
        this.cursorManager = Objects.requireNonNull(cursorManager, "cursorManager");
        this.storageParentPath = Objects.requireNonNull(storageParentPath, "storageParentPath");
        this.deleteDirectory = Objects.requireNonNull(deleteDirectory, "deleteDirectory");
        this.closeCursorManager = Objects.requireNonNull(closeCursorManager, "closeCursorManager");
        this.deleteOffsetFile = Objects.requireNonNull(deleteOffsetFile, "deleteOffsetFile");
    }

        /// Resolves the backup identity this node restores as.
    ///
    /// The transport configuration is authoritative for stable cluster and
    /// Store-generation identity. A live position and durable local cursor
    /// may fill an epoch or recording that was unknown during wiring, but
    /// they never erase configured values. Aeron nodes fail closed if the
    /// required cluster/generation identity is still unavailable after both
    /// fallible reads; each read failure is handled locally and can never be
    /// confused with the Aeron identity check.
    ///
    /// @return best available node backup identity
    /// @throws ReseedRequiredException when an Aeron node lacks required identity
    BackupMetadata.Identity configuredIdentity() {
        BackupMetadata.Identity provider = this.transport.configuredBackupIdentity();
        try {
            provider = provider.fillUnknowns(BackupMetadata.Identity.of(this.positionProvider.latest()));
        } catch (final RuntimeException unavailable) {
            LOGGER.log(System.Logger.Level.DEBUG,
                    "Replication provider reports no backup identity; falling back to the local cursor",
                    unavailable);
        }

        ReplicationCursor local = null;
        try {
            local = this.cursorManager.get().get();
        } catch (final RuntimeException unreadable) {
            LOGGER.log(System.Logger.Level.DEBUG,
                    "Local replication cursor is unreadable; restoring without its identity", unreadable);
        }
        final BackupMetadata.Identity merged = local == null
                ? provider
                : provider.fillUnknowns(BackupMetadata.Identity.of(local));
        if ("aeron".equalsIgnoreCase(this.transport.id()) &&
            (merged.clusterId() == null || merged.storeGeneration() == null)) {
            throw new ReseedRequiredException(
                    "Aeron backup selection requires configured cluster and Store-generation identity");
        }
        return merged;
    }

        /// Installs the newest compatible backup when local storage cannot be
    /// trusted to represent it, and reports whether a restore happened.
    ///
    /// @param storageRootPath Store root directory
    /// @param backend         backup backend
    /// @return `true` when a backup was installed
    /// @throws NodeLibraryException when an incompatible or internally
    ///                              inconsistent backup was selected
    boolean restoreLatestBackupIfRequired(
            final Path storageRootPath,
            final StorageBackupBackend backend
    ) {
        final boolean storageExists = Files.isDirectory(storageRootPath);
        final BackupMetadata.Identity configured = this.configuredIdentity();
        final BackupMetadata selected = backend.findLatestCompatibleBackup(configured);
        if (selected == null) {
            if (!storageExists && backend.containsBackups()) {
                throw new NodeLibraryException(
                        "No backup on the shared volume is compatible with this node %s; refusing to install an unrelated image"
                                .formatted(configured));
            }
            if (!storageExists) {
                return false;
            }
            LOGGER.log(System.Logger.Level.WARNING,
                    "No backup on the shared volume is compatible with this node %s; keeping local storage"
                            .formatted(configured));
            return false;
        }
        final ReplicationCursor backup = backend.getCursorForBackup(selected);
        final BackupMetadata.Identity backupIdentity = BackupMetadata.Identity.of(backup);
        if (!configured.matches(backupIdentity)) {
            throw new NodeLibraryException(
                    "Backup metadata and its stored replication cursor disagree with this node identity; refusing to modify local storage");
        }
        /* Verify metadata against its own cursor before any deletion: a
         * corrupt or mixed archive can pass the configured check (for example
         * when recording is unconfigured) yet disagree internally. A later
         * startup check would reject the installed cursor, but only after
         * local storage was already destroyed. */
        try {
            BackupMetadata.requireConsistentWithCursor(selected, backup);
        } catch (final NodeLibraryException inconsistent) {
            throw new NodeLibraryException(
                    "Backup metadata disagrees with its stored replication cursor; refusing to modify local storage",
                    inconsistent);
        }
        if (!storageExists) {
            this.deleteOffsetFile.run();
            this.restoreBackupAndCursor(backend, selected, backup, storageRootPath);
            return true;
        }

        /* An unreadable local cursor is treated like an unknown boundary: the
         * backup is the newest trusted image, so it replaces the local files
         * instead of failing startup with a raw cursor error. */
        ReplicationCursor local = null;
        try {
            local = this.cursorManager.get().get();
        } catch (final RuntimeException unreadable) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "Local replication cursor is unreadable; replacing local storage with the newest compatible backup",
                    unreadable);
        }
        final boolean localBoundaryUnknown = local == null || local.logicalSequence() < 0;
        final boolean identityMismatch = local != null &&
                                         (!Objects.equals(local.transport(), backup.transport()) ||
                                          !Objects.equals(local.storeGeneration(), backup.storeGeneration()));
        final boolean localBehind = local != null && local.logicalSequence() < backup.logicalSequence();
        final boolean equalSequencePositionMismatch = local != null &&
                                                      local.logicalSequence() == backup.logicalSequence() &&
                                                      !Objects.equals(local.providerPosition(), backup.providerPosition());
        if (!localBoundaryUnknown && !identityMismatch && !localBehind && !equalSequencePositionMismatch) {
            return false;
        }

        LOGGER.log(System.Logger.Level.WARNING,
                "Replacing local storage with the newest compatible backup (local cursor=%s, backup cursor=%s, identityMismatch=%s, localBehind=%s, equalSequencePositionMismatch=%s)"
                        .formatted(
                                local == null ? null : local.logicalSequence(),
                                backup.logicalSequence(),
                                identityMismatch,
                                localBehind,
                                equalSequencePositionMismatch));
        this.closeCursorManager.run();
        this.deleteDirectory.accept(storageRootPath);
        this.deleteOffsetFile.run();
        this.restoreBackupAndCursor(backend, selected, backup, storageRootPath);
        return true;
    }

        /// Installs a backup and its cursor as one trusted startup boundary.
    ///
    /// @param backend         backup backend
    /// @param selected        selected backup metadata
    /// @param backup          archived cursor
    /// @param storageRootPath Store root directory
    private void restoreBackupAndCursor(
            final StorageBackupBackend backend,
            final BackupMetadata selected,
            final ReplicationCursor backup,
            final Path storageRootPath
    ) {
        try {
            backend.restoreBackup(this.storageParentPath.get(), selected);
            this.cursorManager.get().set(backup);
        } catch (final RuntimeException | Error failure) {
            /* A downloaded Store without its matching cursor is not a valid
             * restart image. Remove it so a later startup cannot mistake the
             * partial boundary for trusted local state. */
            try {
                this.deleteDirectory.accept(storageRootPath);
            } catch (final RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }
}
