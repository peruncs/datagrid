package peruncs.datagrid.cluster.node.backup;

import org.eclipse.store.storage.types.StorageConnection;
import peruncs.datagrid.cluster.errors.NodeException;
import peruncs.datagrid.cluster.storage.ReplicationCursor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.eclipse.serializer.math.XMath.notNegative;

/// Stores and retrieves the durable files that make up a backup.
///
/// Backup metadata identifies the message position associated with the
/// stored files. Implementations must not report a backup as usable until its
/// storage and metadata are complete.
///
/// Selection is ordered by [BackupMetadata#OLDEST_FIRST]: the replication
/// sequence when the backup carries one, otherwise the creation timestamp with
/// the random backup id as the deterministic tie-breaker. Nodes sharing one
/// backup volume must keep their clocks NTP-disciplined because timestamps are
/// wall-clock based.
public interface StorageBackupBackend {
        /// Name of the storage directory inside an archive.
    String STORAGE_ENTRY = "storage";
        /// Name of the replication manifest inside an archive.
    String MANIFEST_ENTRY = "manifest";
        /// Name of the completed-backup marker inside an archive.
    String READY_ENTRY = "ready";
        /// File name reserved for user-uploaded storage.
    String USER_UPLOADED_STORAGE_ARCHIVE = "user-uploaded-storage.zip";

        /// Lists complete usable backups.
    ///
    /// @return backups ordered oldest first by [BackupMetadata#OLDEST_FIRST]
    /// @throws NodeException if listing fails
    List<BackupMetadata> listBackups() throws NodeException;

        /// Lists archives that exist on the volume but cannot be trusted.
    ///
    /// These are archives with unreadable or mismatched identity metadata
    /// that [listBackups] deliberately skips. The default backend has no such
    /// archives and reports none; filesystem backends report their names so
    /// the maintenance path can surface them to operators instead of losing
    /// them silently.
    ///
    /// @return archive names that are present but unreadable, possibly empty
    /// @throws NodeException if the scan fails
    default List<String> listUnreadableArchives() throws NodeException {
        return List.of();
    }

        /// Reads the replication cursor stored with one selected backup.
    ///
    /// The cursor is read for a backup that was already selected for
    /// compatibility, so restores never mix a cursor from an unrelated
    /// generation with the installed image.
    ///
    /// @param backup selected backup
    /// @return stored replication cursor
    /// @throws NodeException if reading fails
    ReplicationCursor getCursorForBackup(BackupMetadata backup) throws NodeException;

        /// Selects the newest backup compatible with the given node identity.
    ///
    /// Backups from another cluster, store generation, epoch, or recording
    /// are skipped, so a node on a shared volume never installs an unrelated
    /// image. Ordering follows [BackupMetadata#OLDEST_FIRST].
    ///
    /// @param configured node identity to check against
    /// @return newest compatible backup, or `null` when there is none
    /// @throws NodeException if listing fails
    default BackupMetadata findLatestCompatibleBackup(final BackupMetadata.Identity configured)
            throws NodeException {
        Objects.requireNonNull(configured, "configured");
        return this.listBackups().stream()
                .filter(backup -> backup.isCompatibleWith(configured))
                .max(BackupMetadata.OLDEST_FIRST)
                .orElse(null);
    }

        /// Reports whether at least one backup exists.
    ///
    /// @return `true` when a backup exists
    /// @throws NodeException if listing fails
    default boolean containsBackups() throws NodeException {
        return !this.listBackups().isEmpty();
    }

        /// Returns a backup counted from newest to oldest.
    ///
    /// Zero selects the newest backup; larger values skip that many newer
    /// complete backups. Ordering follows [BackupMetadata#NEWEST_FIRST], the
    /// same rule used to select the latest compatible backup. Negative values
    /// are never meaningful.
    ///
    /// @param skip number of newest backups to skip; zero selects the newest
    /// @return selected backup, or `null` when there is no such backup
    /// @throws NodeException if listing fails
    default BackupMetadata getLastBackup(final int skip) throws NodeException {
        notNegative(skip);

        /* Implementations may return an immutable snapshot. Sorting a copy keeps
         * this default method independent of the list implementation. */
        final var backups = new ArrayList<>(this.listBackups());
        backups.sort(BackupMetadata.NEWEST_FIRST);

        return skip < backups.size() ? backups.get(skip) : null;
    }

        /// Deletes one backup.
    ///
    /// @param backup backup to delete
    /// @throws NodeException if deletion fails
    void deleteBackup(BackupMetadata backup) throws NodeException;

        /// Creates one backup.
    ///
    /// @param connection storage connection
    /// @param cursor     replication cursor to store
    /// @param backup     backup metadata
    /// @throws NodeException if creation fails
    void createBackup(StorageConnection connection, final ReplicationCursor cursor, BackupMetadata backup)
            throws NodeException;

        /// Restores one backup.
    ///
    /// @param storageDestinationParentPath destination parent
    /// @param backup                       backup to restore
    /// @throws NodeException if restore fails
    void restoreBackup(Path storageDestinationParentPath, BackupMetadata backup) throws NodeException;

        /// Reports whether user-uploaded storage exists.
    ///
    /// @return `true` when user storage exists
    /// @throws NodeException if the check fails
    boolean hasUserUploadedStorage() throws NodeException;

        /// Validates the user-uploaded storage archive in full.
    ///
    /// Implementations must verify structure and enforce every extraction
    /// budget on the real (decompressed) content, because the upload skips
    /// the metadata checks of the generated-backup restore path. The check
    /// runs before any caller destroys local storage; backends without an
    /// upload volume implement it as a no-op.
    ///
    /// @throws NodeException when the upload is missing, ambiguous, partial, or over budget
    default void validateUserUploadedStorage() throws NodeException {
    }

        /// Restores user-uploaded storage.
    ///
    /// Implementations that validate uploads must re-validate on the exact
    /// archive they extract, closing the shared-volume window between the
    /// caller's validation call and this restore.
    ///
    /// @param storageDestinationParentPath destination parent
    /// @throws NodeException if restore fails
    void restoreUserUploadedStorage(Path storageDestinationParentPath) throws NodeException;

        /// Deletes user-uploaded storage.
    ///
    /// @throws NodeException if deletion fails
    void deleteUserUploadedStorage() throws NodeException;

}
