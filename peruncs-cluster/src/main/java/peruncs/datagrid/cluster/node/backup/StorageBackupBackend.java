package peruncs.datagrid.cluster.node.backup;

import org.eclipse.store.storage.types.StorageConnection;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ReplicationCursor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.eclipse.serializer.math.XMath.notNegative;

/// This backend stores and retrieves the durable files that make up a backup.
///
/// Backup metadata identifies the message position associated with the
/// stored files. Implementations must not report a backup as usable until its
/// storage and metadata are complete.
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
    /// @return backups ordered by implementation policy
    /// @throws NodeLibraryException if listing fails
    List<BackupMetadata> listBackups() throws NodeLibraryException;

        /// Downloads the latest usable backup.
    ///
    /// @param targetRootPath destination root
    /// @throws NodeLibraryException if download fails
    void downloadLatestBackup(Path targetRootPath) throws NodeLibraryException;

        /// Reads the replication cursor from an earlier backup.
    ///
    /// @param skip number of newest backups to skip; zero selects the newest
    /// @return stored replication cursor, when present
    /// @throws NodeLibraryException if reading fails
    Optional<ReplicationCursor> getCursorFromPreviousBackup(int skip) throws NodeLibraryException;

        /// Reports whether at least one backup exists.
    ///
    /// @return `true` when a backup exists
    /// @throws NodeLibraryException if listing fails
    default boolean containsBackups() throws NodeLibraryException {
        return !this.listBackups().isEmpty();
    }

        /// Returns the newest backup allowed by the slot policy.
    ///
    /// @param ignoreManualSlot whether to ignore the manual slot
    /// @return newest backup, or `null`
    /// @throws NodeLibraryException if listing fails
    default BackupMetadata latestBackup(final boolean ignoreManualSlot) throws NodeLibraryException {
        return this.listBackups()
                .stream()
                .filter(b -> !ignoreManualSlot || !b.manualSlot())
                .max(Comparator.comparingLong(BackupMetadata::timestamp))
                .orElse(null);
    }

        /// Returns a backup counted from newest to oldest.
    ///
    /// @param skip number of newest backups to skip; zero selects the newest
    /// @return selected backup, when present
    /// @throws NodeLibraryException if listing fails
    default Optional<BackupMetadata> getLastBackup(final int skip) throws NodeLibraryException {
        /* Zero selects the newest backup; larger values skip that many newer
         * complete backups.  Negative values are never meaningful. */
        notNegative(skip);

        /* Implementations may return an immutable snapshot. Sorting a copy keeps
         * this default method independent of the list implementation. */
        final var backups = new ArrayList<>(this.listBackups());

        if (backups.size() <= skip) {
            // no previous storage
            return Optional.empty();
        }

        backups.sort(Comparator.comparingLong(BackupMetadata::timestamp));

        return Optional.of(backups.get(backups.size() - 1 - skip));
    }

        /// Deletes one backup.
    ///
    /// @param backup backup to delete
    /// @throws NodeLibraryException if deletion fails
    void deleteBackup(BackupMetadata backup) throws NodeLibraryException;

        /// Creates and uploads one backup.
    ///
    /// @param connection storage connection
    /// @param cursor     replication cursor to store
    /// @param backup     backup metadata
    /// @throws NodeLibraryException if creation or upload fails
    void createAndUploadBackup(StorageConnection connection, final ReplicationCursor cursor, BackupMetadata backup)
            throws NodeLibraryException;

        /// Downloads one backup.
    ///
    /// @param storageDestinationParentPath destination parent
    /// @param backup                       backup to download
    /// @throws NodeLibraryException if download fails
    void downloadBackup(Path storageDestinationParentPath, BackupMetadata backup) throws NodeLibraryException;

        /// Reports whether user-uploaded storage exists.
    ///
    /// @return `true` when user storage exists
    /// @throws NodeLibraryException if the check fails
    boolean hasUserUploadedStorage() throws NodeLibraryException;

        /// Downloads user-uploaded storage.
    ///
    /// @param storageDestinationParentPath destination parent
    /// @throws NodeLibraryException if download fails
    void downloadUserUploadedStorage(Path storageDestinationParentPath) throws NodeLibraryException;

        /// Deletes user-uploaded storage.
    ///
    /// @throws NodeLibraryException if deletion fails
    void deleteUserUploadedStorage() throws NodeLibraryException;

}
