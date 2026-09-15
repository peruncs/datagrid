package peruncs.datagrid.cluster.nodelibrary.backup;

import org.eclipse.store.storage.types.StorageConnection;
import peruncs.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationCursor;

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
        /// Lists complete usable backups.
    ///
    /// @return backups ordered by implementation policy
    /// @throws NodelibraryException if listing fails
    List<BackupMetadata> listBackups() throws NodelibraryException;

        /// Downloads the latest usable backup.
    ///
    /// @param targetRootPath destination root
    /// @throws NodelibraryException if download fails
    void downloadLatestBackup(Path targetRootPath) throws NodelibraryException;

        /// Reads the replication cursor from an earlier backup.
    ///
    /// @param skip number of newest backups to skip; zero selects the newest
    /// @return stored replication cursor, when present
    /// @throws NodelibraryException if reading fails
    Optional<ReplicationCursor> getCursorFromPreviousBackup(int skip) throws NodelibraryException;

        /// Reports whether at least one backup exists.
    ///
    /// @return `true` when a backup exists
    /// @throws NodelibraryException if listing fails
    default boolean containsBackups() throws NodelibraryException {
        return !this.listBackups().isEmpty();
    }

        /// Returns the newest backup allowed by the slot policy.
    ///
    /// @param ignoreManualSlot whether to ignore the manual slot
    /// @return newest backup, or `null`
    /// @throws NodelibraryException if listing fails
    default BackupMetadata latestBackup(final boolean ignoreManualSlot) throws NodelibraryException {
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
    /// @throws NodelibraryException if listing fails
    default Optional<BackupMetadata> getLastBackup(final int skip) throws NodelibraryException {
        /* Zero selects the newest backup; larger values skip that many newer
         * complete backups.  Negative values are never meaningful. */
        notNegative(skip);

        /* Implementations are allowed to return an immutable snapshot (the
         * network backend does so).  Sorting the result in place would therefore
         * make this default method fail only for that backend. */
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
    /// @throws NodelibraryException if deletion fails
    void deleteBackup(BackupMetadata backup) throws NodelibraryException;

        /// Creates and uploads one backup.
    ///
    /// @param connection storage connection
    /// @param cursor     replication cursor to store
    /// @param backup     backup metadata
    /// @throws NodelibraryException if creation or upload fails
    void createAndUploadBackup(StorageConnection connection, final ReplicationCursor cursor, BackupMetadata backup)
            throws NodelibraryException;

        /// Downloads one backup.
    ///
    /// @param storageDestinationParentPath destination parent
    /// @param backup                       backup to download
    /// @throws NodelibraryException if download fails
    void downloadBackup(Path storageDestinationParentPath, BackupMetadata backup) throws NodelibraryException;

        /// Reports whether user-uploaded storage exists.
    ///
    /// @return `true` when user storage exists
    /// @throws NodelibraryException if the check fails
    boolean hasUserUploadedStorage() throws NodelibraryException;

        /// Downloads user-uploaded storage.
    ///
    /// @param storageDestinationParentPath destination parent
    /// @throws NodelibraryException if download fails
    void downloadUserUploadedStorage(Path storageDestinationParentPath) throws NodelibraryException;

        /// Deletes user-uploaded storage.
    ///
    /// @throws NodelibraryException if deletion fails
    void deleteUserUploadedStorage() throws NodelibraryException;

}
