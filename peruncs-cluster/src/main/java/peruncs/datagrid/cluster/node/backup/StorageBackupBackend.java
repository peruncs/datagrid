package peruncs.datagrid.cluster.node.backup;

import org.eclipse.store.storage.types.StorageConnection;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ReplicationCursor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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

        /// Restores the latest usable backup.
    ///
    /// @param targetRootPath destination root
    /// @throws NodeLibraryException if restore fails
    void restoreLatestBackup(Path targetRootPath) throws NodeLibraryException;

        /// Reads the replication cursor from an earlier backup.
    ///
    /// @param skip number of newest backups to skip; zero selects the newest
    /// @return stored replication cursor, or `null` when there is no earlier backup
    /// @throws NodeLibraryException if reading fails
    ReplicationCursor getCursorFromPreviousBackup(int skip) throws NodeLibraryException;

        /// Reports whether at least one backup exists.
    ///
    /// @return `true` when a backup exists
    /// @throws NodeLibraryException if listing fails
    default boolean containsBackups() throws NodeLibraryException {
        return !this.listBackups().isEmpty();
    }

        /// Returns a backup counted from newest to oldest.
    ///
    /// Zero selects the newest backup; larger values skip that many newer
    /// complete backups. Negative values are never meaningful.
    ///
    /// @param skip number of newest backups to skip; zero selects the newest
    /// @return selected backup, or `null` when there is no such backup
    /// @throws NodeLibraryException if listing fails
    default BackupMetadata getLastBackup(final int skip) throws NodeLibraryException {
        notNegative(skip);

        /* Implementations may return an immutable snapshot. Sorting a copy keeps
         * this default method independent of the list implementation. */
        final var backups = new ArrayList<>(this.listBackups());
        backups.sort(Comparator.comparingLong(BackupMetadata::timestamp).reversed());

        return skip < backups.size() ? backups.get(skip) : null;
    }

        /// Deletes one backup.
    ///
    /// @param backup backup to delete
    /// @throws NodeLibraryException if deletion fails
    void deleteBackup(BackupMetadata backup) throws NodeLibraryException;

        /// Creates one backup.
    ///
    /// @param connection storage connection
    /// @param cursor     replication cursor to store
    /// @param backup     backup metadata
    /// @throws NodeLibraryException if creation fails
    void createBackup(StorageConnection connection, final ReplicationCursor cursor, BackupMetadata backup)
            throws NodeLibraryException;

        /// Restores one backup.
    ///
    /// @param storageDestinationParentPath destination parent
    /// @param backup                       backup to restore
    /// @throws NodeLibraryException if restore fails
    void restoreBackup(Path storageDestinationParentPath, BackupMetadata backup) throws NodeLibraryException;

        /// Reports whether user-uploaded storage exists.
    ///
    /// @return `true` when user storage exists
    /// @throws NodeLibraryException if the check fails
    boolean hasUserUploadedStorage() throws NodeLibraryException;

        /// Restores user-uploaded storage.
    ///
    /// @param storageDestinationParentPath destination parent
    /// @throws NodeLibraryException if restore fails
    void restoreUserUploadedStorage(Path storageDestinationParentPath) throws NodeLibraryException;

        /// Deletes user-uploaded storage.
    ///
    /// @throws NodeLibraryException if deletion fails
    void deleteUserUploadedStorage() throws NodeLibraryException;

}
