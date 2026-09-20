package peruncs.datagrid.cluster.node.backup;

import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ReplicationCursorStore;
import peruncs.datagrid.cluster.node.store.StorageFileOperations;
import peruncs.datagrid.cluster.storage.types.AtomicFileWriter;
import peruncs.datagrid.cluster.storage.types.ReplicationCursor;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.eclipse.serializer.util.X.notNull;

/// Stores compressed backup archives on a filesystem volume.
///
/// The volume may be network-mounted. A backup becomes visible only after its
/// complete archive has been atomically moved into the volume root, so listing
/// and restore never observe a partially written archive. Publication,
/// replacement, and deletion of volume archives are serialized by one advisory
/// lock file, and the backend fails fast when the volume cannot provide atomic
/// rename and directory synchronization.
public final class FilesystemVolumeBackupBackend implements StorageBackupBackend {
    /* Crash-test seam, mirroring AtomicFileWriter: a bound hook observes the
     * publication windows a forked child can be killed in. Unbound in
     * production, which costs one scoped-value check per metadata step. */
    private static final ScopedValue<BiConsumer<String, Path>> TEST_HOOK = ScopedValue.newInstance();

        /// Runs an action with the backup publication crash hook bound.
    ///
    /// Test bridge only; a blocking hook is valid solely in a forked child the
    /// parent can kill.
    ///
    /// @param hook   callback receiving the crash point name and involved path
    /// @param action guarded backup operation
    static void runWithTestHook(final BiConsumer<String, Path> hook, final Runnable action) {
        ScopedValue.where(TEST_HOOK, Objects.requireNonNull(hook, "hook"))
                .run(Objects.requireNonNull(action, "action"));
    }

    private static void testPoint(final String point, final Path path) {
        final BiConsumer<String, Path> hook = TEST_HOOK.isBound() ? TEST_HOOK.get() : null;
        if (hook != null) hook.accept(point, path);
    }

    static final String EXPORT_WORKSPACE_PREFIX = ".backup-export-";
    /// Age after which an abandoned export workspace is reaped on first use.
    /// A crash can leave a `.backup-export-*` directory behind; anything
    /// older than this bound cannot belong to a live publication on any
    /// reasonable volume, so it is deleted and logged.
    static final Duration ORPHAN_WORKSPACE_MAX_AGE = Duration.ofHours(24);

    private static final System.Logger LOGGER = System.getLogger(FilesystemVolumeBackupBackend.class.getName());
    /* One advisory lock file per volume serializes archive publication.
     * The existence check, the completeness/identity comparison, the atomic
     * rename, and archive deletion must hold while no other publisher acts:
     * without the lock a second publisher can create the destination between
     * the check and the move, or delete a just-published archive while
     * replacing a partial file, silently overwriting one backup with another.
     * The in-JVM mutex covers threads of this process; the file lock covers
     * separate processes sharing the volume. */
    private static final String PUBLISH_LOCK_FILE_NAME = ".publish.lock";
    private static final ConcurrentHashMap<Path, PublicationMutex> PUBLISH_MUTEXES = new ConcurrentHashMap<>();

    private final Path backupVolumePath;
    private final Path userUploadedStorageArchivePath;
    private final BackupArchiveLimits limits;
    private final ConcurrentHashMap<String, CachedArchive> archiveCache = new ConcurrentHashMap<>();
    private final AtomicBoolean orphanWorkspacesReaped = new AtomicBoolean();

    /// Creates a filesystem backup backend with default restore budgets.
    ///
    /// @param backupVolumePath backup volume path
    /// @return filesystem backup backend
    public static FilesystemVolumeBackupBackend New(final Path backupVolumePath) {
        return new FilesystemVolumeBackupBackend(notNull(backupVolumePath).toAbsolutePath().normalize(),
                BackupArchiveLimits.defaults());
    }

    /// Creates a filesystem backup backend with explicit operator budgets.
    ///
    /// @param backupVolumePath backup volume path
    /// @param limits           extraction and entry budgets
    /// @return filesystem backup backend
    public static FilesystemVolumeBackupBackend New(
            final Path backupVolumePath,
            final BackupArchiveLimits limits
    ) {
        return new FilesystemVolumeBackupBackend(
                notNull(backupVolumePath).toAbsolutePath().normalize(),
                notNull(limits));
    }

    private static final class PublicationMutex {
        private int users;
    }

    /// Identifies one archive file version for the metadata cache.
    private record ArchiveStamp(Object fileKey, FileTime modifiedTime, long size) {
    }

    /// One cached archive resolution; a `null` metadata marks an unreadable file.
    private record CachedArchive(String name, ArchiveStamp stamp, BackupMetadata metadata, String unreadableReason) {
    }

    @FunctionalInterface
    private interface VolumeOperation {
        void run() throws NodeLibraryException;
    }

    private FilesystemVolumeBackupBackend(final Path backupVolumePath, final BackupArchiveLimits limits) {
        this.backupVolumePath = backupVolumePath;
        this.userUploadedStorageArchivePath = backupVolumePath.resolve(StorageBackupBackend.USER_UPLOADED_STORAGE_ARCHIVE);
        this.limits = limits;
        this.probeAtomicPublication();
    }

    /// Probes atomic rename and directory fsync before the backend is used.
    ///
    /// Publication fails late and destructively if the volume silently lacks
    /// these primitives, so the capability is verified once at construction
    /// with the same operations publication uses. The probe deliberately does
    /// not use [AtomicFileWriter#verify] because its owner-only temporary file
    /// requires POSIX permissions, while a backup volume may not provide them.
    private void probeAtomicPublication() {
        try {
            this.ensureVolumeDirectory();
        } catch (final NodeLibraryException failure) {
            throw new NodeLibraryException(
                    "Failed to prepare backup volume %s".formatted(this.backupVolumePath), failure);
        }
        final Path probeTarget = this.backupVolumePath.resolve(".publish-probe-" + UUID.randomUUID());
        Path probeSource = null;
        try {
            probeSource = Files.createTempFile(this.backupVolumePath, ".publish-probe-", ".tmp");
            StorageFileOperations.moveFileAtomically(probeSource, probeTarget);
            probeSource = null;
            StorageFileOperations.forceDirectory(this.backupVolumePath);
        } catch (final AtomicMoveNotSupportedException unsupported) {
            throw new NodeLibraryException(
                    "Backup volume %s does not support atomic rename; backups cannot be published"
                            .formatted(this.backupVolumePath), unsupported);
        } catch (final IOException failure) {
            throw new NodeLibraryException(
                    "Backup volume %s cannot publish archives atomically or sync directory metadata"
                            .formatted(this.backupVolumePath), failure);
        } finally {
            deleteProbe(probeTarget);
            if (probeSource != null) deleteProbe(probeSource);
        }
    }

    private static void deleteProbe(final Path probe) {
        try {
            Files.deleteIfExists(probe);
        } catch (final IOException cleanupFailure) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "Failed to delete atomic-publication probe %s".formatted(probe), cleanupFailure);
        }
    }

    @Override
    public ReplicationCursor getCursorForBackup(final BackupMetadata backup) throws NodeLibraryException {
        Objects.requireNonNull(backup, "backup");
        return this.readBackupCursor(this.toArchivePath(backup));
    }

    private ReplicationCursor readBackupCursor(final Path archive) throws NodeLibraryException {
        try {
            return ReplicationCursorStore.decode(BackupArchive.readManifest(
                    archive, this.limits.maxExtractedBytes(), this.limits.maxArchiveEntries()));
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to decode backup manifest from %s".formatted(archive), failure);
        }
    }

    @Override
    public List<BackupMetadata> listBackups() throws NodeLibraryException {
        final List<String> currentNames = new ArrayList<>();
        final List<BackupMetadata> listed = this.listBackupVolumeFiles().stream()
                .filter(BackupArchive::isBackupFileName)
                .map(this::resolveListedBackup)
                .filter(Objects::nonNull)
                .map(cached -> {
                    currentNames.add(cached.name());
                    return cached;
                })
                .map(CachedArchive::metadata)
                .filter(Objects::nonNull)
                .sorted(BackupMetadata.OLDEST_FIRST)
                .toList();
        /* The cache is keyed by unique-per-archive file names, so without a
         * prune it grows for the process's lifetime as backups rotate. Keep
         * only entries the current volume still holds. */
        this.archiveCache.keySet().retainAll(currentNames);
        return listed;
    }

    @Override
    public List<String> listUnreadableArchives() throws NodeLibraryException {
        return this.listBackupVolumeFiles().stream()
                .filter(BackupArchive::isBackupFileName)
                .map(this::resolveListedBackup)
                .filter(Objects::nonNull)
                .filter(entry -> entry.unreadableReason() != null)
                .map(CachedArchive::name)
                .sorted()
                .toList();
    }

    /// Resolves one volume file from the metadata cache or by reading it.
    ///
    /// The cache key is the file name together with the stable file identity,
    /// modification time, and size, so a replaced or rewritten archive is
    /// re-read while repeated listings skip the ZIP open. Filesystems without
    /// a stable file identity are never cached.
    ///
    /// @param name volume file name
    /// @return cached resolution, or `null` when the entry is not a regular file
    private CachedArchive resolveListedBackup(final String name) {
        final Path archive = this.backupVolumePath.resolve(name);
        final ArchiveStamp stamp;
        try {
            final BasicFileAttributes attributes =
                    Files.readAttributes(archive, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isRegularFile()) {
                return null;
            }
            stamp = new ArchiveStamp(attributes.fileKey(), attributes.lastModifiedTime(), attributes.size());
        } catch (final IOException unreadable) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "Failed to inspect backup candidate %s".formatted(archive), unreadable);
            return null;
        }
        final CachedArchive cached = this.archiveCache.get(name);
        if (cached != null && stamp.equals(cached.stamp())) {
            return cached;
        }
        final CachedArchive resolved = this.parseListedBackup(archive, stamp);
        if (stamp.fileKey() != null) {
            this.archiveCache.put(name, resolved);
        }
        return resolved;
    }

    /// Parses one volume file and overlays the archived identity when present.
    ///
    /// The file name carries the selection fields; the identity sidecar
    /// additionally carries provenance and the content digest. Archives
    /// without a sidecar predate generations and list with unknown
    /// provenance. A sidecar that is unreadable or whose selection fields
    /// disagree with the file name makes the file untrustworthy: it is
    /// skipped with a warning instead of being selected, because otherwise
    /// [BackupMetadata] would resolve a different or nonexistent archive.
    ///
    /// @param archive volume file
    /// @param stamp   file version used as the cache key
    /// @return cached resolution, never `null`
    private CachedArchive parseListedBackup(final Path archive, final ArchiveStamp stamp) {
        final String name = archive.getFileName().toString();
        final BackupMetadata parsed;
        try {
            parsed = BackupArchive.parseMetadata(name, this.backupVolumePath);
        } catch (final NodeLibraryException invalidName) {
            return new CachedArchive(name, stamp, null, "invalid backup file name");
        }
        final BackupMetadata identity;
        try {
            identity = BackupArchive.readIdentity(archive);
        } catch (final NodeLibraryException corrupt) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "Skipping backup archive with an unreadable identity at %s".formatted(archive), corrupt);
            return new CachedArchive(name, stamp, null, "unreadable identity sidecar");
        }
        if (identity == null) {
            return new CachedArchive(name, stamp, parsed, null);
        }
        if (!sameSelectionFields(parsed, identity)) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "Skipping backup archive whose identity sidecar disagrees with its file name at %s"
                            .formatted(archive));
            return new CachedArchive(name, stamp, null, "identity sidecar disagrees with the file name");
        }
        return new CachedArchive(name, stamp, identity, null);
    }

    private static boolean sameSelectionFields(final BackupMetadata parsed, final BackupMetadata identity) {
        return parsed.timestamp() == identity.timestamp() &&
               parsed.manualSlot() == identity.manualSlot() &&
               Objects.equals(parsed.clusterId(), identity.clusterId()) &&
               Objects.equals(parsed.storeGeneration(), identity.storeGeneration()) &&
               parsed.epoch() == identity.epoch() &&
               parsed.recordingId() == identity.recordingId() &&
               parsed.logicalSequence() == identity.logicalSequence() &&
               Objects.equals(parsed.backupId(), identity.backupId());
    }

    @Override
    public void deleteBackup(final BackupMetadata backup) throws NodeLibraryException {
        this.deleteArchive(this.toArchivePath(backup));
    }

    @Override
    public void createBackup(final StorageConnection connection, final ReplicationCursor cursor, final BackupMetadata backup)
            throws NodeLibraryException {
        final Path exportDirectory = this.createTemporaryDirectory(EXPORT_WORKSPACE_PREFIX);
        Throwable primaryFailure = null;
        try {
            final var fs = Storage.DefaultFileSystem();
            connection.issueFullBackup(fs.ensureDirectory(exportDirectory.resolve(StorageBackupBackend.STORAGE_ENTRY)));
            ensureNotInterrupted();
            final byte[] manifestBytes;
            try {
                Files.createDirectories(exportDirectory.resolve(StorageBackupBackend.STORAGE_ENTRY));
                StorageFileOperations.forceDirectory(exportDirectory.resolve(StorageBackupBackend.STORAGE_ENTRY));
                manifestBytes = ReplicationCursorStore.encode(cursor);
                AtomicFileWriter.writeBytes(
                        exportDirectory.resolve(StorageBackupBackend.MANIFEST_ENTRY), manifestBytes);
                /* Kill window: the manifest is durable in the workspace but the
                 * ready marker is not, so the export is provably incomplete. */
                testPoint("AFTER_MANIFEST_BEFORE_READY", exportDirectory);
                AtomicFileWriter.write(exportDirectory.resolve(StorageBackupBackend.READY_ENTRY), channel -> {
                });
            } catch (final IOException failure) {
                throw new NodeLibraryException("Failed to write backup replication manifest", failure);
            }

            /* The digest covers the manifest and the storage payload, so a
             * later publication under the same backup id is idempotent
             * only for identical content. The random backup id in the file
             * name keeps concurrent publishers from ever sharing a name. */
            final BackupMetadata stamped =
                    backup.withDigest(BackupArchive.contentDigestOfDirectory(exportDirectory));
            BackupArchive.writeIdentity(
                    exportDirectory.resolve(BackupArchive.BACKUP_IDENTITY_ENTRY), stamped);
            final Path temporaryArchive = exportDirectory.resolve(BackupArchive.toArchiveFileName(stamped));
            BackupArchive.compressStorage(exportDirectory, temporaryArchive);
            ensureNotInterrupted();
            this.publishArchive(temporaryArchive, this.toArchivePath(backup), manifestBytes, stamped.digest());
        } catch (final RuntimeException | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            try {
                StorageFileOperations.cleanup(exportDirectory, primaryFailure);
            } catch (final NodeLibraryException cleanupFailure) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "Failed to clean up backup export workspace %s".formatted(exportDirectory), cleanupFailure);
            }
        }
    }

    private static void ensureNotInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new NodeLibraryException("Storage backup interrupted before publication");
        }
    }

    @Override
    public void restoreBackup(final Path storageDestinationParentPath, final BackupMetadata backup)
            throws NodeLibraryException {
        this.restoreArchive(this.toArchivePath(backup), storageDestinationParentPath, true);
    }

    @Override
    public boolean hasUserUploadedStorage() throws NodeLibraryException {
        this.ensureVolumeDirectory();
        return Files.isRegularFile(this.userUploadedStorageArchivePath, LinkOption.NOFOLLOW_LINKS);
    }

        /// Validates the upload against the operator-configured budgets.
    ///
    /// The check runs before any caller destroys local storage: a partial,
    /// ambiguous, or over-budget upload is refused here with the local image
    /// intact.
    @Override
    public void validateUserUploadedStorage() throws NodeLibraryException {
        this.ensureVolumeDirectory();
        BackupArchive.validateUpload(this.userUploadedStorageArchivePath, this.limits);
    }

        /// Restores user-uploaded storage.
    ///
    /// The upload is re-validated on the same open archive immediately before
    /// extraction: the time between the caller's validation and this restore
    /// is an unguarded window on a shared volume, and re-pinning the rules on
    /// the exact file being extracted closes it. A swapped or in-place
    /// rewritten upload is refused here rather than installed.
    @Override
    public void restoreUserUploadedStorage(final Path storageDestinationParentPath) throws NodeLibraryException {
        this.ensureVolumeDirectory();
        BackupArchive.validateUpload(this.userUploadedStorageArchivePath, this.limits);
        this.restoreArchive(this.userUploadedStorageArchivePath, storageDestinationParentPath, false);
    }

    @Override
    public void deleteUserUploadedStorage() throws NodeLibraryException {
        this.deleteArchive(this.userUploadedStorageArchivePath);
    }

    private void restoreArchive(
            final Path archive,
            final Path storageDestinationParentPath,
            final boolean requireBackupMetadata
    ) throws NodeLibraryException {
        this.createDestinationDirectory(storageDestinationParentPath);
        final Path workingDirectory = this.createTemporaryDirectory(storageDestinationParentPath, ".backup-restore-");
        Throwable primaryFailure = null;
        try {
            final Path extracted = workingDirectory.resolve("extracted");
            BackupArchive.extractArchive(extracted, archive, requireBackupMetadata, this.limits);
            this.verifyExtractedDigest(archive, extracted);
            StorageFileOperations.installStorage(
                    extracted.resolve(StorageBackupBackend.STORAGE_ENTRY),
                    storageDestinationParentPath.resolve(StorageBackupBackend.STORAGE_ENTRY));
        } catch (final RuntimeException | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            StorageFileOperations.cleanup(workingDirectory, primaryFailure);
        }
    }

    /// Rejects an archive whose content no longer matches its identity digest.
    ///
    /// Archives without an identity sidecar predate generations and skip
    /// verification; anything carrying a digest must still match it before
    /// it may replace local storage.
    ///
    /// @param archive   source archive
    /// @param extracted extraction root holding `manifest` and `storage`
    /// @throws NodeLibraryException when the digest contradicts the content
    private void verifyExtractedDigest(final Path archive, final Path extracted) throws NodeLibraryException {
        final BackupMetadata identity = BackupArchive.readIdentity(archive);
        if (identity == null || identity.digest() < 0L) {
            return;
        }
        final long actual = BackupArchive.contentDigestOfDirectory(extracted);
        if (actual != identity.digest()) {
            throw new NodeLibraryException(
                    "Backup archive content digest mismatch at %s; refusing to install".formatted(archive));
        }
    }

    private Path toArchivePath(final BackupMetadata backup) {
        return this.backupVolumePath.resolve(BackupArchive.toArchiveFileName(backup));
    }

    private void publishArchive(
            final Path temporaryArchive,
            final Path destination,
            final byte[] manifestBytes,
            final long digest
    ) throws NodeLibraryException {
        this.withPublicationLock(
                () -> this.publishArchiveLocked(temporaryArchive, destination, manifestBytes, digest));
    }

    /// Runs one operation while holding the volume publication lock.
    ///
    /// The operation is serialized against every other publication and
    /// deletion within this process (the in-JVM mutex) and across processes
    /// sharing the volume (the file lock).
    ///
    /// @param operation operation to run under the lock
    /// @throws NodeLibraryException when the lock cannot be acquired
    private void withPublicationLock(final VolumeOperation operation) throws NodeLibraryException {
        final PublicationMutex mutex = PUBLISH_MUTEXES.compute(this.backupVolumePath, (ignored, current) -> {
            final PublicationMutex retained = current == null ? new PublicationMutex() : current;
            retained.users++;
            return retained;
        });
        try {
            synchronized (mutex) {
                this.ensureVolumeDirectory();
                final Path lockFile = this.backupVolumePath.resolve(PUBLISH_LOCK_FILE_NAME);
                try (FileChannel lockChannel = FileChannel.open(lockFile,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                     FileLock ignored = lockChannel.lock()) {
                    operation.run();
                } catch (final OverlappingFileLockException overlapped) {
                    throw new NodeLibraryException(
                            "Backup volume publication lock is already held at %s".formatted(lockFile), overlapped);
                } catch (final IOException failure) {
                    throw new NodeLibraryException(
                            "Failed to lock backup volume for publication at %s".formatted(lockFile), failure);
                }
            }
        } finally {
            PUBLISH_MUTEXES.computeIfPresent(this.backupVolumePath, (ignored, current) -> {
                current.users--;
                return current.users == 0 ? null : current;
            });
        }
    }

    private void publishArchiveLocked(
            final Path temporaryArchive,
            final Path destination,
            final byte[] manifestBytes,
            final long digest
    ) throws NodeLibraryException {
        /* Kill window: the complete archive is compressed in the workspace and
         * the publication lock is held, but the atomic rename has not run, so
         * the volume must not expose any selectable archive. */
        testPoint("BEFORE_PUBLISH_RENAME", destination);
        try {
            /* An atomic rename replaces an existing destination on Unix
             * instead of failing, so the collision must be detected with
             * an existence check first; the move-time catch only covers
             * a publisher that bypassed the volume lock. */
            if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
                this.resolveSameNamePublication(temporaryArchive, destination, manifestBytes, digest);
            } else {
                try {
                    StorageFileOperations.moveFileAtomically(temporaryArchive, destination);
                } catch (final FileAlreadyExistsException raced) {
                    this.resolveSameNamePublication(temporaryArchive, destination, manifestBytes, digest);
                }
            }
            StorageFileOperations.forceDirectory(this.backupVolumePath);
        } catch (final AtomicMoveNotSupportedException unsupported) {
            throw new NodeLibraryException("Atomic backup publication is not supported", unsupported);
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to publish backup archive %s".formatted(destination), failure);
        }
    }

    /// Resolves a publication whose archive name is already taken.
    ///
    /// A crash between publication and acknowledgement retries the same
    /// backup identity: the retry is idempotent only when the published
    /// archive is complete and holds the identical manifest and content
    /// digest. A same-name partial file is replaced instead of mistaken
    /// for a durable backup, while the same backup id with different
    /// content is a conflicting publication that fails instead of
    /// overwriting — concurrent nodes must differ by backup id.
    ///
    /// Only conclusive incompleteness replaces the destination. A transient
    /// read error propagates and leaves the complete archive untouched.
    ///
    /// @param temporaryArchive new publication in the export workspace
    /// @param destination      occupied archive path
    /// @param manifestBytes    manifest of the new publication
    /// @param digest           content digest of the new publication
    /// @throws NodeLibraryException on a conflicting publication or read failure
    private void resolveSameNamePublication(
            final Path temporaryArchive,
            final Path destination,
            final byte[] manifestBytes,
            final long digest
    ) throws NodeLibraryException {
        try {
            if (!this.isCompleteArchive(destination)) {
                StorageFileOperations.deleteRegularFile(destination);
                StorageFileOperations.moveFileAtomically(temporaryArchive, destination);
            } else if (this.isIdenticalPublication(destination, manifestBytes, digest)) {
                LOGGER.log(System.Logger.Level.DEBUG,
                        "Backup archive is already published at %s".formatted(destination));
                /* The export workspace cleanup deletes this file; a best
                 * effort delete here must not turn a durable backup into
                 * a publication failure. */
                try {
                    Files.deleteIfExists(temporaryArchive);
                } catch (final IOException cleanupFailure) {
                    LOGGER.log(System.Logger.Level.WARNING,
                            "Failed to delete superseded backup workspace file %s".formatted(temporaryArchive),
                            cleanupFailure);
                }
            } else {
                throw new NodeLibraryException(
                        "Conflicting backup archive is already published at %s; refusing to overwrite"
                                .formatted(destination));
            }
        } catch (final AtomicMoveNotSupportedException unsupported) {
            throw new NodeLibraryException("Atomic backup publication is not supported", unsupported);
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to publish backup archive %s".formatted(destination), failure);
        }
    }

    /// Reports whether the published archive holds the same backup.
    ///
    /// Both the immutable manifest and the content digest must match: the
    /// manifest pins the replication position, the digest pins the Store
    /// image. Anything else under the same backup id is a conflict.
    ///
    /// @param destination   published archive
    /// @param manifestBytes manifest of the new publication
    /// @param digest        content digest of the new publication
    /// @return `true` for an idempotent retry of the same backup
    private boolean isIdenticalPublication(
            final Path destination,
            final byte[] manifestBytes,
            final long digest
    ) throws NodeLibraryException {
        final byte[] publishedManifest = BackupArchive.readManifest(
                destination, this.limits.maxExtractedBytes(), this.limits.maxArchiveEntries());
        if (!Arrays.equals(publishedManifest, manifestBytes)) {
            return false;
        }
        /* The archived bytes must digest to the new publication's digest;
         * a stored digest that disagrees with either side means bit-rot. */
        if (BackupArchive.contentDigestOfArchive(destination, this.limits) != digest) {
            return false;
        }
        final BackupMetadata identity = BackupArchive.readIdentity(destination);
        return identity == null || identity.digest() < 0L || identity.digest() == digest;
    }

    /// Reports whether the published file is a complete, valid backup archive.
    ///
    /// Only conclusive evidence of incompleteness — a missing manifest, a
    /// corrupt ZIP structure, truncation, or an invalid name — returns
    /// `false`, allowing the partial file to be replaced. Any other I/O
    /// failure propagates so a durable archive is never destroyed because of a
    /// transient read error.
    ///
    /// @param destination published archive path
    /// @return `true` when the file is a complete backup
    /// @throws NodeLibraryException on any non-conclusive read failure
    private boolean isCompleteArchive(final Path destination) throws NodeLibraryException {
        final String name = destination.getFileName().toString();
        if (!BackupArchive.isBackupFileName(name)) {
            return false;
        }
        try {
            BackupArchive.parseMetadata(name, this.backupVolumePath);
            BackupArchive.readManifest(
                    destination, this.limits.maxExtractedBytes(), this.limits.maxArchiveEntries());
            /* A truncated archive with an intact manifest must not count
             * as a durable backup. */
            return BackupArchive.containsStoragePayload(destination, this.limits.maxArchiveEntries());
        } catch (final BackupArchive.IncompleteArchiveException incomplete) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "Replacing conclusively incomplete backup archive at %s".formatted(destination));
            return false;
        }
    }

    /// Deletes one volume archive while holding the publication lock when the
    /// path is inside the backup volume.
    ///
    /// @param archive archive to delete
    /// @throws NodeLibraryException if deletion fails
    private void deleteArchive(final Path archive) throws NodeLibraryException {
        final Path normalized = archive.toAbsolutePath().normalize();
        if (normalized.startsWith(this.backupVolumePath)) {
            this.withPublicationLock(() -> this.deleteArchiveLocked(normalized));
        } else {
            this.deleteArchiveLocked(normalized);
        }
    }

    private void deleteArchiveLocked(final Path archive) throws NodeLibraryException {
        try {
            if (StorageFileOperations.deleteRegularFile(archive)) {
                StorageFileOperations.forceDirectory(this.backupVolumePath);
            }
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to delete backup archive %s".formatted(archive), failure);
        }
    }

    private void createDestinationDirectory(final Path destination) throws NodeLibraryException {
        try {
            StorageFileOperations.ensureNoSymbolicLinks(destination);
            Files.createDirectories(destination);
            StorageFileOperations.ensureNoSymbolicLinks(destination);
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to create backup destination %s".formatted(destination), failure);
        }
    }

    private Path createTemporaryDirectory(final String prefix) throws NodeLibraryException {
        this.ensureVolumeDirectory();
        return createTemporaryDirectory(this.backupVolumePath, prefix);
    }

    private Path createTemporaryDirectory(final Path parent, final String prefix) throws NodeLibraryException {
        try {
            StorageFileOperations.ensureNoSymbolicLinks(parent);
            Files.createDirectories(parent);
            final Path temporary =
                    Files.createTempDirectory(parent, prefix, privateDirectoryAttributes(parent));
            StorageFileOperations.ensureNoSymbolicLinks(temporary);
            return temporary;
        } catch (final IOException | UnsupportedOperationException | SecurityException failure) {
            throw new NodeLibraryException("Failed to create backup workspace", failure);
        }
    }

    /// Owner-only permissions where the filesystem supports them, default
    /// permissions otherwise. Passing POSIX attributes to a filesystem
    /// without a POSIX view (Windows, archive filesystems) fails directory
    /// creation outright instead of ignoring them.
    static FileAttribute<?>[] privateDirectoryAttributes(final Path parent) {
        if (parent.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            return new FileAttribute<?>[]{PosixFilePermissions.asFileAttribute(Set.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE))};
        }
        return new FileAttribute<?>[0];
    }

    private void ensureVolumeDirectory() throws NodeLibraryException {
        try {
            StorageFileOperations.ensureNoSymbolicLinks(this.backupVolumePath);
            Files.createDirectories(this.backupVolumePath);
            StorageFileOperations.ensureNoSymbolicLinks(this.backupVolumePath);
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to prepare backup volume %s".formatted(this.backupVolumePath), failure);
        }
        this.reapOrphanWorkspaces();
    }

    /// Reaps abandoned export workspaces older than [ORPHAN_WORKSPACE_MAX_AGE].
    ///
    /// A crash between workspace creation and cleanup leaves a
    /// `.backup-export-*` directory behind forever. The first volume use
    /// deletes those older than the bound and logs each removal; fresh
    /// workspaces are left alone because they may belong to a live
    /// publication in another process sharing the volume.
    private void reapOrphanWorkspaces() {
        if (!this.orphanWorkspacesReaped.compareAndSet(false, true)) {
            return;
        }
        final long cutoffMillis = System.currentTimeMillis() - ORPHAN_WORKSPACE_MAX_AGE.toMillis();
        try (final var entries = Files.list(this.backupVolumePath)) {
            entries.filter(path -> path.getFileName().toString().startsWith(EXPORT_WORKSPACE_PREFIX))
                    .filter(path -> isOlderThan(path, cutoffMillis))
                    .forEach(this::deleteOrphanWorkspace);
        } catch (final IOException failure) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "Failed to scan %s for orphaned backup workspaces".formatted(this.backupVolumePath), failure);
        }
    }

    private static boolean isOlderThan(final Path path, final long cutoffMillis) {
        try {
            return Files.getLastModifiedTime(path, LinkOption.NOFOLLOW_LINKS).toMillis() < cutoffMillis;
        } catch (final IOException unreadable) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "Failed to read the age of backup workspace %s".formatted(path), unreadable);
            return false;
        }
    }

    private void deleteOrphanWorkspace(final Path workspace) {
        try {
            StorageFileOperations.deleteDirectory(workspace);
            LOGGER.log(System.Logger.Level.INFO,
                    "Deleted backup workspace %s abandoned for more than %s"
                            .formatted(workspace, ORPHAN_WORKSPACE_MAX_AGE));
        } catch (final NodeLibraryException failure) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "Failed to delete orphaned backup workspace %s".formatted(workspace), failure);
        }
    }

    private List<String> listBackupVolumeFiles() throws NodeLibraryException {
        this.ensureVolumeDirectory();
        try (final var listStream = Files.list(this.backupVolumePath)) {
            return listStream.map(path -> path.getFileName().toString()).sorted().toList();
        } catch (final IOException failure) {
            throw new NodeLibraryException("Failed to iterate backup files at %s".formatted(this.backupVolumePath), failure);
        }
    }
}
