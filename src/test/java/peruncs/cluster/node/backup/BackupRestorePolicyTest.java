package peruncs.cluster.node.backup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.node.replication.ClusterReplicationTransport;
import peruncs.cluster.node.replication.ReplicationPositionProvider;
import peruncs.cluster.storage.ReplicationPosition;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;


/// Verifies backup-identity resolution never reports an available position as the
/// cause of an identity failure and never lets an unavailable position hide a
/// configured identity.
class BackupRestorePolicyTest {
    private static final UUID CLUSTER = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID GENERATION = UUID.fromString("22222222-2222-2222-2222-222222222222");

    /// An Aeron node with no resolvable cluster/generation identity fails with
    /// exactly one reseed signal even though a local position is available.
    @Test
    void aeronIdentityFailureIsReportedOnceWithoutAPositionCause() {
        final BackupRestorePolicy policy = policy(
                transport(true, BackupMetadata.Identity.unknown()),
                positionProvider(new NodeException("no live position")));

        final ReseedRequiredException failure =
                assertThrows(ReseedRequiredException.class, policy::configuredIdentity);

        assertTrue(failure.getMessage().contains("requires configured cluster and Store-generation identity"),
                "message must name the missing identity, was: " + failure.getMessage());
        assertFalse(failure.getMessage().contains("unavailable"),
                "an available position must not be reported as unavailable");
        assertNull(failure.getCause(),
                "the identity failure must not be wrapped as its own cause");
    }

    /// A writer with existing local storage keeps it when a compatible
    /// backup exists and the local position is unreachable: reloading an older
    /// backup must never overwrite acknowledged local writes.
    @Test
    void writerKeepsExistingLocalStorageWhenBackupIsCompatible(@TempDir final Path temp) throws Exception {
        final Path storageDir = temp.resolve("storage");
        Files.createDirectories(storageDir);
        Files.writeString(storageDir.resolve("data.dat"), "local-content");
        final BackupMetadata.Identity identity = new BackupMetadata.Identity(CLUSTER, GENERATION, 4L, 9L);
        final BackupRestorePolicy policy = policy(
                transport(true, identity),
                positionProvider(new NodeException("no live position")),
                true);

        final Path volume = temp.resolve("backups");
        Files.createDirectories(volume);
        final FakeBackend backend = new FakeBackend(identity);
        final ReplicationPosition backupCursor =
                new ReplicationPosition(CLUSTER, GENERATION, 7L, 4L, 9L, 0L, 1L, UUID.randomUUID());
        final BackupMetadata backup = BackupMetadata.create(1_000L, false, backupCursor);
        backend.backups.add(backup);
        backend.cursorForBackup = backupCursor;

        assertFalse(policy.restoreLatestBackupIfRequired(storageDir, () -> backend),
                "a writer with existing local storage must not replace it from a backup");
        assertTrue(Files.exists(storageDir.resolve("data.dat")),
                "the writer's local files must survive an available backup");
        assertEquals(0, backend.restoreCalls, "a writer must never call the restore path");
    }

    /// An existing local Store never touches the backup volume, so an unavailable volume cannot block startup.
    @Test
    void existingLocalStorageNeverCreatesTheBackupBackend(@TempDir final Path temp) throws Exception {
        final Path storageDir = temp.resolve("storage");
        Files.createDirectories(storageDir);
        Files.writeString(storageDir.resolve("data.dat"), "local-content");
        final BackupMetadata.Identity identity = new BackupMetadata.Identity(CLUSTER, GENERATION, 4L, 9L);
        final BackupRestorePolicy policy = policy(
                transport(true, identity), positionProvider(new NodeException("no live position")), false);

        assertFalse(policy.restoreLatestBackupIfRequired(storageDir, () -> {
            throw new AssertionError("the backup volume must not be opened");
        }));
    }

    /// A missing writer Store cannot be rebuilt from its old replication boundary or a reader seed.
    @Test
    void missingWriterStoreWithRecoveryStateRequiresReseed(@TempDir final Path temp) {
        final BackupMetadata.Identity identity = new BackupMetadata.Identity(CLUSTER, GENERATION, 4L, 9L);
        final BackupRestorePolicy policy = policy(
                transport(true, identity),
                positionProvider(new NodeException("no live position")),
                true);
        final FakeBackend backend = new FakeBackend(identity);
        backend.backups.add(new BackupMetadata(1_000L, false, CLUSTER, GENERATION,
                4L, 9L, BackupMetadata.UNKNOWN, BackupMetadata.UNKNOWN, BackupMetadata.UNKNOWN,
                null, UUID.randomUUID(), BackupMetadata.UNKNOWN));

        final ReseedRequiredException failure = assertThrows(ReseedRequiredException.class,
                () -> policy.restoreLatestBackupIfRequired(temp.resolve("missing-storage"), () -> backend));

        assertTrue(failure.getMessage().contains("writer Store is absent or empty"));
        assertEquals(0, backend.restoreCalls, "writer recovery must not install a reader backup");
    }

    /// A fresh writer also cannot adopt a reader backup as its authoritative image.
    @Test
    void missingWriterStoreCannotRestoreSharedReaderBackup(@TempDir final Path temp) {
        final BackupMetadata.Identity identity = new BackupMetadata.Identity(CLUSTER, GENERATION, 4L, 9L);
        final BackupRestorePolicy policy = policy(
                transport(true, identity),
                positionProvider(new NodeException("no live position")),
                true);
        final FakeBackend backend = new FakeBackend(identity);
        final ReplicationPosition backupCursor =
                new ReplicationPosition(CLUSTER, GENERATION, 7L, 4L, 9L, 0L, 1L, UUID.randomUUID());
        final BackupMetadata backup = BackupMetadata.create(1_000L, false, backupCursor);
        backend.backups.add(backup);
        backend.cursorForBackup = backupCursor;

        final ReseedRequiredException failure = assertThrows(ReseedRequiredException.class,
                () -> policy.restoreLatestBackupIfRequired(temp.resolve("missing-storage"), () -> backend));

        assertTrue(failure.getMessage().contains("shared reader backup"));
        assertEquals(0, backend.restoreCalls, "a reader seed must not become the writer image");
    }

    /// A configured transport identity survives an unavailable local position.
    @Test
    void unavailablePositionFallsBackToConfiguredIdentity() {
        final BackupMetadata.Identity configured = new BackupMetadata.Identity(CLUSTER, GENERATION, 4L, 9L);
        final BackupRestorePolicy policy = policy(
                transport(true, configured),
                positionProvider(new NodeException("no live position")));

        final BackupMetadata.Identity resolved = policy.configuredIdentity();

        assertEquals(configured, resolved);
    }

    private static BackupRestorePolicy policy(
            final ClusterReplicationTransport transport,
            final ReplicationPositionProvider positionProvider,
            final boolean ownAuthoritativeStore) {
        return new BackupRestorePolicy(
                transport,
                positionProvider,
                new BackupRestorePolicy.RestoreActions(() -> Path.of("build", "test-storage"), path -> { }),
                ownAuthoritativeStore);
    }

    private static BackupRestorePolicy policy(
            final ClusterReplicationTransport transport,
            final ReplicationPositionProvider positionProvider) {
        return policy(transport, positionProvider, false);
    }

    private static ClusterReplicationTransport transport(
            final boolean replicated, final BackupMetadata.Identity identity) {
        return (ClusterReplicationTransport) Proxy.newProxyInstance(
                ClusterReplicationTransport.class.getClassLoader(),
                new Class<?>[]{ClusterReplicationTransport.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "replicationMark" -> replicated
                            ? new ReplicationMark(CLUSTER, GENERATION, 1L, 1L) : null;
                    case "configuredBackupIdentity" -> identity;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static ReplicationPositionProvider positionProvider(final RuntimeException latestFailure) {
        return new ReplicationPositionProvider() {
            @Override
            public void init() {
            }

            @Override
            public ReplicationPosition latest() {
                throw latestFailure;
            }

            @Override
            public void close() {
            }
        };
    }

    private static Object defaultValue(final Class<?> type) {
        if (!type.isPrimitive()) return null;
        if (type == boolean.class) return false;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        throw new AssertionError("unsupported primitive " + type);
    }

    /// In-memory backup backend stub: backs backup-listing queries; restore
    /// invocations are counted, never performed.
    private static final class FakeBackend implements StorageBackupBackend {
        private final List<BackupMetadata> backups = new ArrayList<>();
        private Object cursorForBackup;
        int restoreCalls;

        FakeBackend(final BackupMetadata.Identity identity) {
            Objects.requireNonNull(identity, "identity");
        }

        @Override
        public List<BackupMetadata> listBackups() {
            return this.backups;
        }

        @Override
        public boolean hasUserUploadedStorage() {
            return false;
        }

        @Override
        public void restoreUserUploadedStorage(final Path targetParentPath) {
            this.restoreCalls++;
        }

        @Override
        public void deleteUserUploadedStorage() {
        }

        @Override
        public void createBackup(final org.eclipse.store.storage.types.StorageConnection storageConnection,
                                 final BackupMetadata metadata) {
        }

        @Override
        public void deleteBackup(final BackupMetadata metadata) {
        }

        @Override
        public ReplicationPosition retentionBoundary(final BackupMetadata metadata) {
            return (ReplicationPosition) this.cursorForBackup;
        }

        @Override
        public void restoreBackup(final Path targetParentPath, final BackupMetadata metadata) {
            this.restoreCalls++;
        }
    }
}
