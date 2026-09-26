package peruncs.cluster.node.backup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.node.replication.ClusterReplicationTransport;
import peruncs.cluster.node.replication.DurableCursorFile;
import peruncs.cluster.node.replication.ReplicationPositionProvider;
import peruncs.cluster.storage.ReplicationCursor;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;


/// Verifies backup-identity resolution never reports a healthy cursor as the
/// cause of an identity failure and never lets an unreadable cursor hide a
/// configured identity.
class BackupRestorePolicyTest {
    private static final UUID CLUSTER = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID GENERATION = UUID.fromString("22222222-2222-2222-2222-222222222222");

    /// An Aeron node with no resolvable cluster/generation identity fails with
    /// exactly one reseed signal even though the local cursor itself is readable.
    @Test
    void aeronIdentityFailureIsReportedOnceWithoutACursorCause() {
        final BackupRestorePolicy policy = policy(
                transport("aeron", BackupMetadata.Identity.unknown()),
                positionProvider(new NodeException("no live position")),
                cursorManager(ReplicationCursor.NONE));

        final ReseedRequiredException failure =
                assertThrows(ReseedRequiredException.class, policy::configuredIdentity);

        assertTrue(failure.getMessage().contains("requires configured cluster and Store-generation identity"),
                "message must name the missing identity, was: " + failure.getMessage());
        assertFalse(failure.getMessage().contains("unreadable"),
                "a readable cursor must never be reported as unreadable");
        assertNull(failure.getCause(),
                "the identity failure must not be wrapped as its own cause");
    }

    /// A writer with existing local storage keeps it when a compatible
    /// backup exists and the local cursor is unreachable: reloading an older
    /// backup must never overwrite acknowledged local writes.
    @Test
    void writerKeepsExistingLocalStorageWhenBackupIsCompatible(@TempDir final Path temp) throws Exception {
        final Path storageDir = temp.resolve("storage");
        Files.createDirectories(storageDir);
        Files.writeString(storageDir.resolve("data.dat"), "local-content");
        final BackupMetadata.Identity identity = new BackupMetadata.Identity(CLUSTER, GENERATION, 4L, 9L);
        final BackupRestorePolicy policy = policy(
                transport("aeron", identity),
                positionProvider(new NodeException("no live position")),
                cursorManagerFailure(new NodeException("cursor is corrupt")),
                true);

        final Path volume = temp.resolve("backups");
        Files.createDirectories(volume);
        final FakeBackend backend = new FakeBackend(identity);
        final ReplicationCursor backupCursor = ReplicationCursor.of("aeron", GENERATION, 9L,
                new peruncs.cluster.storage.aeron.checkpoint.AeronReplicationCursor(
                        CLUSTER, UUID.randomUUID(), GENERATION, 7L, 1L, 4L, 0L, 9L).encode());
        final BackupMetadata backup = BackupMetadata.create(1_000L, false, backupCursor);
        backend.backups.add(backup);
        backend.cursorForBackup = backupCursor;

        assertFalse(policy.restoreLatestBackupIfRequired(storageDir, backend),
                "a writer with existing local storage must not replace it from a backup");
        assertTrue(Files.exists(storageDir.resolve("data.dat")),
                "the writer's local files must survive an available backup");
        assertEquals(0, backend.restoreCalls, "a writer must never call the restore path");
    }

    /// A configured transport identity survives an unreadable local cursor.
    @Test
    void unreadableCursorFallsBackToConfiguredIdentity() {
        final BackupMetadata.Identity configured = new BackupMetadata.Identity(CLUSTER, GENERATION, 4L, 9L);
        final BackupRestorePolicy policy = policy(
                transport("aeron", configured),
                positionProvider(new NodeException("no live position")),
                cursorManagerFailure(new NodeException("cursor is corrupt")));

        final BackupMetadata.Identity resolved = policy.configuredIdentity();

        assertEquals(configured, resolved);
    }

    private static BackupRestorePolicy policy(
            final ClusterReplicationTransport transport,
            final ReplicationPositionProvider positionProvider,
            final DurableCursorFile cursorManager) {
        return policy(transport, positionProvider, cursorManager, false);
    }

    private static BackupRestorePolicy policy(
            final ClusterReplicationTransport transport,
            final ReplicationPositionProvider positionProvider,
            final DurableCursorFile cursorManager,
            final boolean ownAuthoritativeStore) {
        return new BackupRestorePolicy(
                transport,
                positionProvider,
                () -> cursorManager,
                () -> Path.of("build", "test-storage"),
                path -> {
                },
                () -> {
                },
                () -> {
                },
                ownAuthoritativeStore);
    }

    private static ClusterReplicationTransport transport(
            final String id, final BackupMetadata.Identity identity) {
        return (ClusterReplicationTransport) Proxy.newProxyInstance(
                ClusterReplicationTransport.class.getClassLoader(),
                new Class<?>[]{ClusterReplicationTransport.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "id" -> id;
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
            public ReplicationCursor latest() {
                throw latestFailure;
            }

            @Override
            public void close() {
            }
        };
    }

    private static DurableCursorFile cursorManager(final ReplicationCursor cursor) {
        return cursorManagerResult(cursor, null);
    }

    private static DurableCursorFile cursorManagerFailure(final RuntimeException failure) {
        return cursorManagerResult(null, failure);
    }

    private static DurableCursorFile cursorManagerResult(
            final ReplicationCursor cursor, final RuntimeException failure) {
        return new DurableCursorFile() {
            @Override
            public ReplicationCursor get() {
                if (failure != null) throw failure;
                return cursor;
            }

            @Override
            public void set(final ReplicationCursor value) {
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
        private final java.util.List<BackupMetadata> backups = new java.util.ArrayList<>();
        private Object cursorForBackup;
        int restoreCalls;

        FakeBackend(final BackupMetadata.Identity identity) {
            Objects.requireNonNull(identity, "identity");
        }

        @Override
        public java.util.List<BackupMetadata> listBackups() {
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
                                 final ReplicationCursor cursor,
                                 final BackupMetadata metadata) {
        }

        @Override
        public void deleteBackup(final BackupMetadata metadata) {
        }

        @Override
        public ReplicationCursor getCursorForBackup(final BackupMetadata metadata) {
            return (ReplicationCursor) this.cursorForBackup;
        }

        @Override
        public void restoreBackup(final Path targetParentPath, final BackupMetadata metadata) {
            this.restoreCalls++;
        }
    }
}
