package peruncs.datagrid.cluster.node.backup;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.errors.NodeException;
import peruncs.datagrid.cluster.errors.ReseedRequiredException;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.node.replication.DurableCursorFile;
import peruncs.datagrid.cluster.node.replication.ReplicationPositionProvider;
import peruncs.datagrid.cluster.storage.ReplicationCursor;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
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
                });
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
}
