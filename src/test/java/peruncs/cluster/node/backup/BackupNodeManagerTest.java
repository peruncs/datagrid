package peruncs.cluster.node.backup;

import org.eclipse.store.storage.types.StorageController;
import org.junit.jupiter.api.Test;
import peruncs.cluster.api.BackupInfo;
import peruncs.cluster.api.BackupSlot;
import peruncs.cluster.api.BackupStatus;
import peruncs.cluster.storage.ReplicationCursor;
import peruncs.cluster.storage.binary.ReplicationApplier;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies that backup-node health reflects the replication reader state.
class BackupNodeManagerTest {
    private static final ReplicationCursor CURSOR =
            new ReplicationCursor("test", null, 7L, "010203");

    private static BackupNodeManager manager(final FakeClient client, final FakeTasks tasks) {
        final StorageController controller = (StorageController) Proxy.newProxyInstance(
                BackupNodeManagerTest.class.getClassLoader(),
                new Class<?>[]{StorageController.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "isRunning" -> true;
                    case "isStartingUp" -> false;
                    case "toString" -> "running-controller";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> switch (method.getReturnType().getName()) {
                        case "boolean" -> false;
                        case "long" -> 0L;
                        case "int" -> 0;
                        default -> null;
                    };
                });
        return BackupNodeManager.create(tasks, client, controller, () -> 1L, "test");
    }

    /// Verifies a backup node with a running replication reader reports healthy and ready.
    @Test
    void healthyWhileTheReaderIsRunning() {
        final FakeClient client = new FakeClient();
        client.running = true;

        final BackupNodeManager manager = manager(client, new FakeTasks());

        assertTrue(manager.isHealthy());
        assertTrue(manager.isReady());
    }

    /// Verifies a backup node with a stopped reader reports neither healthy nor ready.
    @Test
    void unhealthyWhenTheReaderHasStopped() {
        final FakeClient client = new FakeClient();

        final BackupNodeManager manager = manager(client, new FakeTasks());

        assertFalse(manager.isHealthy(), "a stopped reader must not report healthy");
        assertFalse(manager.isReady());
    }

    /// Verifies a backup node with a failed replication reader reports unhealthy.
    @Test
    void unhealthyAfterAReaderFailure() {
        final FakeClient client = new FakeClient();
        client.running = true;
        client.failure = new IllegalStateException("reader failed");

        final BackupNodeManager manager = manager(client, new FakeTasks());

        assertFalse(manager.isHealthy());
    }

    @Test
    void backupFailureIsReportedWithoutChangingReaderHealth() {
        final FakeClient client = new FakeClient();
        client.running = true;
        final FakeTasks tasks = new FakeTasks();
        tasks.failure = new IllegalStateException("backup failed");

        final BackupNodeManager manager = manager(client, tasks);

        assertTrue(manager.isHealthy());
        assertTrue(manager.isReady());
        assertEquals(new BackupStatus(true, "backup failed", -1L, false, ""), manager.backupStatus());
    }

    @Test
    void postPublicationMaintenanceFailureIsReportedSeparately() {
        final FakeClient client = new FakeClient();
        client.running = true;
        final FakeTasks tasks = new FakeTasks();
        tasks.maintenanceFailure = new IllegalStateException("retention failed");

        final BackupNodeManager manager = manager(client, tasks);

        assertTrue(manager.isHealthy());
        assertEquals(new BackupStatus(false, "", -1L, true, "retention failed"), manager.backupStatus());
    }

    /// Verifies a backup node stays healthy but not ready while an intentional backup holds the single-flight lock and stops the reader.
    @Test
    void healthyDuringAnIntentionalBackupStop() {
        final FakeClient client = new FakeClient();
        final FakeTasks tasks = new FakeTasks();
        tasks.backupRunning = true;
        tasks.backupExecuting = true;

        final BackupNodeManager manager = manager(client, tasks);

        assertTrue(manager.isHealthy(), "a backup holding the single-flight lock stops the reader on purpose");
        assertFalse(manager.isReady(), "readiness still requires the reader to run");
    }

    @Test
    void queuedBackupDoesNotHideAnUnexpectedlyStoppedReader() {
        final FakeClient client = new FakeClient();
        final FakeTasks tasks = new FakeTasks();
        tasks.backupRunning = true;

        assertFalse(manager(client, tasks).isHealthy());
    }

    private static final class FakeClient implements ReplicationApplier {
        private boolean running;
        private RuntimeException failure;

        @Override
        public void start() {
            this.running = true;
        }

        @Override
        public void stopAtLatestMessage() {
            this.running = false;
        }

        @Override
        public ReplicationCursor cursor() {
            return CURSOR;
        }

        @Override
        public boolean isRunning() {
            return this.running;
        }

        @Override
        public RuntimeException failure() {
            return this.failure;
        }

        @Override
        public void resume() {
            this.running = true;
        }

        @Override
        public void dispose() {
            this.running = false;
        }
    }

    private static final class FakeTasks implements StorageBackupTaskExecutor {
        private boolean backupRunning;
        private boolean backupExecuting;
        private Throwable failure;
        private Throwable maintenanceFailure;

        @Override
        public CompletableFuture<BackupInfo> runBackup(final BackupSlot slot) {
            return CompletableFuture.completedFuture(
                    new BackupInfo(UUID.randomUUID(), Instant.now(), CURSOR.logicalSequence(), slot == BackupSlot.MANUAL));
        }

        @Override
        public boolean isRunningBackup() {
            return this.backupRunning;
        }

        @Override
        public boolean isBackupExecuting() {
            return this.backupExecuting;
        }

        @Override
        public Throwable backupFailure() {
            return this.failure;
        }

        @Override
        public long lastSuccessEpochMillis() {
            return -1L;
        }

        @Override
        public Throwable maintenanceFailure() {
            return this.maintenanceFailure;
        }

        @Override
        public void runChecks() {
        }

        @Override
        public boolean isRunningChecks() {
            return false;
        }

        @Override
        public Throwable failure() {
            return null;
        }
    }
}
