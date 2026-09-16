package peruncs.datagrid.cluster.node.backup;

import org.eclipse.store.storage.types.StorageController;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.replication.ReplicationCursor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;

import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        return BackupNodeManager.New(tasks, client, controller, () -> 1L, "test");
    }

    @Test
    void healthyWhileTheReaderIsRunning() {
        final FakeClient client = new FakeClient();
        client.running = true;

        final BackupNodeManager manager = manager(client, new FakeTasks());

        assertTrue(manager.isHealthy());
        assertTrue(manager.isReady());
    }

    @Test
    void unhealthyWhenTheReaderHasStopped() {
        final FakeClient client = new FakeClient();

        final BackupNodeManager manager = manager(client, new FakeTasks());

        assertFalse(manager.isHealthy(), "a stopped reader must not report healthy");
        assertFalse(manager.isReady());
    }

    @Test
    void unhealthyAfterAReaderFailure() {
        final FakeClient client = new FakeClient();
        client.running = true;
        client.failure = new IllegalStateException("reader failed");

        final BackupNodeManager manager = manager(client, new FakeTasks());

        assertFalse(manager.isHealthy());
    }

    @Test
    void healthyDuringAnIntentionalBackupStop() {
        final FakeClient client = new FakeClient();
        final FakeTasks tasks = new FakeTasks();
        tasks.backupRunning = true;

        final BackupNodeManager manager = manager(client, tasks);

        assertTrue(manager.isHealthy(), "a backup holding the single-flight lock stops the reader on purpose");
        assertFalse(manager.isReady(), "readiness still requires the reader to run");
    }

    private static final class FakeClient implements StorageBinaryDataClient {
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

        @Override
        public BackupStartResult runBackup(final boolean useManualSlot) {
            return BackupStartResult.STARTED;
        }

        @Override
        public boolean isRunningBackup() {
            return this.backupRunning;
        }

        @Override
        public Throwable backupFailure() {
            return null;
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
