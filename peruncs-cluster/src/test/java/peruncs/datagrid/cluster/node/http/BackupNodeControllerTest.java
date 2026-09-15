package peruncs.datagrid.cluster.node.http;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.backup.BackupBusyException;
import peruncs.datagrid.cluster.node.backup.BackupNodeManager;
import peruncs.datagrid.cluster.node.exceptions.HttpResponseException;
import peruncs.datagrid.cluster.node.http.StorageNodeRestRouteConfigurations.PostBackup;

import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies a busy backup maps to 409 Conflict, not a server error.
class BackupNodeControllerTest {
        /// A backup requested while one runs reports conflict.
    @Test
    void busyBackupMapsToConflict() {
        final BackupNodeManager busy = (BackupNodeManager) Proxy.newProxyInstance(
                BackupNodeManager.class.getClassLoader(),
                new Class<?>[]{BackupNodeManager.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("createStorageBackup")) {
                        throw new BackupBusyException("Storage backup is already running");
                    }
                    final Class<?> result = method.getReturnType();
                    if (result == boolean.class) return false;
                    return null;
                });
        final ClusterRestRequestController controller = ClusterRestRequestController.BackupNode(busy);

        final HttpResponseException failure = assertThrows(HttpResponseException.class,
                () -> controller.postBackup(new PostBackup.Body(Boolean.FALSE)));
        assertEquals(409, failure.statusCode());
    }
}
