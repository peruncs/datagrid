package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.store.storage.types.StorageConnection;

import java.lang.reflect.Proxy;

/// Proxy stand-ins for merger tests that exercise the merger's own state
/// machine rather than Store behaviour.
final class StorageBinaryDataMergerTestSupport {
    private StorageBinaryDataMergerTestSupport() {
    }

    static BinaryPersistenceFoundation<?> foundation() {
        return (BinaryPersistenceFoundation<?>) Proxy.newProxyInstance(
                BinaryPersistenceFoundation.class.getClassLoader(),
                new Class<?>[]{BinaryPersistenceFoundation.class},
                (proxy, method, args) -> defaultValue(method.getReturnType()));
    }

    static StorageConnection connection() {
        return (StorageConnection) Proxy.newProxyInstance(
                StorageConnection.class.getClassLoader(),
                new Class<?>[]{StorageConnection.class},
                (proxy, method, args) -> defaultValue(method.getReturnType()));
    }

    static Object defaultValue(final Class<?> type) {
        if (type == boolean.class) return false;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == double.class) return 0d;
        if (type == float.class) return 0f;
        if (type == short.class) return (short) 0;
        if (type == byte.class) return (byte) 0;
        if (type == char.class) return (char) 0;
        return null;
    }
}
