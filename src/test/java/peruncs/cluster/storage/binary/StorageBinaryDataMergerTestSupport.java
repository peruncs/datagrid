package peruncs.cluster.storage.binary;

import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.cluster.storage.StorageGraphCoordinator;

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

    /// Builds a merger configuration with the documented non-timing defaults
    /// and an unwired coordinator.
    ///
    /// @param foundation               persistence foundation
    /// @param storage                  Store connection
    /// @param objectGraphUpdateHandler graph update handler
    /// @param cachingTimeoutMs         coalescing delay
    /// @param cachedBytesLimit         soft backpressure threshold in bytes
    /// @param applyTimeoutMs           materialization wait budget
    /// @return configuration with default hard cap, disposal, and validation bounds
    static StorageBinaryDataMerger.Configuration configuration(
            final BinaryPersistenceFoundation<?> foundation,
            final StorageConnection storage,
            final ObjectGraphUpdateHandler objectGraphUpdateHandler,
            final long cachingTimeoutMs,
            final long cachedBytesLimit,
            final long applyTimeoutMs) {
        return configuration(foundation, storage, objectGraphUpdateHandler,
                cachingTimeoutMs, cachedBytesLimit, applyTimeoutMs, null);
    }

    /// Builds a merger configuration with the documented non-timing defaults.
    ///
    /// @param foundation               persistence foundation
    /// @param storage                  Store connection
    /// @param objectGraphUpdateHandler graph update handler
    /// @param cachingTimeoutMs         coalescing delay
    /// @param cachedBytesLimit         soft backpressure threshold in bytes
    /// @param applyTimeoutMs           materialization wait budget
    /// @param graphCoordinator         optional per-Store graph coordinator
    /// @return configuration with default hard cap, disposal, and validation bounds
    static StorageBinaryDataMerger.Configuration configuration(
            final BinaryPersistenceFoundation<?> foundation,
            final StorageConnection storage,
            final ObjectGraphUpdateHandler objectGraphUpdateHandler,
            final long cachingTimeoutMs,
            final long cachedBytesLimit,
            final long applyTimeoutMs,
            final StorageGraphCoordinator graphCoordinator) {
        return new StorageBinaryDataMerger.Configuration(
                foundation,
                storage,
                objectGraphUpdateHandler,
                cachingTimeoutMs,
                cachedBytesLimit,
                StorageBinaryDataMerger.MAX_CACHED_BYTES,
                applyTimeoutMs,
                StorageBinaryDataMerger.DISPOSE_ORDERLY_TIMEOUT_MS,
                StorageBinaryDataMerger.DISPOSE_INTERRUPT_TIMEOUT_MS,
                StorageBinaryDataMerger.MAX_VALIDATED_INDEX_OBJECTS,
                graphCoordinator);
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
