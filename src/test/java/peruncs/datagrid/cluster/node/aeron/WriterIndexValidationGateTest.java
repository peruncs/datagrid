package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceManager;
import org.eclipse.serializer.persistence.types.PersistenceRootReferencing;
import org.eclipse.serializer.persistence.types.PersistenceRootsView;
import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Verifies the per-write graph validation gate skips unchanged root sets.
class WriterIndexValidationGateTest {
        /// Verifies repeated writes with an unchanged root set validate once, and a
        /// root addition or replacement triggers exactly one re-validation.
    @Test
    void unchangedRootSetSkipsRescanAndRootChangeForcesIt() {
        final Map<String, Object> roots = new LinkedHashMap<>();
        final StorageConnection connection = connectionWith(roots);
        final WriterIndexValidationGate gate = new WriterIndexValidationGate();

        gate.validate(connection);
        gate.validate(connection);
        gate.validate(connection);
        assertEquals(1, gate.validations(), "an unchanged root set must not be rescanned");

        roots.put("root", new Object());
        gate.validate(connection);
        assertEquals(2, gate.validations(), "a new root must force a re-validation");
        gate.validate(connection);
        assertEquals(2, gate.validations(), "the unchanged set after the change must not be rescanned");

        roots.put("root", new Object());
        gate.validate(connection);
        assertEquals(3, gate.validations(), "a replaced root object must force a re-validation");
    }

    private static StorageConnection connectionWith(final Map<String, Object> roots) {
        final PersistenceRootsView view = new PersistenceRootsView() {
            @Override
            public PersistenceRootReferencing rootReference() {
                return null;
            }

            @Override
            public <C extends BiConsumer<String, Object>> C iterateEntries(final C iterator) {
                roots.forEach(iterator);
                return iterator;
            }
        };
        final PersistenceManager<Binary> manager = (PersistenceManager<Binary>) Proxy.newProxyInstance(
                WriterIndexValidationGateTest.class.getClassLoader(),
                new Class<?>[]{PersistenceManager.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "viewRoots" -> view;
                    case "toString" -> "stub-persistence-manager";
                    default -> defaultValue(method.getReturnType());
                });
        return (StorageConnection) Proxy.newProxyInstance(
                WriterIndexValidationGateTest.class.getClassLoader(),
                new Class<?>[]{StorageConnection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "persistenceManager" -> manager;
                    case "toString" -> "stub-storage-connection";
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static Object defaultValue(final Class<?> type) {
        if (!type.isPrimitive()) return null;
        if (type == boolean.class) return false;
        if (type == byte.class) return (byte) 0;
        if (type == short.class) return (short) 0;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == float.class) return 0f;
        if (type == double.class) return 0d;
        return (char) 0;
    }
}
