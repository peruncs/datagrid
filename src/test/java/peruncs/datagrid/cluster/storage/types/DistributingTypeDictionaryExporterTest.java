package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryAssembler;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Verifies the distributing exporter updates the local delegate before
/// publishing, so receivers learn type definitions first.
class DistributingTypeDictionaryExporterTest {
        /// Local export happens before distribution, with the assembled dictionary.
    @Test
    void localExportPrecedesDistribution() {
        final List<String> order = new ArrayList<>();
        final AtomicReference<String> distributed = new AtomicReference<>();
        final PersistenceTypeDictionaryExporter delegate = (PersistenceTypeDictionaryExporter) Proxy.newProxyInstance(
                DistributingTypeDictionaryExporterTest.class.getClassLoader(),
                new Class<?>[]{PersistenceTypeDictionaryExporter.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) return objectMethodValue(proxy, method, args);
                    order.add("delegate");
                    return null;
                });
        final PersistenceTypeDictionaryAssembler assembler = (PersistenceTypeDictionaryAssembler) Proxy.newProxyInstance(
                DistributingTypeDictionaryExporterTest.class.getClassLoader(),
                new Class<?>[]{PersistenceTypeDictionaryAssembler.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) return objectMethodValue(proxy, method, args);
                    return "assembled-dictionary";
                });
        final StorageBinaryDataDistributor distributor = (StorageBinaryDataDistributor) Proxy.newProxyInstance(
                DistributingTypeDictionaryExporterTest.class.getClassLoader(),
                new Class<?>[]{StorageBinaryDataDistributor.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) return objectMethodValue(proxy, method, args);
                    if (method.getName().equals("distributeTypeDictionary")) {
                        order.add("distributor");
                        distributed.set((String) args[0]);
                    }
                    return null;
                });

        new DistributingTypeDictionaryExporter(delegate, assembler, distributor)
                .exportTypeDictionary(dictionaryProxy());

        assertEquals(List.of("delegate", "distributor"), order);
        assertEquals("assembled-dictionary", distributed.get());
    }

    private static Object objectMethodValue(
            final Object proxy, final java.lang.reflect.Method method, final Object[] args) {
        return switch (method.getName()) {
            case "equals" -> proxy == args[0];
            case "hashCode" -> System.identityHashCode(proxy);
            case "toString" -> "testDouble";
            default -> throw new AssertionError("unexpected Object method " + method.getName());
        };
    }

    private static PersistenceTypeDictionary dictionaryProxy() {
        return (PersistenceTypeDictionary) Proxy.newProxyInstance(
                DistributingTypeDictionaryExporterTest.class.getClassLoader(),
                new Class<?>[]{PersistenceTypeDictionary.class},
                (proxy, method, args) -> {
                    final Class<?> result = method.getReturnType();
                    if (result == boolean.class) return false;
                    if (result == long.class) return 0L;
                    return null;
                });
    }

}
