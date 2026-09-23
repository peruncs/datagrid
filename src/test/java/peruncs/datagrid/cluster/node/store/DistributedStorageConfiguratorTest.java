package peruncs.datagrid.cluster.node.store;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.binary.DistributingTypeDictionaryExporter;
import peruncs.datagrid.cluster.storage.binary.ReplicationPublisher;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies that one Store object implementing both extension SPIs keeps both contracts.
class DistributedStorageConfiguratorTest {
    /// Verifies a store implementing both target and dictionary-exporter contracts keeps both after decoration.
    @Test
    void preservesTargetAndDictionaryExporterContracts() {
        final DistributedStorage.Configurator configurator = new DistributedStorage.Configurator(new NoOpDistributor(), java.util.function.UnaryOperator.identity());
        final Object decorated = configurator.apply(new BothContracts());

        assertInstanceOf(PersistenceTarget.class, decorated);
        assertInstanceOf(PersistenceTypeDictionaryExporter.class, decorated);
    }

        /// Plain subjects that implement neither SPI pass through untouched.
    @Test
    void plainObjectsPassThroughUnchanged() {
        final DistributedStorage.Configurator configurator = new DistributedStorage.Configurator(new NoOpDistributor(), java.util.function.UnaryOperator.identity());
        final Object subject = new Object();

        assertSame(subject, configurator.apply(subject));
    }

        /// A target-only subject is wrapped by the configured target factory.
    @Test
    void targetOnlySubjectUsesTargetFactory() {
        final List<PersistenceTarget<Binary>> seen = new ArrayList<>();
        final DistributedStorage.Configurator configurator = new DistributedStorage.Configurator(
                new NoOpDistributor(),
                delegate -> {
                    seen.add(delegate);
                    return delegate;
                });
        final PersistenceTarget<Binary> target = targetProxy(new ArrayList<>());

        assertSame(target, configurator.apply(target));
        assertEquals(List.of(target), seen);
    }

        /// An exporter-only subject gains the distributing exporter contract.
    @Test
    void exporterOnlySubjectIsDecorated() {
        final DistributedStorage.Configurator configurator = new DistributedStorage.Configurator(new NoOpDistributor(), java.util.function.UnaryOperator.identity());
        final PersistenceTypeDictionaryExporter exporter = exporterProxy();

        assertInstanceOf(DistributingTypeDictionaryExporter.class, configurator.apply(exporter));
    }

        /// The combined adapter forwards target lifecycle calls to the wrapped
    /// target, so Store startup and shutdown reach the real target.
    @Test
    void bothContractsForwardTargetLifecycle() {
        final List<String> lifecycle = new ArrayList<>();
        final PersistenceTarget<Binary> target = lifecycleTargetProxy(lifecycle);
        final DistributedStorage.Configurator configurator = new DistributedStorage.Configurator(
                new NoOpDistributor(), delegate -> target);

        final PersistenceTarget<Binary> decorated = configurator.apply(new BothContracts());
        assertNotNull(decorated);
        decorated.prepareTarget();
        decorated.closeTarget();

        assertEquals(List.of("prepareTarget", "closeTarget"), lifecycle,
                "the combined adapter must forward target lifecycle calls");
    }

        /// The combined adapter forwards writes to the factory target.
    @Test
    void bothContractsForwardWritesToFactoryTarget() {
        final List<Binary> written = new ArrayList<>();
        final DistributedStorage.Configurator configurator = new DistributedStorage.Configurator(
                new NoOpDistributor(), delegate -> targetProxy(written));

        final PersistenceTarget<Binary> decorated = configurator.apply(new BothContracts());
        assertNotNull(decorated);
        decorated.write(null);

        assertEquals(1, written.size());
    }

    @SuppressWarnings("unchecked") // dynamic proxy stand-in for the lifecycle target
    private static PersistenceTarget<Binary> lifecycleTargetProxy(final List<String> lifecycle) {
        return (PersistenceTarget<Binary>) Proxy.newProxyInstance(
                DistributedStorageConfiguratorTest.class.getClassLoader(),
                new Class<?>[]{PersistenceTarget.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) return objectMethodValue(proxy, method, args);
                    switch (method.getName()) {
                        case "prepareTarget", "closeTarget" -> lifecycle.add(method.getName());
                        case "isWritable" -> {
                            return true;
                        }
                        default -> {
                        }
                    }
                    return null;
                });
    }

    @SuppressWarnings("unchecked")
    private static PersistenceTarget<Binary> targetProxy(final List<Binary> written) {
        return (PersistenceTarget<Binary>) Proxy.newProxyInstance(
                DistributedStorageConfiguratorTest.class.getClassLoader(),
                new Class<?>[]{PersistenceTarget.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) return objectMethodValue(proxy, method, args);
                    if (method.getName().equals("write")) {
                        written.add((Binary) args[0]);
                        return null;
                    }
                    if (method.getName().equals("isWritable")) return true;
                    final Class<?> result = method.getReturnType();
                    if (result == boolean.class) return false;
                    return null;
                });
    }

    private static PersistenceTypeDictionaryExporter exporterProxy() {
        return (PersistenceTypeDictionaryExporter) Proxy.newProxyInstance(
                DistributedStorageConfiguratorTest.class.getClassLoader(),
                new Class<?>[]{PersistenceTypeDictionaryExporter.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) return objectMethodValue(proxy, method, args);
                    return null;
                });
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

    private static final class BothContracts
            implements PersistenceTarget<Binary>, PersistenceTypeDictionaryExporter {
        @Override
        public void write(final Binary data) {
        }

        @Override
        public boolean isWritable() {
            return true;
        }

        @Override
        public void exportTypeDictionary(final PersistenceTypeDictionary typeDictionary) {
        }
    }

    private static final class NoOpDistributor implements ReplicationPublisher {
        @Override
        public void distributeData(final Binary data) {
        }

        @Override
        public void distributeTypeDictionary(final String typeDictionaryData) {
        }

        @Override
        public void dispose() {
        }
    }
}
