package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.collections.Set_long;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.types.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the batch materializer: local roots are skipped, repeated object
/// ids are collected once, and unknown types fail fast.
class ObjectMaterializerTest {
    private interface TestRoots extends PersistenceRoots {
    }

    private static final class Fixture {
        final Map<Long, Class<?>> types = new HashMap<>();
        boolean cleared;
        /* Snapshots taken inside each collect call: materialization truncates
         * its working set right after collection, so the live set cannot be
         * asserted afterwards. */
        final List<Set<Long>> collections = new ArrayList<>();
        /* Direct buffers stay reachable for the whole test so their native
         * memory cannot be reclaimed between encoding and acceptance. */
        @SuppressWarnings("MismatchedCollectionQueryUpdate") // write-only reachability so native buffers outlive the batch
        final List<ByteBuffer> pinned = new ArrayList<>();

                /// Writes one 24-byte entity header (length, type id, object id) off-heap.
        ///
        /// The header uses the native byte order, matching how the binary
        /// readers decode raw entity addresses.
        long entityAddress(final long typeId, final long objectId) {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(24).order(ByteOrder.nativeOrder());
            buffer.putLong(8, typeId);
            buffer.putLong(16, objectId);
            this.pinned.add(buffer);
            return XMemory.getDirectByteBufferAddress(buffer);
        }

        ObjectMaterializer materializer() {
            final PersistenceTypeDictionary dictionary = (PersistenceTypeDictionary) Proxy.newProxyInstance(
                    ObjectMaterializerTest.class.getClassLoader(),
                    new Class<?>[]{PersistenceTypeDictionary.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("lookupTypeById")) {
                            final Class<?> type = this.types.get((Long) args[0]);
                            return type == null ? null : definition(type);
                        }
                        return defaultValue(proxy, method, args);
                    });
            final PersistenceObjectRegistry registry = (PersistenceObjectRegistry) Proxy.newProxyInstance(
                    ObjectMaterializerTest.class.getClassLoader(),
                    new Class<?>[]{PersistenceObjectRegistry.class},
                    (proxy, method, args) -> method.getName().equals("containsClearedObject")
                            ? this.cleared
                            : defaultValue(proxy, method, args));
            final PersistenceLoader loader = (PersistenceLoader) Proxy.newProxyInstance(
                    ObjectMaterializerTest.class.getClassLoader(),
                    new Class<?>[]{PersistenceLoader.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("collect")) {
                            final Set<Long> snapshot = new HashSet<>();
                            ((Set_long) args[1]).iterate(snapshot::add);
                            this.collections.add(snapshot);
                            return args[0];
                        }
                        return defaultValue(proxy, method, args);
                    });
            final PersistenceManager<?> manager = (PersistenceManager<?>) Proxy.newProxyInstance(
                    ObjectMaterializerTest.class.getClassLoader(),
                    new Class<?>[]{PersistenceManager.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "typeDictionary" -> dictionary;
                        case "objectRegistry" -> registry;
                        case "createLoader" -> loader;
                        default -> defaultValue(proxy, method, args);
                    });
            return new ObjectMaterializer(manager);
        }

        private static PersistenceTypeDefinition definition(final Class<?> type) {
            return (PersistenceTypeDefinition) Proxy.newProxyInstance(
                    ObjectMaterializerTest.class.getClassLoader(),
                    new Class<?>[]{PersistenceTypeDefinition.class},
                    (proxy, method, args) -> method.getName().equals("type")
                            ? type
                            : defaultValue(proxy, method, args));
        }
    }

    private static Object defaultValue(final Object proxy, final java.lang.reflect.Method method, final Object[] args) {
        if (method.getDeclaringClass() == Object.class) {
            return switch (method.getName()) {
                case "equals" -> proxy == args[0];
                case "hashCode" -> System.identityHashCode(proxy);
                case "toString" -> "test-double";
                default -> throw new AssertionError("unexpected Object method " + method.getName());
            };
        }
        final Class<?> result = method.getReturnType();
        if (result == boolean.class) return false;
        if (result == long.class) return 0L;
        if (result == int.class) return 0;
        return null;
    }

        /// Entities whose type is a local root are accepted but never materialized.
    @Test
    void rootsAreSkipped() {
        final Fixture fixture = new Fixture();
        fixture.types.put(7L, TestRoots.class);
        final ObjectMaterializer materializer = fixture.materializer();
        final long address = fixture.entityAddress(7L, 42L);

        assertTrue(materializer.acceptEntityData(address, address + 24));
        materializer.materialize();

        assertEquals(List.of(Set.of()), fixture.collections);
    }

        /// Repeated object ids in one batch are materialized exactly once.
    @Test
    void repeatedObjectsAreDeduplicated() {
        final Fixture fixture = new Fixture();
        fixture.types.put(7L, String.class);
        final ObjectMaterializer materializer = fixture.materializer();

        final long first = fixture.entityAddress(7L, 42L);
        assertTrue(materializer.acceptEntityData(first, first + 24));
        final long second = fixture.entityAddress(7L, 42L);
        assertTrue(materializer.acceptEntityData(second, second + 24));
        final long other = fixture.entityAddress(7L, 43L);
        assertTrue(materializer.acceptEntityData(other, other + 24));
        materializer.materialize();

        assertEquals(List.of(Set.of(42L, 43L)), fixture.collections);
    }

        /// The working set is truncated after each batch, so a second batch
    /// starts empty even without new entities.
    @Test
    void workingSetIsTruncatedAfterEachBatch() {
        final Fixture fixture = new Fixture();
        fixture.types.put(7L, String.class);
        final ObjectMaterializer materializer = fixture.materializer();
        final long address = fixture.entityAddress(7L, 42L);
        assertTrue(materializer.acceptEntityData(address, address + 24));

        materializer.materialize();
        materializer.materialize();

        assertEquals(List.of(Set.of(42L), Set.of()), fixture.collections);
    }

        /// Already-cleared objects need no re-materialization.
    @Test
    void clearedObjectsAreSkipped() {
        final Fixture fixture = new Fixture();
        fixture.types.put(7L, String.class);
        fixture.cleared = true;
        final ObjectMaterializer materializer = fixture.materializer();
        final long address = fixture.entityAddress(7L, 42L);

        assertTrue(materializer.acceptEntityData(address, address + 24));
        materializer.materialize();

        assertEquals(List.of(Set.of()), fixture.collections);
    }

        /// A truncated entity header fails the whole batch instead of silently
    /// dropping the entities that follow it.
    @Test
    void truncatedHeaderFailsTheBatch() {
        final Fixture fixture = new Fixture();
        final ObjectMaterializer materializer = fixture.materializer();
        final long address = fixture.entityAddress(7L, 42L);

        final StorageBinaryDataException failure = assertThrows(StorageBinaryDataException.class,
                () -> materializer.acceptEntityData(address, address + 8));
        assertTrue(failure.getMessage().contains("truncated entity header"),
                "a truncated header must be named as truncation: " + failure.getMessage());
    }

        /// Entities with an unknown type id fail instead of materializing blindly.
    @Test
    void unknownTypeFails() {
        final Fixture fixture = new Fixture();
        final ObjectMaterializer materializer = fixture.materializer();
        final long address = fixture.entityAddress(99L, 42L);

        assertThrows(StorageBinaryDataException.class,
                () -> materializer.acceptEntityData(address, address + 24));
    }
}
