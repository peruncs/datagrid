package peruncs.datagrid.cluster.storage.types;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.errors.ReaderWriteRejectedException;

import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Pins the read-only target's write-controller behavior: it must stay
/// writable so every application write reaches the loud rejection instead of
/// being skipped silently, and Store's default validators must accept it.
class RejectingPersistenceTargetTest {
    /// Verifies the target reports writable and store-enabled so Store routes
    /// writes into the rejection, while the default validators stay no-ops.
    @Test
    @SuppressWarnings("unchecked") // dynamic proxy stand-in for the delegate target
    void writablePredicatesRouteWritesIntoTheRejection() {
        final PersistenceTarget<Binary> delegate = (PersistenceTarget<Binary>) Proxy.newProxyInstance(
                RejectingPersistenceTargetTest.class.getClassLoader(),
                new Class<?>[]{PersistenceTarget.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "equals" -> proxy == args[0];
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "toString" -> "delegate";
                            default -> throw new AssertionError(method.getName());
                        };
                    }
                    if (method.getName().equals("isWritable")) return true;
                    return null;
                });
        final RejectingPersistenceTarget target = RejectingPersistenceTarget.create(delegate);

        assertDoesNotThrow(target::validateIsWritable,
                "Store's default validator must accept the always-writable target");
        assertDoesNotThrow(target::validateIsStoringEnabled,
                "the default storing validator delegates to the always-true writable predicate");
        assertThrows(ReaderWriteRejectedException.class, () -> target.write(null));
    }
}
