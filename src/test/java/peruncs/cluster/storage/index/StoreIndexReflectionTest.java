package peruncs.cluster.storage.index;

import org.eclipse.store.gigamap.jvector.VectorIndex;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Keeps the temporary upstream layout workaround fail-closed.
class StoreIndexReflectionTest {
    @Test
    void unsupportedVectorIndexLayoutFailsClosed() {
        final VectorIndex<?> index = (VectorIndex<?>) Proxy.newProxyInstance(
                VectorIndex.class.getClassLoader(), new Class<?>[]{VectorIndex.class}, (proxy, method, args) -> null);

        assertThrows(IllegalStateException.class, () -> StoreIndexReflection.invalidateVectorGraph(index));
    }

    @Test
    void productionReflectionStaysInsideTheVectorWorkaround() throws IOException {
        final Path sourceRoot = Path.of("src/main/java");
        final Set<Path> reflectiveSources = new HashSet<>();
        try (var paths = Files.walk(sourceRoot)) {
            for (final Path path : paths.filter(file -> file.toString().endsWith(".java")).toList()) {
                final String source = Files.readString(path);
                if (source.contains("java.lang.reflect") || source.contains(".getDeclared") ||
                    source.contains(".getField(") || source.contains(".getFields(") ||
                    source.contains(".getMethod(") || source.contains(".getMethods(") ||
                    source.contains(".getConstructor(") || source.contains(".getConstructors(") ||
                    source.contains(".getRecordComponents(") || source.contains(".getAnnotations(") ||
                    source.contains(".getAnnotation(") ||
                    source.contains(".setAccessible(") || source.contains(".trySetAccessible(") ||
                    source.contains("Class.forName(") || source.contains("privateLookupIn(")) {
                    reflectiveSources.add(sourceRoot.relativize(path));
                }
            }
        }
        assertEquals(Set.of(Path.of("peruncs", "cluster", "storage", "index", "StoreIndexReflection.java")),
                reflectiveSources);
    }
}
