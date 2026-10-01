package peruncs.cluster.storage.index;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Keeps production code free of Java reflection and of private-state access to the Store.
class NoReflectionTest {
    @Test
    void productionCodeUsesNoReflection() throws IOException {
        final List<String> markers = List.of("java.lang.reflect", ".getDeclared", ".getField(", ".getFields(",
                ".getMethod(", ".getMethods(", ".getConstructor(", ".getConstructors(", ".getRecordComponents(",
                ".getAnnotations(", ".getAnnotation(", ".setAccessible(", ".trySetAccessible(", "Class.forName(",
                "privateLookupIn(", "unreflect");
        try (var paths = Files.walk(Path.of("src/main/java"))) {
            final List<Path> reflective = paths.filter(file -> file.toString().endsWith(".java"))
                    .filter(file -> {
                        try {
                            final String source = Files.readString(file);
                            return markers.stream().anyMatch(source::contains);
                        } catch (final IOException failure) {
                            throw new UncheckedIOException(failure);
                        }
                    }).toList();
            assertEquals(List.of(), reflective);
        }
    }
}
