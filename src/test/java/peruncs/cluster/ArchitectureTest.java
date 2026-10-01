package peruncs.cluster;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/// Guards the package layering that the module descriptor documents.
class ArchitectureTest {
    private static final Path MAIN = Path.of("src/main/java/peruncs/cluster");
    private static final Pattern IMPORT = Pattern.compile("^import\\s+(?:static\\s+)?(peruncs\\.cluster\\.[\\w.]+)", Pattern.MULTILINE);

    /// The error types are the lowest layer: everything else may depend on them, never the reverse.
    @Test
    void errorsDependOnNoOtherClusterPackage() throws IOException {
        assertNoImports(MAIN.resolve("errors"), List.of("peruncs.cluster.api", "peruncs.cluster.node", "peruncs.cluster.storage"));
    }

    private static void assertNoImports(final Path directory, final List<String> forbiddenPrefixes) throws IOException {
        final List<String> violations;
        try (Stream<Path> files = Files.walk(directory)) {
            violations = files.filter(file -> file.toString().endsWith(".java")).flatMap(file -> {
                try {
                    final String source = Files.readString(file);
                    return IMPORT.matcher(source).results()
                            .map(match -> match.group(1))
                            .filter(imported -> forbiddenPrefixes.stream().anyMatch(imported::startsWith))
                            .map(imported -> file + " imports " + imported);
                } catch (final IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            }).toList();
        }
        assertTrue(violations.isEmpty(), "layering violated: " + violations);
    }
}
