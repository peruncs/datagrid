package peruncs.cluster;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
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

    /// The storage layer is below the node layer: the node assembles storage services, never the reverse.
    @Test
    void storageDependsOnNoNodePackage() throws IOException {
        assertNoImports(MAIN.resolve("storage"), List.of("peruncs.cluster.node"));
    }

    /// The configuration and exported API types sit under every service package.
    @Test
    void errorsAndStorageDoNotDependOnTheApiFacade() throws IOException {
        assertNoImports(MAIN.resolve("errors"), List.of("peruncs.cluster.api"));
        assertNoImports(MAIN.resolve("storage"), List.of("peruncs.cluster.api.ClusterStorage"));
    }

    /// Production code imports what it uses instead of spelling out fully qualified JDK names.
    @Test
    void productionCodeUsesImportsNotFullyQualifiedJdkNames() throws IOException {
        final Pattern fullyQualified = Pattern.compile("[^.\\w\"]java\\.(?:util|io|nio|time)\\.[a-z.]*[A-Z]\\w+");
        final List<String> violations;
        try (Stream<Path> files = Files.walk(MAIN)) {
            violations = files.filter(file -> file.toString().endsWith(".java")).flatMap(file -> {
                try {
                    return Files.readAllLines(file).stream()
                            .filter(line -> !line.stripLeading().matches("^(import|///|//|\\*|/\\*).*"))
                            .filter(line -> fullyQualified.matcher(line).find())
                            .map(line -> file + ": " + line.strip());
                } catch (final IOException failure) {
                    throw new UncheckedIOException(failure);
                }
            }).toList();
        }
        assertTrue(violations.isEmpty(), "fully qualified names: " + violations);
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
                    throw new UncheckedIOException(failure);
                }
            }).toList();
        }
        assertTrue(violations.isEmpty(), "layering violated: " + violations);
    }
}
