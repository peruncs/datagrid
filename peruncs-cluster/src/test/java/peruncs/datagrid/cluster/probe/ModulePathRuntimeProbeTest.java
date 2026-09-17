package peruncs.datagrid.cluster.probe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/// Forked named-module runtime gate for the JPMS descriptor.
///
/// Every surefire suite runs on the class path (see the surefire comment in
/// the module pom), so the reflective index validator
/// (`ClusterStoreIndexes`) is never exercised under JPMS access rules by the
/// normal suites. The descriptor consistency test only parses
/// `module-info.class` statically. This test closes that gap in the normal
/// gate: it classifies the test class path into module-path entries (jars and
/// exploded directories that ship a `module-info.class`) and class-path
/// entries, forks a JVM that resolves `peruncs.datagrid.cluster` as a real
/// named module from the module path, and runs
/// [ModulePathProbeMain] there — which registers the embedded Lucene and
/// JVector index kinds and validates them through the reflective validator.
///
/// The probe class itself stays on the class path as the unnamed module, so
/// the run also proves the exported storage contracts are consumable from an
/// unnamed module while the implementation resolves through the module path.
@Timeout(120)
class ModulePathRuntimeProbeTest {
    @Test
    void indexValidatorRunsInsideTheNamedModule(@TempDir final Path root) throws Exception {
        final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final List<String> entries = List.of(
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"))
                        .split(File.pathSeparator));
        /* Every jar goes on the module path: libraries ship either a full
         * descriptor, an `Automatic-Module-Name` header (Agrona), or resolve
         * as a filename-derived automatic module (jvector), which the
         * upstream gigamap descriptor requires by name. Exploded class
         * directories join the module path when they carry a
         * `module-info.class` (the reactor's own modules); the remaining
         * directories stay on the class path, so the probe class runs as the
         * unnamed module while its dependencies resolve as named modules. */
        final List<String> modulePath = new ArrayList<>();
        final List<String> classPath = new ArrayList<>();
        for (final String entry : entries) {
            if (entry.isBlank()) continue;
            final boolean modular = entry.endsWith(".jar")
                    || Files.isRegularFile(Path.of(entry, "module-info.class"));
            (modular ? modulePath : classPath).add(entry);
        }
        assertFalse(modulePath.isEmpty(), "the test class path must contain jar entries");
        assertTrue(classPath.stream().anyMatch(entry -> entry.endsWith("test-classes")),
                "the probe must run from the test-classes directory");

        /* Output goes to a file, not a pipe: a pipe nobody drains while
         * `waitFor` blocks can fill up, block the child, and turn a healthy
         * run into a timeout. */
        final Path output = root.resolve("module-path-probe.log");
        final Process child = new ProcessBuilder(
                java,
                "--enable-preview",
                "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--module-path", String.join(File.pathSeparator, modulePath),
                "--add-modules", "peruncs.datagrid.cluster",
                "-cp", String.join(File.pathSeparator, classPath),
                ModulePathProbeMain.class.getName())
                .redirectErrorStream(true)
                .redirectOutput(output.toFile())
                .start();
        if (!child.waitFor(90, TimeUnit.SECONDS)) {
            child.destroyForcibly();
            fail("module-path probe timed out");
        }
        final String outputText = Files.readString(output);
        assertEquals(0, child.exitValue(), "module-path probe failed: %s".formatted(outputText));
        assertTrue(outputText.contains("MODULE-PATH-PROBE-OK"),
                "probe did not report success: %s".formatted(outputText));
    }
}
