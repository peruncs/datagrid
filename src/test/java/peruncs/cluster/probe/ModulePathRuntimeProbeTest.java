package peruncs.cluster.probe;

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
/// The runtime probe resolves PerunCS and the index dependencies as named
/// modules while using the facade from the unnamed module. A second probe
/// compiles a named consumer against only `requires peruncs.cluster`, proving
/// Lucene and JVector facade types are transitively readable.
@Timeout(120)
class ModulePathRuntimeProbeTest {
    /// Verifies the exported facade works while its implementation is resolved on the module path.
    @Test
    void facadeRunsWithClusterAndIndexDependenciesOnTheModulePath(@TempDir final Path root) throws Exception {
        final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final RuntimePaths paths = runtimePaths();
        /* Every jar goes on the module path: libraries ship either a full
         * descriptor, an `Automatic-Module-Name` header (Agrona), or resolve
         * as a filename-derived automatic module (jvector), which the
         * upstream gigamap descriptor requires by name. Exploded class
         * directories join the module path when they carry a
         * `module-info.class` (the reactor's own modules); the remaining
         * directories stay on the class path, so the probe class runs as the
         * unnamed module while its dependencies resolve as named modules. */
        final List<String> modulePath = paths.modulePath();
        final List<String> classPath = paths.classPath();
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
                "--add-modules", "peruncs.cluster,jdk.incubator.vector",
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

    @Test
    void namedConsumerReadsIndexFacadeTypesTransitively(@TempDir final Path root) throws Exception {
        final RuntimePaths paths = runtimePaths();
        final Path sourceRoot = root.resolve("source");
        final Path moduleRoot = sourceRoot.resolve("api.consumer");
        final Path packageRoot = moduleRoot.resolve("consumer");
        final Path output = root.resolve("classes");
        Files.createDirectories(packageRoot);
        Files.createDirectories(output);
        Files.writeString(moduleRoot.resolve("module-info.java"),
                "module api.consumer { requires peruncs.cluster; }\n");
        Files.writeString(packageRoot.resolve("FacadeUse.java"), """
                package consumer;
                import org.apache.lucene.document.Document;
                import org.eclipse.store.gigamap.lucene.DocumentPopulator;
                import org.eclipse.store.gigamap.lucene.LuceneContext;
                import org.eclipse.store.gigamap.jvector.VectorIndexConfiguration;
                import org.eclipse.store.gigamap.jvector.VectorIndices;
                import org.eclipse.store.gigamap.jvector.VectorSimilarityFunction;
                import org.eclipse.store.gigamap.jvector.Vectorizer;
                import org.eclipse.store.gigamap.types.GigaMap;
                import peruncs.cluster.api.ClusterIndexes;
                public final class FacadeUse {
                    public static void main(String[] args) {
                        useFacade();
                        System.out.println("NAMED-CONSUMER-OK");
                    }
                    static final class Populator extends DocumentPopulator<String> {
                        public void populate(Document document, String value) { }
                    }
                    static final class StringVectorizer extends Vectorizer<String> {
                        public float[] vectorize(String value) { return new float[] { 1.0f }; }
                    }
                    static void useFacade() {
                        final Populator populator = new Populator();
                        final LuceneContext<String> context = ClusterIndexes.embeddedLuceneContext(populator);
                        final GigaMap<String> map = GigaMap.New();
                        ClusterIndexes.registerLucene(map, populator);
                        ClusterIndexes.addVector(map.index().register(VectorIndices.Category()), "consumer",
                            VectorIndexConfiguration.builder().dimension(1)
                                .similarityFunction(VectorSimilarityFunction.COSINE).build(),
                            new StringVectorizer());
                        if (context.directoryCreator() != null) throw new AssertionError();
                    }
                }
                """);

        final Path javac = Path.of(System.getProperty("java.home"), "bin", "javac");
        final Path log = root.resolve("named-consumer.log");
        final Process compiler = new ProcessBuilder(javac.toString(),
                "--enable-preview", "--source", "27",
                "--add-modules", "jdk.incubator.vector",
                "--module-path", String.join(File.pathSeparator, paths.modulePath()),
                "--module-source-path", sourceRoot.toString(), "-d", output.toString(), "-m", "api.consumer")
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        if (!compiler.waitFor(60, TimeUnit.SECONDS)) {
            compiler.destroyForcibly();
            fail("named-module consumer compilation timed out");
        }
        assertEquals(0, compiler.exitValue(), Files.readString(log));

        /* Run it too: the named consumer must also work at run time, with the index facade and its
         * transitive Lucene and JVector types resolved as named modules. */
        final Path runLog = root.resolve("named-consumer-run.log");
        final List<String> runModulePath = new ArrayList<>(paths.modulePath());
        runModulePath.add(output.toString());
        final Process runner = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--enable-preview",
                "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-modules", "jdk.incubator.vector",
                "--module-path", String.join(File.pathSeparator, runModulePath),
                "-m", "api.consumer/consumer.FacadeUse")
                .redirectErrorStream(true)
                .redirectOutput(runLog.toFile())
                .start();
        if (!runner.waitFor(60, TimeUnit.SECONDS)) {
            runner.destroyForcibly();
            fail("named-module consumer run timed out");
        }
        final String runOutput = Files.readString(runLog);
        assertEquals(0, runner.exitValue(), runOutput);
        assertTrue(runOutput.contains("NAMED-CONSUMER-OK"), runOutput);
    }

    private static RuntimePaths runtimePaths() {
        final List<String> entries = List.of(
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"))
                        .split(File.pathSeparator));
        final List<String> modulePath = new ArrayList<>();
        final List<String> classPath = new ArrayList<>();
        for (final String entry : entries) {
            if (entry.isBlank()) continue;
            final boolean modular = entry.endsWith(".jar")
                    || Files.isRegularFile(Path.of(entry, "module-info.class"));
            (modular ? modulePath : classPath).add(entry);
        }
        return new RuntimePaths(List.copyOf(modulePath), List.copyOf(classPath));
    }

    private record RuntimePaths(List<String> modulePath, List<String> classPath) {
    }
}
