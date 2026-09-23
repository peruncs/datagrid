package peruncs.datagrid.cluster.node.aeron;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/// Proves the production provider overrides Aeron's process-exiting timeout handler.
class AeronProviderDriverFailureTest {
    /// Verifies a dead owned driver fails provider health in a child JVM without exiting the process.
    @Test
    void deadOwnedDriverFailsHealthWithoutExitingTheJvm(@TempDir final Path root) throws Exception {
        final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        final Process child = new ProcessBuilder(java, "--enable-preview", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", classpath, "-Ddg.driver.failure.root=%s".formatted(root),
                AeronProviderDriverFailureChildMain.class.getName()).redirectErrorStream(true).start();
        try {
            assertTrue(child.waitFor(15, TimeUnit.SECONDS), "provider driver-timeout child did not exit");
            final String output = new String(child.getInputStream().readAllBytes());
            assertEquals(0, child.exitValue(), output);
            assertEquals("connected", Files.readString(root.resolve("control/connected")));
            assertEquals("FAILED", Files.readString(root.resolve("control/outcome")));
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                child.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }
}
