package peruncs.cluster.storage.aeron.crashtest;


import org.junit.jupiter.api.Test;
import peruncs.cluster.test.ChildJava;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Regression test for Aeron's default DriverTimeoutException exit policy.
class AeronDriverFailureIT {
    private static void deleteTree(final Path root) throws IOException {
        try (var paths = Files.walk(root)) {
            for (final Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        }
    }

    /// Verifies a dead Aeron driver is reported as a DriverTimeoutException without terminating the parent JVM.
    @Test
    void deadDriverIsReportedWithoutTerminatingTheParentJvm() throws Exception {
        final Path root = Files.createTempDirectory("dg-aeron-driver-failure-");
        try {
            final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            /* Agrona uses jdk.internal.misc.Unsafe on this JDK; this is module access,
             * not reflective test plumbing. */
            final Process child = new ProcessBuilder(java, "--enable-preview", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                    "-cp", ChildJava.classpath(),
                    "-Ddg.driver.failure.root=%s".formatted(root), AeronDriverFailureChildMain.class.getName())
                    .redirectErrorStream(true).start();
            assertTrue(child.waitFor(15, TimeUnit.SECONDS), "driver failure child did not exit");
            final String output = new String(child.getInputStream().readAllBytes());
            assertEquals(0, child.exitValue(), output);
            assertEquals("connected", Files.readString(root.resolve("control/connected")));
            final String outcome = Files.readString(root.resolve("control/outcome"));
            assertTrue(outcome.startsWith("FAILURE=io.aeron.exceptions.DriverTimeoutException"), outcome);
        } finally {
            deleteTree(root);
        }
    }
}
