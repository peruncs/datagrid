package peruncs.datagrid.storage.distributed.aeron.crashtest;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Process-level crash tests. The child is killed while AtomicFileStore is
 * still in its temporary-file phase; exceptions in the same JVM cannot prove
 * that the old destination survives that boundary.
 */
class AeronCrashMatrixIT {
    private static void deleteTree(final Path root) throws IOException {
        if (!Files.exists(root)) return;
        IOException failure = null;
        try (var paths = Files.walk(root)) {
            for (final Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                try {
                    Files.deleteIfExists(path);
                } catch (final IOException deleteFailure) {
                    if (failure == null) failure = deleteFailure;
                    else failure.addSuppressed(deleteFailure);
                }
            }
        }
        if (failure != null) throw failure;
    }

    /** Verifies kill during checkpoint temp write retains previous checkpoint. */
    @Test
    void killDuringCheckpointTempWriteRetainsPreviousCheckpoint() throws Exception {
        final Path base = Files.createTempDirectory("dg-crash-matrix-");
        Process crashChild = null;
        try {
            this.launch(base, "baseline", false);
            Files.deleteIfExists(base.resolve("control/ready"));
            Files.deleteIfExists(base.resolve("control/after-temp-write"));
            crashChild = this.launch(base, "crash-write", true);
            final Path milestone = base.resolve("control/after-temp-write");
            this.await(milestone);
            crashChild.destroyForcibly();
            assertTrue(crashChild.waitFor(10, TimeUnit.SECONDS), "crash child did not exit");
            assertEquals("baseline", Files.readString(base.resolve("checkpoint.bin"), StandardCharsets.UTF_8));

            this.launch(base, "recover", false);
            assertEquals("OUTCOME=baseline",
                    Files.readString(base.resolve("control/outcome"), StandardCharsets.UTF_8));
        } finally {
            if (crashChild != null && crashChild.isAlive()) crashChild.destroyForcibly();
            deleteTree(base);
        }
    }

    private Process launch(final Path base, final String mode, final boolean wait)
            throws IOException, InterruptedException {
        final String javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final Process process = new ProcessBuilder(javaExecutable, "-cp", ChildJava.classpath(),
                "-Ddg.crash.base=" + base, "-Ddg.crash.mode=" + mode,
                AeronCrashChildMain.class.getName()).redirectErrorStream(true).start();
        if (wait) {
            this.await(base.resolve("control/ready"));
        } else {
            assertTrue(process.waitFor(10, TimeUnit.SECONDS), "child did not complete: " + mode);
            final String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(0, process.exitValue(), "child failed: " + mode + "\n" + output);
        }
        return process;
    }

    private void await(final Path path) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (!Files.exists(path) && System.nanoTime() < deadline) Thread.sleep(10L);
        assertTrue(Files.exists(path), "timed out waiting for " + path);
    }
}
