package peruncs.cluster.node.aeron;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.test.ChildJava;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Exercises writer tail recovery against a real Archive recording and Store process.
class AeronWriterRecoveryIT {
    private static final long CHILD_TIMEOUT_SECONDS = 90L;

    @Test
    @Timeout(value = 8, unit = TimeUnit.MINUTES)
    void recordedPrepareRecoversAsAbortAndLocalWriteRecoversAsCommit(@TempDir final Path temporaryDirectory)
            throws Exception {
        for (final CrashCase crashCase : CrashCase.values()) runCase(temporaryDirectory.resolve(crashCase.mode()), crashCase);
    }

    private static void runCase(final Path root, final CrashCase crashCase) throws Exception {
        Files.createDirectories(root);
        final UUID clusterId = UUID.randomUUID();
        final UUID nodeId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final Path control = Files.createDirectories(root.resolve("control"));
        final Path log = root.resolve("child.log");

        runChild(root, clusterId, nodeId, generation, "initial", log);
        Files.deleteIfExists(control.resolve("crash-hook"));
        final Process crashing = startChild(root, clusterId, nodeId, generation, crashCase.mode(), log);
        try {
            final Path hook = control.resolve("crash-hook");
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(CHILD_TIMEOUT_SECONDS);
            while (crashing.isAlive() && !Files.exists(hook) && System.nanoTime() < deadline) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            }
            assertTrue(Files.exists(hook), "crash hook not reached; child output: " + output(log));
            final String[] marker = Files.readString(hook).split(";");
            final long beforeSequence = Long.parseLong(marker[0]);
            assertEquals(beforeSequence + 1L, Long.parseLong(marker[1]), "crash sequence must follow the mark");
            assertEquals(crashCase.point(), marker[2]);
        } finally {
            crashing.destroyForcibly();
            assertTrue(crashing.waitFor(10, TimeUnit.SECONDS), "crash child did not exit after forcible stop");
        }

        runChild(root, clusterId, nodeId, generation, "recover", log);
        final String result = Files.readString(control.resolve("recovery-outcome"));
        final long beforeSequence = Long.parseLong(Files.readString(control.resolve("crash-hook")).split(";")[0]);
        /* Recovery resolves S+1; writer startup then commits its fencing mark at S+2,
         * and this follow-up write proves admission resumed at S+3. */
        assertEquals("terminal=%s;store=%s;next=%s".formatted(crashCase.terminal(),
                        crashCase.storeValuePresent(), beforeSequence + 3L), result);
    }

    private static void runChild(
            final Path root, final UUID clusterId, final UUID nodeId, final UUID generation,
            final String mode, final Path log) throws Exception {
        final Process child = startChild(root, clusterId, nodeId, generation, mode, log);
        if (!child.waitFor(CHILD_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            child.destroyForcibly();
            child.waitFor(10, TimeUnit.SECONDS);
            throw new AssertionError("Store child timed out in %s: %s".formatted(mode, output(log)));
        }
        assertEquals(0, child.exitValue(), "%s child failed: %s".formatted(mode, output(log)));
    }

    private static Process startChild(
            final Path root, final UUID clusterId, final UUID nodeId, final UUID generation,
            final String mode, final Path log) throws IOException {
        final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(java,
                "--enable-preview", "--add-modules", "jdk.incubator.vector",
                "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", ChildJava.classpath(),
                "-Ddg.aeron.store.root=%s".formatted(root),
                "-Ddg.aeron.store.cluster=%s".formatted(clusterId),
                "-Ddg.aeron.store.node=%s".formatted(nodeId),
                "-Ddg.aeron.store.generation=%s".formatted(generation),
                AeronStoreProcessChildMain.class.getName(), mode)
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
    }

    private static String output(final Path log) {
        try {
            return Files.readString(log);
        } catch (final IOException failure) {
            return "cannot read child log: " + failure;
        }
    }

    private enum CrashCase {
        BEFORE_LOCAL_WRITE("crash-before-local-write", "AFTER_PREPARE_BEFORE_LOCAL_WRITE", "ABORT", false),
        AFTER_LOCAL_WRITE("crash-after-local-write", "AFTER_LOCAL_WRITE_BEFORE_COMMIT", "COMMIT", true);

        private final String mode;
        private final String point;
        private final String terminal;
        private final boolean storeValuePresent;

        CrashCase(final String mode, final String point, final String terminal, final boolean storeValuePresent) {
            this.mode = mode;
            this.point = point;
            this.terminal = terminal;
            this.storeValuePresent = storeValuePresent;
        }

        private String mode() {
            return mode;
        }

        private String point() {
            return point;
        }

        private String terminal() {
            return terminal;
        }

        private boolean storeValuePresent() {
            return storeValuePresent;
        }
    }
}
