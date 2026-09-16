package peruncs.datagrid.cluster.node.aeron.crashtest;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;
import peruncs.datagrid.cluster.test.ChildJava;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Verifies that losing the external Archive forces a writer reseed.
class ExternalArchiveCrashIT {
    private static Process launchArchive(final Path base, final int controlPort) throws IOException {
        return launchArchive(base, controlPort, false);
    }

    private static Process launchArchive(final Path base, final int controlPort, final boolean reseed)
            throws IOException {
        Files.createDirectories(base.resolve("control"));
        Files.deleteIfExists(base.resolve("control/archive-ready"));
        Files.deleteIfExists(base.resolve("control/archive-outcome"));
        Files.deleteIfExists(base.resolve("control/archive-stop"));
        final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(java, "--enable-preview", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", ChildJava.classpath(),
                "-Ddg.archive.base=%s".formatted(base),
                "-Ddg.archive.reseed=%s".formatted(reseed),
                "-Ddg.archive.controlChannel=aeron:udp?endpoint=localhost:%s".formatted(controlPort),
                "-Ddg.archive.replayChannel=aeron:udp?endpoint=localhost:0",
                ArchiveProcessMain.class.getName())
                .redirectOutput(base.resolve("control/archive.log").toFile())
                .redirectErrorStream(true).start();
    }

    private static Process launchWriter(final Path base, final String mode, final String point,
                                        final int livePort, final int controlPort) throws IOException {
        final Path control = base.resolve("control");
        Files.deleteIfExists(control.resolve("ready"));
        Files.deleteIfExists(control.resolve("ready-phase2"));
        Files.deleteIfExists(control.resolve("milestone.reached"));
        Files.deleteIfExists(control.resolve("outcome"));
        final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(java, "--enable-preview", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", ChildJava.classpath(),
                "-Ddg.crash.base=%s".formatted(base),
                "-Ddg.crash.mode=%s".formatted(mode),
                "-Ddg.crash.barrier=%s".formatted(point),
                "-Ddg.crash.externalArchive=true",
                "-Ddg.crash.durability=%s".formatted(ReplicationDurabilityMode.ARCHIVE_FIRST),
                "-Ddg.crash.livePort=%s".formatted(livePort),
                "-Ddg.crash.controlPort=%s".formatted(controlPort),
                ProviderCrashChildMain.class.getName())
                .redirectOutput(base.resolve("control/phase1.log").toFile())
                .redirectErrorStream(true).start();
    }

    private static void await(final Path path, final Process process, final long timeout)
            throws IOException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        while (!Files.exists(path) && System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                final boolean archiveControlFile = path.getFileName().toString().startsWith("archive-");
                final Path log = path.getParent().resolve(archiveControlFile ? "archive.log" : "phase1.log");
                throw new AssertionError("process exited before %s: %s".formatted(path, (Files.exists(log) ? Files.readString(log) : "no process log")));
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
        }
        assertTrue(Files.exists(path), "timed out waiting for %s".formatted(path));
    }

        /// Verifies archive loss during commit wait requires reseed.
    @Test
    void archiveLossDuringCommitWaitRequiresReseed() throws Exception {
        try (DirectoryLayout layout = DirectoryLayout.create()) {
            final Path base = layout.root();
            Process archive = null;
            Process writer = null;
            Process recoveryArchive = null;
            Process recoveryWriter = null;
            try {
                archive = launchArchive(base, layout.controlPort());
                await(base.resolve("control/archive-ready"), archive, 30_000L);
                writer = launchWriter(base, "phase1", "AFTER_COMMIT_OFFER",
                        layout.livePort(), layout.controlPort());
                await(base.resolve("control/ready"), writer, 30_000L);
                final Path milestone = base.resolve("control/milestone.reached");
                await(milestone, writer, 60_000L);
                archive.destroyForcibly();
                assertTrue(archive.waitFor(10, TimeUnit.SECONDS), "Archive process did not exit");
                /* The external Archive is SIGKILLed, so no client can query its
                 * stop position after death. Waiting for process exit is the only
                 * observable stop boundary; the recovery child still retries the
                 * catalog-open race and reports the exact failure. */
                Files.writeString(base.resolve("control/release"), "release");
                assertTrue(writer.waitFor(30, TimeUnit.SECONDS), "writer did not fail after Archive loss");
                recoveryArchive = launchArchive(base, layout.controlPort(), true);
                await(base.resolve("control/archive-ready"), recoveryArchive, 30_000L);
                recoveryWriter = launchWriter(base, "phase2", "NONE",
                        layout.livePort(), layout.controlPort());
                await(base.resolve("control/outcome"), recoveryWriter, 30_000L);
                assertTrue(recoveryWriter.waitFor(30, TimeUnit.SECONDS), "recovery writer did not exit");
                final String outcome = Files.readString(base.resolve("control/outcome"));
                final CrashOutcome parsed = CrashOutcome.parse(outcome);
                assertEquals(RecoveryPolicy.RESEED_REQUIRED, parsed.policy(), outcome);
                assertEquals("RESEED_REQUIRED", parsed.health(), outcome);
                assertTrue(parsed.error() != null && parsed.error().startsWith("RESEED_REQUIRED:"), outcome);
            } finally {
                for (final Process process : new Process[]{writer, recoveryWriter, archive, recoveryArchive}) {
                    if (process != null && process.isAlive()) process.destroyForcibly();
                }
            }
        }
    }

        /// Verifies stale archive catalog is never extended silently.
    @Test
    void staleArchiveCatalogIsNeverExtendedSilently() throws Exception {
        try (DirectoryLayout layout = DirectoryLayout.create()) {
            final Path base = layout.root();
            Process archive = null;
            Process writer = null;
            Process recoveredArchive = null;
            Process recoveredWriter = null;
            try {
                archive = launchArchive(base, layout.controlPort());
                await(base.resolve("control/archive-ready"), archive, 30_000L);
                writer = launchWriter(base, "phase1", "AFTER_COMMIT_OFFER",
                        layout.livePort(), layout.controlPort());
                await(base.resolve("control/ready"), writer, 30_000L);
                await(base.resolve("control/milestone.reached"), writer, 60_000L);
                archive.destroyForcibly();
                assertTrue(archive.waitFor(10, TimeUnit.SECONDS), "Archive process did not exit");
                Files.writeString(base.resolve("control/release"), "release");
                assertTrue(writer.waitFor(30, TimeUnit.SECONDS), "writer did not fail after Archive loss");

                recoveredArchive = launchArchive(base, layout.controlPort(), false);
                try {
                    await(base.resolve("control/archive-ready"), recoveredArchive, 30_000L);
                } catch (final AssertionError startupFailure) {
                    final String archiveOutcome = Files.exists(base.resolve("control/archive-outcome"))
                            ? Files.readString(base.resolve("control/archive-outcome")) : "";
                    assertTrue(archiveOutcome.startsWith("OUTCOME=RESEED_REQUIRED\n"),
                            "%s\n%s".formatted(archiveOutcome, startupFailure.getMessage()));
                    return;
                }
                recoveredWriter = launchWriter(base, "phase2", "NONE",
                        layout.livePort(), layout.controlPort());
                await(base.resolve("control/outcome"), recoveredWriter, 30_000L);
                assertTrue(recoveredWriter.waitFor(30, TimeUnit.SECONDS), "recovery writer did not exit");
                final String outcome = Files.readString(base.resolve("control/outcome"));
                final CrashOutcome parsed = CrashOutcome.parse(outcome);
                assertEquals(RecoveryPolicy.RESEED_REQUIRED, parsed.policy(), outcome);
                assertEquals("RESEED_REQUIRED", parsed.health(), outcome);
                assertTrue(parsed.error() != null && parsed.error().startsWith("RESEED_REQUIRED:"), outcome);
            } finally {
                for (final Process process : new Process[]{writer, recoveredWriter, archive, recoveredArchive}) {
                    if (process != null && process.isAlive()) process.destroyForcibly();
                }
            }
        }
    }
}
