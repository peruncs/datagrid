package peruncs.datagrid.cluster.node.backup;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.types.ReplicationCursor;
import peruncs.datagrid.cluster.test.ChildJava;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Crash cells for the backup publication atomics.
///
/// A forked [BackupCrashChildMain] runs one backup creation through the
/// production [FilesystemVolumeBackupBackend] and is SIGKILLed at an exact
/// publication window:
///
/// - mid-export, after a partial data file reached the workspace;
/// - after the manifest write but before the ready marker;
/// - after compression, while holding the publication lock, before the
///   atomic rename.
///
/// After the kill the parent asserts through the production volume API that a
/// killed export is never selectable (no listed backup, no unreadable
/// archive), that an abandoned workspace directory left behind is ignored or
/// cleaned, and that a subsequent complete backup is the one restore
/// installs. Cells run under the `crashmatrix` failsafe profile.
final class BackupCrashMatrixIT {
    private static final ReplicationCursor CURSOR =
            new ReplicationCursor("backup-crash", null, 9L, "0304");

    /// Verifies a kill mid-export leaves no selectable backup on the volume.
    @Test
    void killedMidExportNeverBecomesSelectable() throws Exception {
        this.assertKillCell("MID_EXPORT");
    }

    /// Verifies a kill after the manifest write but before the ready marker
    /// leaves no selectable backup on the volume.
    @Test
    void killedBeforeReadyMarkerNeverBecomesSelectable() throws Exception {
        this.assertKillCell("AFTER_MANIFEST_BEFORE_READY");
    }

    /// Verifies a kill with the compressed archive staged next to the volume,
    /// publication lock held, rename not started, leaves no selectable backup.
    @Test
    void killedBeforePublishRenameNeverBecomesSelectable() throws Exception {
        this.assertKillCell("BEFORE_PUBLISH_RENAME");
    }

    /// Runs one cell: kill the exporting child at the named window, verify no
    /// backup is selectable, then publish a complete backup and prove restore
    /// installs exactly that archive.
    private void assertKillCell(final String point) throws Exception {
        final Path base = Files.createTempDirectory("backup-crash-");
        try {
            final Path volume = base.resolve("backup-volume");
            Process child = launch(base, point);
            try {
                await(base.resolve("control/ready"), child);
                await(base.resolve("control/milestone.reached"), child);
            } finally {
                child.destroyForcibly();
                child.waitFor(10, TimeUnit.SECONDS);
            }
            /* Observation through the production API: listing is the only
             * selection input for backups, so any archive a crash leaked would
             * be selectable here. */
            final FilesystemVolumeBackupBackend verifier = FilesystemVolumeBackupBackend.New(volume);
            assertTrue(verifier.listBackups().isEmpty(),
                    "a killed export must never be a selectable backup at point %s: %s".formatted(
                            point, verifier.listBackups()));
            assertTrue(verifier.listUnreadableArchives().isEmpty(),
                    "a killed export must not surface as an unreadable archive at point %s".formatted(point));

            final BackupMetadata complete = BackupMetadata.New(42L, false, CURSOR);
            verifier.createBackup(new TestStorageConnection(), CURSOR, complete);
            final List<BackupMetadata> listed = verifier.listBackups();
            assertEquals(1, listed.size(), "exactly one complete backup must be selectable");
            assertEquals(complete.backupId(), listed.getFirst().backupId(),
                    "restore selection must resolve to the complete archive, never the killed export");

            final Path restoreRoot = Files.createDirectories(base.resolve("restored"));
            verifier.restoreBackup(restoreRoot, listed.getFirst());
            assertTrue(Files.isDirectory(restoreRoot.resolve(StorageBackupBackend.STORAGE_ENTRY)),
                    "restore must install the complete archive's storage image");
        } finally {
            deleteTree(base);
        }
    }

    private static Process launch(final Path base, final String point) throws IOException {
        final Path control = base.resolve("control");
        Files.createDirectories(control);
        Files.deleteIfExists(control.resolve("ready"));
        Files.deleteIfExists(control.resolve("milestone.reached"));
        Files.deleteIfExists(control.resolve("release"));
        final String javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final ProcessBuilder builder = new ProcessBuilder(javaExecutable,
                "--enable-preview", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", ChildJava.classpath(),
                "-Ddg.crash.base=%s".formatted(base),
                "-Ddg.crash.point=%s".formatted(point),
                BackupCrashChildMain.class.getName());
        builder.redirectOutput(control.resolve("child-stdout.log").toFile());
        builder.redirectError(control.resolve("child-stderr.log").toFile());
        return builder.start();
    }

    private static void await(final Path path, final Process child) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(60_000L);
        while (!Files.exists(path) && System.nanoTime() < deadline) {
            if (!child.isAlive()) {
                throw new AssertionError("backup child exited before %s".formatted(path));
            }
            Thread.sleep(10L);
        }
        assertTrue(Files.exists(path), "timed out waiting for %s".formatted(path));
    }

    private static void deleteTree(final Path root) throws IOException {
        if (!Files.exists(root)) return;
        try (var paths = Files.walk(root)) {
            for (final Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
