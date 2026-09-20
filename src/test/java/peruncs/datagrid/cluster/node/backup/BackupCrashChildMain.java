package peruncs.datagrid.cluster.node.backup;

import org.eclipse.serializer.afs.types.AFile;
import org.eclipse.serializer.afs.types.AWritableFile;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;
import org.eclipse.store.storage.types.StorageLiveFileProvider;
import peruncs.datagrid.cluster.storage.types.ReplicationCursor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/// Forked child for the backup-publication crash cells. The parent kills this
/// process only after the exact publication milestone is written to the
/// control directory; this class never self-kills.
///
/// Supported crash points:
///
/// - `MID_EXPORT`: the Store export is parked after writing a partial data
///   file into the export workspace, before the manifest exists;
/// - `AFTER_MANIFEST_BEFORE_READY`: the manifest is durable in the workspace
///   but the ready marker is not — the export is provably incomplete;
/// - `BEFORE_PUBLISH_RENAME`: the complete archive is compressed in the
///   workspace and the volume publication lock is held, but the atomic rename
///   has not run, so the volume must not expose any selectable archive.
final class BackupCrashChildMain {
    private static final Set<String> SUPPORTED_POINTS =
            Set.of("MID_EXPORT", "AFTER_MANIFEST_BEFORE_READY", "BEFORE_PUBLISH_RENAME");
    private static final ReplicationCursor CURSOR =
            new ReplicationCursor("backup-crash", null, 7L, "010203");

    private BackupCrashChildMain() {
    }

    static void main(final String[] arguments) throws Exception {
        final String baseProperty = System.getProperty("dg.crash.base");
        if (baseProperty == null || baseProperty.isBlank()) {
            throw new IllegalArgumentException("missing -Ddg.crash.base");
        }
        final Path base = Path.of(baseProperty).toAbsolutePath().normalize();
        final Path control = base.resolve("control");
        Files.createDirectories(control);
        final String point = System.getProperty("dg.crash.point", "");
        if (!SUPPORTED_POINTS.contains(point)) {
            throw new IllegalArgumentException("unknown backup crash point: %s".formatted(point));
        }
        final Path volume = base.resolve("backup-volume");
        final FilesystemVolumeBackupBackend backend = FilesystemVolumeBackupBackend.New(volume);
        mark(control.resolve("ready"), "ready");
        if ("MID_EXPORT".equals(point)) {
            backend.createBackup(new ParkingExportConnection(control, point), CURSOR,
                    BackupMetadata.New(11L, false, CURSOR));
            return;
        }
        /* The production seam is ScopedValue-scoped: an instance created
         * before the hook is bound still observes it, because the lookup
         * happens inside createBackup at the crash point itself. */
        FilesystemVolumeBackupBackend.runWithTestHook((name, path) -> {
            if (point.equals(name)) {
                mark(control.resolve("milestone.reached"), name);
                awaitParent(control.resolve("release"));
            }
        }, () -> backend.createBackup(new TestStorageConnection(), CURSOR,
                BackupMetadata.New(11L, false, CURSOR)));
        mark(control.resolve("outcome"), "PUBLISHED");
    }

    private static void mark(final Path path, final String value) {
        try {
            Files.createDirectories(path.toAbsolutePath().getParent());
            final Path temporary = Files.createTempFile(path.getParent(), "mark-", ".tmp");
            Files.move(
                    Files.writeString(temporary, "%s%n".formatted(value), StandardCharsets.UTF_8,
                            StandardOpenOption.WRITE),
                    path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel directory = FileChannel.open(path.getParent(), StandardOpenOption.READ)) {
                directory.force(true);
            }
        } catch (final IOException failure) {
            throw new UncheckedIOException("cannot write backup crash control file " + path, failure);
        }
    }

    private static void awaitParent(final Path release) {
        try {
            while (!Files.exists(release)) {
                TimeUnit.MILLISECONDS.sleep(10L);
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("backup crash child interrupted", interrupted);
        }
    }

    /// A Store connection that writes one partial data file into the export
    /// workspace and then parks forever, standing in for a node killed in the
    /// middle of a real export.
    private static final class ParkingExportConnection extends TestStorageConnection {
        private final Path control;
        private final String point;

        private ParkingExportConnection(final Path control, final String point) {
            this.control = control;
            this.point = point;
        }

        @Override
        public void issueFullBackup(
                final StorageLiveFileProvider targetFileProvider,
                final PersistenceTypeDictionaryExporter typeDictionaryExporter
        ) {
            final AFile partial = targetFileProvider.provideDataFile(0, 0);
            final AWritableFile writable = partial.useWriting();
            try {
                writable.ensureExists();
                writable.writeBytes(java.nio.ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
            } catch (final RuntimeException failure) {
                throw new IllegalStateException("cannot stage a partial export", failure);
            } finally {
                writable.close();
            }
            mark(this.control.resolve("milestone.reached"), this.point);
            awaitParent(this.control.resolve("release"));
        }
    }
}
