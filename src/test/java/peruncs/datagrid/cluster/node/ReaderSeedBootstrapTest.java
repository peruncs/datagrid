package peruncs.datagrid.cluster.node;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cluster.errors.ReaderWriteRejectedException;
import peruncs.datagrid.cluster.errors.ReseedRequiredException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies that a reader cannot start from an empty directory.
///
/// A reader owns no authoritative Store image: without a restored backup or
/// a seeded Store directory it could only manufacture an independent root
/// that later replication deltas cannot resolve against. Startup must fail
/// fast with [ReseedRequiredException] instead of diverging, while a reader
/// seeded with the writer's Store image starts normally.
class ReaderSeedBootstrapTest {
    /// Environment-backed properties with an explicit role and directories.
    static class TestProperties extends NodeLibraryPropertiesProvider.Env {
        private final Path storagePath;
        private final Path backupPath;
        private final String role;

        TestProperties(final Path storagePath, final Path backupPath, final String role) {
            this.storagePath = storagePath;
            this.backupPath = backupPath;
            this.role = role;
        }

        @Override
        public boolean isProdMode() {
            return true;
        }

        @Override
        public String replicationRole() {
            return this.role;
        }

        @Override
        public String replicationTransport() {
            return "none";
        }

        @Override
        public String replicationProperty(final String name) {
            if (NodeLibraryPropertiesProvider.Env.EnvKeys.STORAGE_PATH.equals(name)) {
                return this.storagePath.toString();
            }
            if (NodeLibraryPropertiesProvider.Env.EnvKeys.BACKUP_PATH.equals(name)) {
                return this.backupPath.toString();
            }
            return null;
        }

        @Override
        public boolean isBackupNode() {
            return false;
        }

        @Override
        public Integer keptBackupsCount() {
            return 3;
        }

        @Override
        public Integer storageLimitCheckerIntervalMinutes() {
            return 60;
        }

        @Override
        public Integer storageLimitGB() {
            return 1;
        }
    }

    /// Verifies an empty reader fails fast with a reseed error instead of manufacturing a divergent root, leaving no fresh Store image behind.
    @Test
    void populatedWriterThenEmptyReaderFailsWithoutSeed(@TempDir final Path root) throws Exception {
        final Path writerHome = root.resolve("writer-home");
        final Path readerHome = root.resolve("reader-home");

        try (final ClusterFoundation writer = ClusterFoundation.create()
                .setNodeLibraryPropertiesProvider(
                        new TestProperties(writerHome, root.resolve("writer-backups"), NodeLibraryPropertiesProvider.WRITER_ROLE))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            final var manager = writer.startStorageManager();
            final ArrayList<String> writerRoot = new ArrayList<>();
            writerRoot.add("populated-before-reader-start");
            manager.setRoot(writerRoot);
            manager.storeRoot();
        }

        try (final ClusterFoundation reader = ClusterFoundation.create()
                .setNodeLibraryPropertiesProvider(
                        new TestProperties(readerHome, root.resolve("reader-backups"), NodeLibraryPropertiesProvider.READER_ROLE))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            final var failure = assertThrows(ReseedRequiredException.class, reader::startStorageManager);
            assertTrue(failure.getMessage().startsWith("RESEED_REQUIRED"),
                    "reader seed failure must carry the reseed marker, was: %s".formatted(failure.getMessage()));
            assertTrue(isMissingOrEmpty(readerHome.resolve("storage")),
                    "a rejected reader must leave no fresh Store image behind");
        }
    }

    /// Verifies an Aeron reader with Store files but no durable replication cursor fails closed with a reseed error naming the missing cursor.
    @Test
    void aeronReaderWithStoreFilesButNoCursorFailsClosed(@TempDir final Path root) throws Exception {
        final Path readerHome = root.resolve("lost-cursor-reader");
        final Path storage = readerHome.resolve("storage");
        Files.createDirectories(storage);
        Files.writeString(storage.resolve("orphaned-channel.dat"), "unaddressable history");

        try (final ClusterFoundation reader = ClusterFoundation.create()
                .setNodeLibraryPropertiesProvider(new AeronReaderProperties(
                        readerHome, root.resolve("lost-cursor-backups"), root.resolve("lost-cursor-aeron")))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            final var failure = assertThrows(ReseedRequiredException.class, reader::startStorageManager);
            assertTrue(failure.getMessage().startsWith("RESEED_REQUIRED"),
                    "lost cursor must fail closed, was: %s".formatted(failure.getMessage()));
            assertTrue(failure.getMessage().contains("no durable replication cursor"),
                    "lost cursor must name the missing cursor, was: %s".formatted(failure.getMessage()));
        }
    }

        /// A backup node holding leftover Store files that carry no root must
    /// fail closed when root creation would be needed: only the writer, or a
    /// backup node seeding from a user-uploaded Store, may manufacture a
    /// root. This is the loophole path past the file-existence and
    /// lost-cursor gates — the failure belongs to root creation, not to
    /// seeding.
    @Test
    void backupReaderWithRootlessStoreFilesFailsAtRootCreation(@TempDir final Path root) {
        final Path backupHome = root.resolve("rootless-backup");
        final Path storage = backupHome.resolve("storage");

        /* Build a valid Store image that holds files but no root: open the raw
         * embedded foundation (no root supplier), persist one detached entity
         * so channel files exist, and leave the root unset. */
        final var image = EmbeddedStorageFoundation.New()
                .setConfiguration(StorageConfiguration.Builder()
                        .setStorageFileProvider(Storage.FileProvider(storage))
                        .createConfiguration())
                .start();
        assertNull(image.root(), "image preparation must not create a root");
        image.store(new ArrayList<String>());
        assertNull(image.root(), "storing a detached entity must not create a root");
        image.shutdown();
        assertTrue(Files.isDirectory(storage), "the rootless image must hold Store files");

        try (final ClusterFoundation backup = ClusterFoundation.create()
                .setNodeLibraryPropertiesProvider(new TestProperties(
                        backupHome, root.resolve("rootless-backups"),
                        NodeLibraryPropertiesProvider.BACKUP_READER_ROLE))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            final var failure = assertThrows(ReseedRequiredException.class, backup::startStorageManager);
            assertTrue(failure.getMessage().contains("opened a Store without a root"),
                    "the backup reader must fail at root creation instead of manufacturing a divergent image, was: %s"
                            .formatted(failure.getMessage()));
        }
    }

    /// Verifies a reader seeded with the writer's Store image starts normally and reproduces the writer's root.
    @Test
    void seededReaderStartsFromWriterImage(@TempDir final Path root) throws Exception {
        final Path writerHome = root.resolve("writer-home");
        final Path readerHome = root.resolve("reader-home");

        try (final ClusterFoundation writer = ClusterFoundation.create()
                .setNodeLibraryPropertiesProvider(
                        new TestProperties(writerHome, root.resolve("writer-backups"), NodeLibraryPropertiesProvider.WRITER_ROLE))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            final var manager = writer.startStorageManager();
            final ArrayList<String> writerRoot = new ArrayList<>();
            writerRoot.add("seeded-value");
            manager.setRoot(writerRoot);
            manager.storeRoot();
        }

        assertTrue(Files.isDirectory(writerHome.resolve("storage")),
                "writer must have created its Store directory at %s".formatted(writerHome.resolve("storage")));

        copyDirectory(writerHome.resolve("storage"), readerHome.resolve("storage"));

        try (final ClusterFoundation reader = ClusterFoundation.create()
                .setNodeLibraryPropertiesProvider(
                        new TestProperties(readerHome, root.resolve("reader-backups"), NodeLibraryPropertiesProvider.READER_ROLE))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            @SuppressWarnings("unchecked")
            final ArrayList<String> readerRoot = reader.startStorageManager()
                    .readRoot(stored -> new ArrayList<>((ArrayList<String>) stored));
            assertTrue(readerRoot.contains("seeded-value"), "seeded reader must reproduce the writer's root");
        }
    }

    /// Verifies seeded reader and backup-reader nodes reject local writes through every mutation entry point.
    @Test
    void seededReaderAndBackupReaderRejectLocalWrites(@TempDir final Path root) throws Exception {
        final Path writerHome = root.resolve("writer-home");

        try (final ClusterFoundation writer = ClusterFoundation.create()
                .setNodeLibraryPropertiesProvider(
                        new TestProperties(writerHome, root.resolve("writer-backups"), NodeLibraryPropertiesProvider.WRITER_ROLE))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            final var manager = writer.startStorageManager();
            final ArrayList<String> writerRoot = new ArrayList<>();
            writerRoot.add("seeded-value");
            manager.setRoot(writerRoot);
            manager.storeRoot();
        }

        for (final String role : new String[]{
                NodeLibraryPropertiesProvider.READER_ROLE, NodeLibraryPropertiesProvider.BACKUP_READER_ROLE}) {
            final Path readerHome = root.resolve("seeded-" + role);
            copyDirectory(writerHome.resolve("storage"), readerHome.resolve("storage"));

            try (final ClusterFoundation reader = ClusterFoundation.create()
                    .setNodeLibraryPropertiesProvider(
                            new TestProperties(readerHome, root.resolve("backups-" + role), role))
                    .setRootSupplier(ArrayList<String>::new)
                    .build()) {
                final var manager = reader.startStorageManager();
                assertThrows(ReaderWriteRejectedException.class, () -> manager.store("divergent"),
                        "%s must reject local writes".formatted(role));
                assertThrows(ReaderWriteRejectedException.class, manager::storeRoot,
                        "%s must reject root stores".formatted(role));
                assertThrows(ReaderWriteRejectedException.class,
                        () -> manager.persistenceManager().target().write(null),
                        "%s must reject raw target writes".formatted(role));
            }
        }
    }

        /// Aeron-backed reader properties with explicit Aeron settings for seed-gate tests.
    static final class AeronReaderProperties extends TestProperties {
        private final Path aeronHome;

        AeronReaderProperties(final Path storagePath, final Path backupPath, final Path aeronHome) {
            super(storagePath, backupPath, NodeLibraryPropertiesProvider.READER_ROLE);
            this.aeronHome = aeronHome;
        }

        @Override
        public String replicationTransport() {
            return "aeron";
        }

        @Override
        public String replicationStreamName() {
            return "seed-gate-test-stream";
        }

        @Override
        public String replicationProperty(final String name) {
            return switch (name) {
                case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID" -> "11111111-1111-1111-1111-111111111111";
                case "ECLIPSE_DATAGRID_AERON_WIRE_NONCE" -> "731947";
                case "ECLIPSE_DATAGRID_AERON_TRUSTED_NETWORK" -> "true";
                case "ECLIPSE_DATAGRID_AERON_NODE_ID" -> "22222222-2222-2222-2222-222222222222";
                case "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> "33333333-3333-3333-3333-333333333333";
                case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> this.aeronHome.resolve("driver").toString();
                case "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY" -> this.aeronHome.resolve("archive").toString();
                case "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH" ->
                        this.aeronHome.resolve("writer.checkpoint").toString();
                case "ECLIPSE_DATAGRID_AERON_AUTH_ALLOW_INSECURE" -> "true";
                /* Documentation addresses: the seed gate under test throws
                 * before any Aeron channel is opened, so these are never
                 * connected. Production mode only requires them to be
                 * non-loopback. */
                case "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL" ->
                        "aeron:udp?control=192.0.2.1:40123|control-mode=dynamic|fc=max|term-length=16m|alias=seed-gate-test";
                case "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL" ->
                        "aeron:udp?endpoint=192.0.2.1:0|control=192.0.2.1:40123|control-mode=dynamic";
                case "ECLIPSE_DATAGRID_AERON_ARCHIVE_REPLICATION_CHANNEL", "ECLIPSE_DATAGRID_AERON_CONTROL_RESPONSE_CHANNEL" ->
                        "aeron:udp?endpoint=192.0.2.1:0";
                case "ECLIPSE_DATAGRID_AERON_WATERMARK_CHANNEL" ->
                        "aeron:udp?endpoint=192.0.2.1:40125";
                case "ECLIPSE_DATAGRID_AERON_CONTROL_CHANNEL" ->
                        "aeron:udp?endpoint=192.0.2.1:40124";
                default -> super.replicationProperty(name);
            };
        }
    }

    private static boolean isMissingOrEmpty(final Path directory) throws IOException {
        if (!Files.isDirectory(directory)) {
            return true;
        }
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.findAny().isEmpty();
        }
    }

    private static void copyDirectory(final Path source, final Path target) throws IOException {
        try (final Stream<Path> paths = Files.walk(source)) {
            for (final var path : paths.sorted(Comparator.naturalOrder()).toList()) {
                final Path destination = target.resolve(source.relativize(path).toString());
                if (Files.isDirectory(path)) {
                    Files.createDirectories(destination);
                } else {
                    Files.createDirectories(destination.getParent());
                    Files.copy(path, destination);
                }
            }
        } catch (final UncheckedIOException failure) {
            throw failure.getCause();
        }
    }
}
