package peruncs.cluster.node;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.node.aeron.TestNodeConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies Aeron readers require a marked Store image and standalone nodes can reopen a Store.
class ReaderSeedBootstrapTest {
    private static NodeConfig localConfig(final Path storagePath, final Path backupPath) {
        return NodeConfig.fromMap(Map.of(
                NodeConfig.Setting.PROD_MODE.key(), "true",
                NodeConfig.Setting.STORAGE_PATH.key(), storagePath.toString(),
                NodeConfig.Setting.BACKUP_PATH.key(), backupPath.toString(),
                NodeConfig.Setting.STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES.key(), "1",
                NodeConfig.Setting.STORAGE_LIMIT_GB.key(), "1"));
    }

    private static NodeConfig readerConfig(final Path storagePath, final Path aeronHome) {
        return TestNodeConfig.aeron(storagePath, "reader", true, Map.ofEntries(
                Map.entry(NodeConfig.Setting.AERON_CLUSTER_ID.key(), "11111111-1111-1111-1111-111111111111"),
                Map.entry(NodeConfig.Setting.AERON_NODE_ID.key(), "22222222-2222-2222-2222-222222222222"),
                Map.entry(NodeConfig.Setting.AERON_STORE_GENERATION.key(), "33333333-3333-3333-3333-333333333333"),
                Map.entry(NodeConfig.Setting.AERON_DIRECTORY.key(), aeronHome.resolve("driver").toString()),
                Map.entry(NodeConfig.Setting.AERON_ARCHIVE_DIRECTORY.key(), aeronHome.resolve("archive").toString()),
                Map.entry(NodeConfig.Setting.AERON_LIVE_CHANNEL.key(),
                        "aeron:udp?control=192.0.2.1:40123|control-mode=dynamic|fc=max|term-length=16m|alias=seed-gate-test"),
                Map.entry(NodeConfig.Setting.AERON_REPLAY_CHANNEL.key(),
                        "aeron:udp?endpoint=192.0.2.1:0|control=192.0.2.1:40123|control-mode=dynamic"),
                Map.entry(NodeConfig.Setting.AERON_ARCHIVE_REPLICATION_CHANNEL.key(), "aeron:udp?endpoint=192.0.2.1:0"),
                Map.entry(NodeConfig.Setting.AERON_CONTROL_RESPONSE_CHANNEL.key(), "aeron:udp?endpoint=192.0.2.1:0"),
                Map.entry(NodeConfig.Setting.AERON_WATERMARK_CHANNEL.key(), "aeron:udp?endpoint=192.0.2.1:40125"),
                Map.entry(NodeConfig.Setting.AERON_CONTROL_CHANNEL.key(), "aeron:udp?endpoint=192.0.2.1:40124")));
    }

    /// Verifies an empty Aeron reader fails before creating a divergent Store image.
    @Test
    void emptyAeronReaderFailsWithoutSeed(@TempDir final Path root) {
        final Path readerHome = root.resolve("reader-home");

        try (final NodeLifecycle reader = NodeLifecycle.create()
                .setNodeConfig(readerConfig(readerHome, root.resolve("aeron")))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            final var failure = assertThrows(ReseedRequiredException.class, reader::startStorageManager);
            assertTrue(failure.getMessage().startsWith("RESEED_REQUIRED"),
                    "reader seed failure must carry the reseed marker, was: %s".formatted(failure.getMessage()));
            assertFalse(Files.exists(readerHome.resolve("storage")),
                    "a rejected reader must not create a Store image");
        }
    }

    /// Verifies an Aeron reader with Store files but no durable replication mark fails closed with a reseed error.
    @Test
    void aeronReaderWithStoreFilesButNoMarkFailsClosed(@TempDir final Path root) throws Exception {
        final Path readerHome = root.resolve("lost-cursor-reader");
        final Path storage = readerHome.resolve("storage");
        Files.createDirectories(storage);
        Files.writeString(storage.resolve("orphaned-channel.dat"), "unaddressable history");

        try (final NodeLifecycle reader = NodeLifecycle.create()
                .setNodeConfig(readerConfig(readerHome, root.resolve("lost-cursor-aeron")))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            final var failure = assertThrows(ReseedRequiredException.class, reader::startStorageManager);
            assertTrue(failure.getMessage().startsWith("RESEED_REQUIRED"),
                    "lost cursor must fail closed, was: %s".formatted(failure.getMessage()));
            assertTrue(failure.getMessage().contains("no committed replication mark"),
                    "lost mark must name the missing Store boundary, was: %s".formatted(failure.getMessage()));
        }
    }

    /// Verifies a standalone node can reopen a copied Store image and read its root.
    @Test
    void standaloneNodeStartsFromCopiedStoreImage(@TempDir final Path root) throws Exception {
        final Path writerHome = root.resolve("writer-home");
        final Path readerHome = root.resolve("reader-home");

        try (final NodeLifecycle writer = NodeLifecycle.create()
                .setNodeConfig(localConfig(writerHome, root.resolve("writer-backups")))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            final var manager = writer.startStorageManager();
            final ArrayList<String> writerRoot = new ArrayList<>();
            writerRoot.add("seeded-value");
            manager.setRoot(org.eclipse.serializer.reference.Lazy.Reference(writerRoot));
            manager.storeRoot();
        }

        assertTrue(Files.isDirectory(writerHome.resolve("storage")),
                "writer must have created its Store directory at %s".formatted(writerHome.resolve("storage")));

        copyDirectory(writerHome.resolve("storage"), readerHome.resolve("storage"));

        try (final NodeLifecycle reader = NodeLifecycle.create()
                .setNodeConfig(localConfig(readerHome, root.resolve("reader-backups")))
                .setRootSupplier(ArrayList<String>::new)
                .build()) {
            @SuppressWarnings("unchecked")
            final ArrayList<String> readerRoot = reader.startStorageManager()
                    .graphBoundary().read(() -> new ArrayList<>((ArrayList<String>) ((org.eclipse.serializer.reference.Lazy<?>) reader.startStorageManager().root()).get()));
            assertTrue(readerRoot.contains("seeded-value"), "seeded reader must reproduce the writer's root");
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
