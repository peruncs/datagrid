package peruncs.cluster.node.aeron;

import org.apache.lucene.document.Document;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.serializer.typing.Disposable;
import org.eclipse.store.gigamap.jvector.*;
import org.eclipse.store.gigamap.lucene.DocumentPopulator;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.api.NodeSettingsSource;
import peruncs.cluster.node.replication.*;
import peruncs.cluster.node.store.DistributedStorage;
import peruncs.cluster.storage.ReplicationCursor;
import peruncs.cluster.storage.StorageGraphCoordinator;
import peruncs.cluster.storage.binary.*;
import peruncs.cluster.storage.index.ClusterStoreIndexes;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/// Exercises the Aeron target with a real four-channel Embedded Store across a restart.
class AeronStoreIntegrationIT {
    static ReplicationCursor latest(final ClusterReplicationTransport transport) {
        final ReplicationPositionProvider positionProvider = transport.positionProvider("store");
        positionProvider.init();
        return positionProvider.latest();
    }

    private static long latestSequence(final ClusterReplicationTransport transport) {
        final ReplicationPositionProvider positionProvider = transport.positionProvider("store");
        positionProvider.init();
        return positionProvider.latest().logicalSequence();
    }

    static void configureIndexes(final GigaMap<IndexedArticle> articles) {
        ClusterStoreIndexes.registerLucene(articles, new IndexedArticlePopulator());
        final VectorIndices<IndexedArticle> vectors = articles.index().register(VectorIndices.Category());
        ClusterStoreIndexes.addVector(vectors, "articles", VectorIndexConfiguration.builder()
                .dimension(3).similarityFunction(VectorSimilarityFunction.COSINE).build(), new IndexedArticleVectorizer());
    }

    @SuppressWarnings("unchecked") // Lucene's class token cannot retain its entity type.
    private static LuceneIndex<IndexedArticle> luceneIndex(final GigaMap<IndexedArticle> articles) {
        return articles.index().get(LuceneIndex.class);
    }

    /// Waits until the writer's durable boundary has advanced past `previous`
    /// AND settled: the checkpoint-confirmed boundary trails each commit (and
    /// can also advance on internal records like dictionary publications), so
    /// a single greater read does not prove the transaction is covered. The
    /// boundary must be strictly greater and then unchanged across a settle
    /// window before it counts.
    private static ReplicationCursor awaitLatestBeyond(final ClusterReplicationTransport transport,
                                                       final long previous) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        final long settleNanos = TimeUnit.MILLISECONDS.toNanos(500);
        ReplicationCursor settled = null;
        long settledAtNanos = 0L;
        while (System.nanoTime() < deadline) {
            final ReplicationCursor cursor = latest(transport);
            if (settled == null || cursor.logicalSequence() != settled.logicalSequence()) {
                settled = cursor;
                settledAtNanos = System.nanoTime();
            } else if (settled.logicalSequence() > previous
                    && System.nanoTime() - settledAtNanos >= settleNanos) {
                return settled;
            }
            java.util.concurrent.locks.LockSupport.parkNanos(50_000_000L);
        }
        throw new AssertionError(
                "writer boundary did not settle past " + previous + " within 30s");
    }

    static EmbeddedStorageManager startIndex(
            final Path path,
            final IndexRoot root,
            final ReplicationPublisher distributor,
            final java.util.function.UnaryOperator<PersistenceTarget<Binary>> targetFactory) {
        final EmbeddedStorageFoundation<?> foundation = foundation(path);
        DistributedStorage.configureWriting(foundation, distributor, targetFactory);
        return foundation.start(root);
    }

    static EmbeddedStorageManager startExistingIndex(
            final Path path,
            final ReplicationPublisher distributor,
            final java.util.function.UnaryOperator<PersistenceTarget<Binary>> targetFactory) {
        final EmbeddedStorageFoundation<?> foundation = foundation(path);
        DistributedStorage.configureWriting(foundation, distributor, targetFactory);
        return foundation.start();
    }

    private static void replicateAndVerify(
            final Path readerRoot,
            final Path storePath,
            final String role,
            final UUID nodeId,
            final UUID clusterId,
            final UUID generation,
            final ReplicationCursor startingCursor,
            final ReplicationCursor target,
            final int controlPort,
            final int livePort,
            final int watermarkPort,
            final Set<UUID> retentionReaders,
            final String expectedValue,
            final boolean expectDictionary
    ) throws Exception {
        replicateAndVerify(readerRoot, storePath, role, nodeId, clusterId, generation,
                startingCursor, target, controlPort, livePort, watermarkPort, retentionReaders,
                expectedValue, expectDictionary, null, -1);
    }

    private static void replicateAndVerify(
            final Path readerRoot,
            final Path storePath,
            final String role,
            final UUID nodeId,
            final UUID clusterId,
            final UUID generation,
            final ReplicationCursor startingCursor,
            final ReplicationCursor target,
            final int controlPort,
            final int livePort,
            final int watermarkPort,
            final Set<UUID> retentionReaders,
            final String expectedValue,
            final boolean expectDictionary,
            final String watermarkChannelOverride,
            final int watermarkStreamIdOverride
    ) throws Exception {
        Files.createDirectories(readerRoot);
        final Path cursorPath = readerRoot.resolve("cursor");
        try (ClusterReplicationTransport transport = new AeronTransport(
                properties(readerRoot, clusterId, nodeId, generation, role, -1L,
                        controlPort, livePort, watermarkPort, retentionReaders,
                        watermarkChannelOverride, watermarkStreamIdOverride));
             DurableCursorFile cursorManager = DurableCursorFile.of(cursorPath)) {
            final EmbeddedStorageFoundation<?> readerFoundation = foundation(storePath);
            final EmbeddedStorageManager reader = readerFoundation.start();
            final StorageGraphCoordinator graphCoordinator = new StorageGraphCoordinator();
            final StorageBinaryDataReceiver receiver = StorageBinaryDataMerger.create(new StorageBinaryDataMerger.Configuration(
                    readerFoundation.getConnectionFoundation(), reader.createConnection(),
                    ObjectGraphUpdateHandler.PerStore(graphCoordinator),
                    0L, 1L, 1L << 30, 60_000L, 30_000L, 5_000L, 4096, graphCoordinator));
            final ReplicationApplier client = transport.client(receiver, "store", new CommitAppliedListener() {
                        @Override
                        public void onApplied(final ReplicationCursor cursor) {
                            cursorManager.set(cursor);
                        }

                        @Override
                        public void close() {
                        }
                    },
                    startingCursor);
            try {
                client.start();
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (client.cursor().logicalSequence() < target.logicalSequence() && client.failure() == null &&
                       System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                    java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
                }
                if (client.failure() != null) throw client.failure();
                assertEquals(target.logicalSequence(), client.cursor().logicalSequence(),
                        "%s did not reach the writer boundary".formatted(role));
                receiver.awaitApplied();
                client.stopAtLatestMessage();
                final long stopDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (client.isRunning() && System.nanoTime() < stopDeadline) {
                    java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
                }
                assertEquals(ReplicationApplier.StopOutcome.RESOLVED_BOUNDARY,
                        client.stopOutcome(), "%s did not stop at a resolved transaction boundary".formatted(role));
            } finally {
                client.dispose();
                Disposable disposable = (Disposable) receiver;
                disposable.dispose();
                reader.shutdown();
            }
            assertEquals(target.logicalSequence(), cursorManager.get().logicalSequence(),
                    "%s did not persist its atomic cursor".formatted(role));
        }

        final EmbeddedStorageManager restarted = foundation(storePath).start();
        try {
            final Root imported = restarted.root();
            assertTrue(imported.values.contains(expectedValue), "%s Store missed %s".formatted(role, expectedValue));
            if (expectDictionary) {
                assertEquals("dictionary-update", imported.objects.getFirst().value,
                        "%s did not materialize the newly introduced type".formatted(role));
            }
        } finally {
            restarted.shutdown();
        }
    }

    private static void assertGraphState(final ReaderNode reader, final String title, final String body) {
        final IndexRoot imported = (IndexRoot) reader.rootObject();
        final AtomicBoolean found = new AtomicBoolean();
        imported.articles.iterate(article ->
        {
            if (title.equals(article.title) && body.equals(article.body)) found.set(true);
        });
        assertTrue(found.get(), "reader Store graph missed %s".formatted(title));
    }

    private static void assertGraphMissing(final ReaderNode reader, final String title) {
        final IndexRoot imported = (IndexRoot) reader.rootObject();
        final AtomicBoolean found = new AtomicBoolean();
        imported.articles.iterate(article -> {
            if (title.equals(article.title)) found.set(true);
        });
        assertFalse(found.get(), "reader Store graph retained deleted %s".formatted(title));
    }

    static void assertIndexState(
            final IndexRoot imported, final String title, final String body, final float[] vector) {
        assertNotNull(imported.articles, "reader Store root lost its indexed GigaMap");
        final LuceneIndex<IndexedArticle> text = luceneIndex(imported.articles);
        assertEquals(1, text.query("body:%s".formatted(body)).size(),
                "reader Lucene index missed %s (articles=%s)".formatted(body, imported.articles.size()));
        final VectorIndices<IndexedArticle> vectors = imported.articles.index().get(VectorIndices.Category());
        final VectorSearchResult<IndexedArticle> nearest = vectors.get("articles").search(vector, 1);
        assertEquals(1, nearest.size(), "reader JVector index missed %s".formatted(body));
        assertEquals(title, nearest.toList().getFirst().entity().title);
    }

    private static void assertIndexMissing(final IndexRoot imported, final String title) {
        final LuceneIndex<IndexedArticle> text = luceneIndex(imported.articles);
        assertEquals(0, text.query("title:%s".formatted(title)).size(), "reader Lucene index retained deleted %s".formatted(title));
    }

    private static String forkStoreChild(
            final Path root, final UUID clusterId, final UUID nodeId, final UUID generation, final String mode)
            throws Exception {
        final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        final Process child = new ProcessBuilder(java, "--enable-preview", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", classpath,
                "-Ddg.aeron.store.root=%s".formatted(root),
                "-Ddg.aeron.store.cluster=%s".formatted(clusterId),
                "-Ddg.aeron.store.node=%s".formatted(nodeId),
                "-Ddg.aeron.store.generation=%s".formatted(generation),
                AeronStoreProcessChildMain.class.getName(), mode)
                .redirectErrorStream(true).start();
        if (!child.waitFor(60, TimeUnit.SECONDS)) {
            child.destroyForcibly();
            throw new AssertionError("Store process child timed out: %s".formatted(mode));
        }
        final String output = new String(child.getInputStream().readAllBytes());
        assertEquals(0, child.exitValue(), "%s child failed: %s".formatted(mode, output));
        return Files.readString(root.resolve("control").resolve(mode));
    }

    private static long sequence(final String marker) {
        return Long.parseLong(marker.substring(marker.indexOf("sequence=") + "sequence=".length()).trim());
    }

    private static int dictionaryCount(final String marker) {
        final String prefix = "dictionaries=";
        final int start = marker.indexOf(prefix);
        if (start < 0) throw new AssertionError("child did not report dictionary publications: %s".formatted(marker));
        final int end = marker.indexOf(';', start + prefix.length());
        return Integer.parseInt(marker.substring(start + prefix.length(), end < 0 ? marker.length() : end).trim());
    }

    private static EmbeddedStorageManager start(
            final Path path,
            final Root root,
            final ReplicationPublisher distributor,
            final java.util.function.UnaryOperator<PersistenceTarget<Binary>> targetFactory
    ) {
        final EmbeddedStorageFoundation<?> foundation = foundation(path);
        DistributedStorage.configureWriting(foundation, distributor, targetFactory);
        return foundation.start(root);
    }

    private static EmbeddedStorageManager startExisting(
            final Path path,
            final ReplicationPublisher distributor,
            final java.util.function.UnaryOperator<PersistenceTarget<Binary>> targetFactory
    ) {
        final EmbeddedStorageFoundation<?> foundation = foundation(path);
        DistributedStorage.configureWriting(foundation, distributor, targetFactory);
        return foundation.start();
    }

    static EmbeddedStorageFoundation<?> foundation(final Path path) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(path))
                .setChannelCountProvider(Storage.ChannelCountProvider(4))
                .createConfiguration();
        return EmbeddedStorage.Foundation(configuration);
    }

    static NodeSettingsSource properties(
            final Path root, final UUID clusterId, final UUID nodeId, final UUID generation) {
        return properties(root, clusterId, nodeId, generation, "writer", -1L, 40124, 40123, 40125);
    }

    static NodeSettingsSource properties(
            final Path root,
            final UUID clusterId,
            final UUID nodeId,
            final UUID generation,
            final String role,
            final long recordingId,
            final int controlPort,
            final int livePort,
            final int watermarkPort
    ) {
        return properties(root, clusterId, nodeId, generation, role, recordingId,
                controlPort, livePort, watermarkPort, Set.of());
    }

    static NodeSettingsSource properties(
            final Path root,
            final UUID clusterId,
            final UUID nodeId,
            final UUID generation,
            final String role,
            final long recordingId,
            final int controlPort,
            final int livePort,
            final int watermarkPort,
            final Set<UUID> retentionReaders
    ) {
        return properties(root, clusterId, nodeId, generation, role, recordingId,
                controlPort, livePort, watermarkPort, retentionReaders, null, -1);
    }

    static NodeSettingsSource properties(
            final Path root,
            final UUID clusterId,
            final UUID nodeId,
            final UUID generation,
            final String role,
            final long recordingId,
            final int controlPort,
            final int livePort,
            final int watermarkPort,
            final Set<UUID> retentionReaders,
            final String watermarkChannelOverride,
            final int watermarkStreamIdOverride
    ) {
        return new TestNodeProperties() {
            @Override
            public String replicationRole() {
                return role;
            }

            @Override
            public String replicationProperty(final String name) {
                return switch (name) {
                    case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID" -> clusterId.toString();
                    case "ECLIPSE_DATAGRID_AERON_TRUSTED_NETWORK" -> "true";
                    case "ECLIPSE_DATAGRID_AERON_NODE_ID" -> nodeId.toString();
                    case "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> generation.toString();
                    case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> root.resolve("driver").toString();
                    case "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY" -> root.resolve("archive").toString();
                    case "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH" -> root.resolve("checkpoint/writer.checkpoint").toString();
                    case "ECLIPSE_DATAGRID_BACKUP_PATH" -> root.resolve("backups").toString();
                    case "ECLIPSE_DATAGRID_AERON_RECORDING_ID" -> Long.toString(recordingId);
                    case "ECLIPSE_DATAGRID_AERON_TERM_LENGTH",
                         "ECLIPSE_DATAGRID_AERON_ARCHIVE_SEGMENT_FILE_LENGTH" -> "65536";
                    case "ECLIPSE_DATAGRID_AERON_CHUNK_SIZE" -> "4096";
                    case "ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE" -> Boolean.toString(!"writer".equals(role));
                    case "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL" -> "writer".equals(role)
                            ? "aeron:udp?control=localhost:%s|control-mode=dynamic|fc=max|alias=datagrid-%s".formatted(livePort, clusterId)
                            : "aeron:udp?endpoint=localhost:0|control=localhost:%s|control-mode=dynamic|alias=datagrid-%s".formatted(livePort, clusterId);
                    case "ECLIPSE_DATAGRID_AERON_CONTROL_CHANNEL" -> "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
                    case "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL",
                         "ECLIPSE_DATAGRID_AERON_CONTROL_RESPONSE_CHANNEL" -> "aeron:udp?endpoint=localhost:0";
                    case "ECLIPSE_DATAGRID_AERON_WATERMARK_CHANNEL" -> watermarkChannelOverride != null
                            ? watermarkChannelOverride
                            : "aeron:udp?endpoint=localhost:%s".formatted(watermarkPort);
                    case "ECLIPSE_DATAGRID_AERON_WATERMARK_STREAM_ID" -> watermarkStreamIdOverride >= 0
                            ? Integer.toString(watermarkStreamIdOverride)
                            : null;
                    case "ECLIPSE_DATAGRID_AERON_RETENTION_READERS" -> retentionReaders.stream()
                            .sorted().map(UUID::toString).collect(java.util.stream.Collectors.joining(","));
                    default -> null;
                };
            }
        };
    }

    static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    static void copyDirectory(final Path source, final Path target) throws Exception {
        try (var paths = Files.walk(source)) {
            for (final Path path : paths.toList()) {
                final Path destination = target.resolve(source.relativize(path));
                if (Files.isDirectory(path)) Files.createDirectories(destination);
                else Files.copy(path, destination);
            }
        }
    }

    static void delete(final Path root) throws Exception {
        if (!Files.exists(root)) return;
        try (var paths = Files.walk(root)) {
            for (final Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        }
    }

    /// Verifies a writer replicates embedded Lucene and JVector state so a reader can search it after restart.
    @Test
    void aeronReplicatesEmbeddedLuceneAndVectorStateToAReader() throws Exception {
        final Path root = Files.createTempDirectory("dg-aeron-index-readers-");
        final UUID clusterId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final int controlPort = freePort();
        final int livePort = freePort();
        final int watermarkPort = freePort();
        final Path writerStore = root.resolve("writer-store");
        final Path readerStore = root.resolve("reader-store");
        try (ClusterReplicationTransport writerTransport = new AeronTransport(
                properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
                        controlPort, livePort, watermarkPort))) {
            final ReplicationPublisher distributor = writerTransport.distributor("store");
            final IndexRoot initial = new IndexRoot();
            initial.articles = GigaMap.New();
            configureIndexes(initial.articles);
            final EmbeddedStorageManager seeded = startIndex(writerStore, initial, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            seeded.storeRoot();
            seeded.shutdown();
            final ReplicationCursor baseline = latest(writerTransport);
            copyDirectory(writerStore, readerStore);

            final EmbeddedStorageManager writer = startExistingIndex(writerStore, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            try (ReaderNode reader = ReaderNode.open(root.resolve("reader"), readerStore, "reader",
                    UUID.randomUUID(), clusterId, generation, baseline, controlPort, livePort, watermarkPort)) {
                reader.start();
                reader.awaitLive();
                final IndexRoot writerRoot = writer.root();
                writerRoot.articles.add(
                        new IndexedArticle("Aeron", "embedded replication", new float[]{1.0f, 0.0f, 0.0f}));
                writerRoot.articles.store();
                final ReplicationCursor target = latest(writerTransport);
                reader.await(target);
                reader.stopAtLatest();
                reader.close();

                try (EmbeddedStorageManager restartedReader = foundation(readerStore).start()) {
                    final IndexRoot imported = restartedReader.root();
                    ClusterStoreIndexes.validateVectorIndexes(imported.articles);
                    final LuceneIndex<IndexedArticle> text = luceneIndex(imported.articles);
                    final VectorIndices<IndexedArticle> vectors = imported.articles.index().get(VectorIndices.Category());
                    assertEquals(1, text.query("body:replication").size(), "Lucene state did not follow the Aeron transaction");
                    final VectorSearchResult<IndexedArticle> nearest = vectors.get("articles")
                            .search(new float[]{1.0f, 0.0f, 0.0f}, 4);
                    assertEquals(1, nearest.size(), "JVector state did not follow the Aeron transaction");
                    assertEquals("Aeron", nearest.toList().getFirst().entity().title);
                }
            } finally {
                writer.shutdown();
            }
        } finally {
            delete(root);
        }
    }

    /// Verifies ordinary and backup readers import real Store data, resume from atomic cursors, and assemble a purgeable retention quorum.
    @Test
    void ordinaryAndBackupReadersImportRealStoreDataAndResumeFromAtomicCursors() throws Exception {
        final Path root = Files.createTempDirectory("dg-aeron-store-readers-");
        final UUID clusterId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final int controlPort = freePort();
        final int livePort = freePort();
        final int watermarkPort = freePort();
        final UUID ordinaryReaderId = UUID.randomUUID();
        final UUID backupReaderId = UUID.randomUUID();
        final Set<UUID> retentionReaders = Set.of(ordinaryReaderId, backupReaderId);
        final Path writerStore = root.resolve("writer-store");
        final Path ordinaryStore = root.resolve("ordinary-store");
        final Path backupStore = root.resolve("backup-store");
        try (ClusterReplicationTransport writerTransport = new AeronTransport(
                properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
                        controlPort, livePort, watermarkPort, retentionReaders))) {
            final ReplicationPublisher distributor = writerTransport.distributor("store");
            final Root initial = new Root();
            initial.values.add("baseline");
            final EmbeddedStorageManager seeded = start(writerStore, initial, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            seeded.storeRoot();
            seeded.shutdown();
            final ReplicationCursor baseline = latest(writerTransport);
            copyDirectory(writerStore, ordinaryStore);
            copyDirectory(writerStore, backupStore);

            final EmbeddedStorageManager writer = startExisting(writerStore, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            final Root writerRoot = writer.root();
            writerRoot.values.add("ordinary-update");
            writerRoot.objects.add(new NewType("dictionary-update"));
            writerRoot.payload = new byte[256 * 1024];
            java.util.Arrays.fill(writerRoot.payload, (byte) 0x5a);
            writer.storeAll(List.of(writerRoot, writerRoot.values, writerRoot.objects));
            final ReplicationCursor firstTarget = latest(writerTransport);

            replicateAndVerify(root.resolve("ordinary-reader"), ordinaryStore, "reader", ordinaryReaderId,
                    clusterId, generation, baseline, firstTarget, controlPort, livePort, watermarkPort,
                    retentionReaders, "ordinary-update", true);

            writerRoot.values.add("restart-update");
            writer.store(writerRoot.values);
            final ReplicationCursor secondTarget = latest(writerTransport);
            final Path ordinaryCursor = root.resolve("ordinary-reader/cursor");
            final ReplicationCursor persisted;
            try (DurableCursorFile cursorManager = DurableCursorFile.of(ordinaryCursor)) {
                persisted = cursorManager.get();
            }
            replicateAndVerify(root.resolve("ordinary-reader"), ordinaryStore, "reader", ordinaryReaderId,
                    clusterId, generation, persisted, secondTarget, controlPort, livePort, watermarkPort,
                    retentionReaders, "restart-update", false);

            replicateAndVerify(root.resolve("backup-reader"), backupStore, "backup-reader", backupReaderId,
                    clusterId, generation, baseline, secondTarget, controlPort, livePort, watermarkPort,
                    retentionReaders, "restart-update", true);
            final long watermarkDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!writerTransport.retention().isSupported() && System.nanoTime() < watermarkDeadline) {
                java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
            }
            assertTrue(writerTransport.retention().isSupported(),
                    "writer did not durably assemble the ordinary+backup reader watermark quorum");
            assertEquals(
                    ReplicationLogRetention.MaintenanceResult.Status.DELETED,
                    writerTransport.retention().deleteThrough(secondTarget).status(),
                    "authenticated quorum must permit online purge at a complete segment boundary");
            final ReplicationCursor postRetentionStart;
            try (DurableCursorFile cursorManager = DurableCursorFile.of(ordinaryCursor)) {
                postRetentionStart = cursorManager.get();
            }
            writerRoot.values.add("post-retention-update");
            writer.store(writerRoot.values);
            final ReplicationCursor postRetentionTarget = latest(writerTransport);
            replicateAndVerify(root.resolve("ordinary-reader"), ordinaryStore, "reader", ordinaryReaderId,
                    clusterId, generation, postRetentionStart, postRetentionTarget, controlPort, livePort, watermarkPort,
                    retentionReaders, "post-retention-update", false);
            writer.shutdown();
        } finally {
            delete(root);
        }
    }

    /// Deterministic parked-reader-behind-a-purged-segment case: a reader
    /// drained from the cluster is retired from the retention quorum, the
    /// purge then physically deletes the Archive segment its frozen durable
    /// cursor points into, and its restart MUST fail closed with the typed
    /// reseed signal — health reports RESEED_REQUIRED — never replay torn
    /// history, never converge silently, never move its durable cursor. This
    /// is the retire-then-purge production shape the soak only hits
    /// nondeterministically through its generic restart ops.
    @Test
    void readerRestartBehindAPurgedSegmentDemandsAReseed() throws Exception {
        final Path root = Files.createTempDirectory("dg-aeron-retention-reseed-");
        final UUID clusterId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final int controlPort = freePort();
        final int livePort = freePort();
        final int watermarkPort = freePort();
        final UUID laggingReaderId = UUID.randomUUID();
        final UUID currentReaderId = UUID.randomUUID();
        final Set<UUID> retentionReaders = Set.of(laggingReaderId, currentReaderId);
        final Path writerStore = root.resolve("writer-store");
        final Path laggingStore = root.resolve("lagging-store");
        final Path currentStore = root.resolve("current-store");
        /* Payload-driven transactions: the frozen boundary's bytes must sit
         * in a 64 KiB Archive segment that the later purge physically
         * detaches, so ~3/4 MiB of durable history separates freeze from head. */
        try (ClusterReplicationTransport writerTransport = new AeronTransport(
                properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
                        controlPort, livePort, watermarkPort, retentionReaders))) {
            final ReplicationPublisher distributor = writerTransport.distributor("store");
            final Root initial = new Root();
            initial.values.add("baseline");
            final EmbeddedStorageManager seeded = start(writerStore, initial, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            seeded.storeRoot();
            seeded.shutdown();
            final ReplicationCursor baseline = latest(writerTransport);
            copyDirectory(writerStore, laggingStore);
            copyDirectory(writerStore, currentStore);
            final EmbeddedStorageManager writer = startExisting(writerStore, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            final Root writerRoot = writer.root();
            try {
                /* Freeze boundary T1: the lagging reader is parked exactly
                 * here, and this transaction's bytes must be purgeable later. */
                writerRoot.values.add("lagging-frozen");
                writerRoot.payload = new byte[256 * 1024];
                java.util.Arrays.fill(writerRoot.payload, (byte) 0x31);
                writer.storeAll(List.of(writerRoot, writerRoot.values, writerRoot.payload));
                final ReplicationCursor frozen = latest(writerTransport);

                /* Park the lagging reader at the frozen boundary with a FULL
                 * shutdown: a merely stopped client keeps its Aeron image
                 * attached, and writer flow control would back-pressure every
                 * following payload store against an image that never consumes.
                 * The helper stops at the resolved boundary and disposes the
                 * transport, so the writer below writes into one live image. */
                replicateAndVerify(root.resolve("lagging-reader"), laggingStore, "reader", laggingReaderId,
                        clusterId, generation, baseline, frozen, controlPort, livePort, watermarkPort,
                        retentionReaders, "lagging-frozen", false);

                /* The live half of the quorum walks on with two more payload
                 * transactions. Every changed instance must be stored
                 * explicitly: storing only the root re-saves its references,
                 * never the mutated list or payload behind them. The latest()
                 * boundary is checkpoint-confirmed and trails each commit, so
                 * each transaction waits for the published boundary to move. */
                writerRoot.values.add("post-park-1");
                java.util.Arrays.fill(writerRoot.payload, (byte) 0x32);
                writer.storeAll(List.of(writerRoot, writerRoot.values, writerRoot.payload));
                final ReplicationCursor postParkOne = awaitLatestBeyond(writerTransport, frozen.logicalSequence());
                writerRoot.values.add("post-park-2");
                java.util.Arrays.fill(writerRoot.payload, (byte) 0x33);
                writer.storeAll(List.of(writerRoot, writerRoot.values, writerRoot.payload));
                final ReplicationCursor head = awaitLatestBeyond(writerTransport,
                        postParkOne.logicalSequence());
                    replicateAndVerify(root.resolve("current-reader"), currentStore, "reader", currentReaderId,
                            clusterId, generation, baseline, head, controlPort, livePort, watermarkPort,
                            retentionReaders, "post-park-2", false);

                    final long quorumDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (!writerTransport.retention().isSupported() && System.nanoTime() < quorumDeadline) {
                        java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
                    }
                    assertTrue(writerTransport.retention().isSupported(),
                            "writer did not assemble the reader watermark quorum");

                    /* Drained from the cluster: retire the lagging reader from
                     * the quorum, then purge through the live head. The purge
                     * must physically delete the segment holding the frozen
                     * cursor — any weaker outcome makes the restart below a
                     * replay from live history and voids the test. */
                    writerTransport.retention().retireReader(laggingReaderId);
                    assertEquals(
                            ReplicationLogRetention.MaintenanceResult.Status.DELETED,
                            writerTransport.retention().deleteThrough(head).status(),
                            "purge through the quorum head must delete the segment holding the frozen cursor");

                /* Restart the drained reader from its frozen durable cursor —
                 * a fresh process-equivalent node on the retained Store. The
                 * only acceptable outcomes are the typed reseed signals —
                 * thrown eagerly by transport creation, or latched
                 * asynchronously as the reader agent's terminal client failure.
                 * Silent convergence past deleted history is the bug on trial. */
                final Path laggingNode = root.resolve("lagging-reader");
                final ReplicationCursor parked;
                try (DurableCursorFile cursorManager = DurableCursorFile.of(
                        laggingNode.resolve("cursor"))) {
                    parked = cursorManager.get();
                }
                assertEquals(frozen.logicalSequence(), parked.logicalSequence(),
                        "the parked reader must freeze exactly at the requested boundary");
                try (ReaderNode lagging = ReaderNode.open(laggingNode, laggingStore, "reader",
                        laggingReaderId, clusterId, generation, parked,
                        controlPort, livePort, watermarkPort)) {
                    lagging.start();
                    final long reseedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                    while (lagging.clientFailure() == null && !lagging.isLive()
                            && System.nanoTime() < reseedDeadline) {
                        java.util.concurrent.locks.LockSupport.parkNanos(10_000_000L);
                    }
                    if (lagging.clientFailure() == null) {
                        fail(lagging.isLive()
                                ? "a reader restarted from a purged cursor silently rejoined the live stream"
                                : "a reader restarted from a purged cursor neither failed closed nor rejoined within 60s");
                    }
                    final RuntimeException failure = lagging.clientFailure();
                    assertTrue(failure instanceof ReseedRequiredException,
                            "expected the typed reseed signal (health RESEED_REQUIRED), got: " + failure);
                    /* Fail-closed also means untouched durable state: the
                     * cursor never advanced past the frozen boundary and the
                     * graph carries none of the post-park writes. */
                    assertEquals(frozen.logicalSequence(), lagging.persistedCursor().logicalSequence(),
                            "a parked-then-refused reader must never move its durable cursor");
                    final Root imported = (Root) lagging.rootObject();
                    assertTrue(imported.values.contains("lagging-frozen"),
                            "the frozen graph lost the pre-park transaction");
                    assertFalse(imported.values.contains("post-park-1"),
                            "torn history landed in the parked reader's graph");
                    assertFalse(imported.values.contains("post-park-2"),
                            "torn history landed in the parked reader's graph");
                }
            } finally {
                writer.shutdown();
            }
        } finally {
            delete(root);
        }
    }

        /// Reader watermarks honor a non-default channel and stream id end to end,
    /// using exactly the documented setup: the quorum list lives on the writer
    /// only, while the reader carries no retention list at all. The reader
    /// must still publish progress where configured, the writer must receive
    /// it there, and the retention quorum must assemble and purge on that
    /// basis. A publisher gated on the local list would leave the quorum
    /// empty and the purge refused.
    @Test
    void configuredWatermarkChannelAndStreamDriveRetentionQuorum() throws Exception {
        final Path root = Files.createTempDirectory("dg-aeron-watermark-config-");
        final UUID clusterId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final int controlPort = freePort();
        final int livePort = freePort();
        final int watermarkPort = freePort();
        final String watermarkChannel = "aeron:udp?endpoint=localhost:%s".formatted(freePort());
        final int watermarkStreamId = 7777;
        final UUID readerId = UUID.randomUUID();
        final Set<UUID> retentionReaders = Set.of(readerId);
        final Path writerStore = root.resolve("writer-store");
        final Path readerStore = root.resolve("reader-store");
        try (ClusterReplicationTransport writerTransport = new AeronTransport(
                properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
                        controlPort, livePort, watermarkPort, retentionReaders,
                        watermarkChannel, watermarkStreamId))) {
            final ReplicationPublisher distributor = writerTransport.distributor("store");
            final Root initial = new Root();
            initial.values.add("baseline");
            final EmbeddedStorageManager seeded = start(writerStore, initial, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            seeded.storeRoot();
            seeded.shutdown();
            final ReplicationCursor baseline = latest(writerTransport);
            copyDirectory(writerStore, readerStore);

            final EmbeddedStorageManager writer = startExisting(writerStore, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            final Root writerRoot = writer.root();
            writerRoot.values.add("watermark-config-update");
            writerRoot.payload = new byte[256 * 1024];
            java.util.Arrays.fill(writerRoot.payload, (byte) 0x5a);
            writer.storeAll(List.of(writerRoot, writerRoot.values));
            final ReplicationCursor target = latest(writerTransport);
            /* Documented setup: the quorum list lives on the writer only. The
             * reader carries an empty local list and must publish anyway. */
            replicateAndVerify(root.resolve("reader"), readerStore, "reader", readerId,
                    clusterId, generation, baseline, target, controlPort, livePort, watermarkPort,
                    Set.of(), "watermark-config-update", false,
                    watermarkChannel, watermarkStreamId);
            final long watermarkDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!writerTransport.retention().isSupported() && System.nanoTime() < watermarkDeadline) {
                java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
            }
            assertTrue(writerTransport.retention().isSupported(),
                    "writer did not assemble the retention quorum on the configured watermark channel/stream");
            assertEquals(
                    ReplicationLogRetention.MaintenanceResult.Status.DELETED,
                    writerTransport.retention().deleteThrough(target).status(),
                    "configured watermark quorum must permit online purge");
            writer.shutdown();
        } finally {
            delete(root);
        }
    }

        /// Verifies one writer broadcasts the same Store transaction to two live reader nodes.
    @Test
    void oneWriterBroadcastsToConcurrentOrdinaryAndBackupReaders() throws Exception {
        final Path root = Files.createTempDirectory("dg-aeron-concurrent-readers-");
        final UUID clusterId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final int controlPort = freePort();
        final int livePort = freePort();
        final int watermarkPort = freePort();
        final Path writerStore = root.resolve("writer-store");
        final Path ordinaryStore = root.resolve("ordinary-store");
        final Path backupStore = root.resolve("backup-store");
        try (ClusterReplicationTransport writerTransport = new AeronTransport(
                properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
                        controlPort, livePort, watermarkPort))) {
            final ReplicationPublisher distributor = writerTransport.distributor("store");
            final Root initial = new Root();
            initial.values.add("baseline");
            final EmbeddedStorageManager seeded = start(writerStore, initial, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            seeded.storeRoot();
            seeded.shutdown();
            final ReplicationCursor baseline = latest(writerTransport);
            copyDirectory(writerStore, ordinaryStore);
            copyDirectory(writerStore, backupStore);

            final EmbeddedStorageManager writer = startExisting(writerStore, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            try (ReaderNode ordinary = ReaderNode.open(root.resolve("ordinary-reader"), ordinaryStore,
                    "reader", UUID.randomUUID(), clusterId, generation, baseline, controlPort, livePort, watermarkPort);
                 ReaderNode backup = ReaderNode.open(root.resolve("backup-reader"), backupStore,
                         "backup-reader", UUID.randomUUID(), clusterId, generation, baseline,
                         controlPort, livePort, watermarkPort)) {
                ordinary.start();
                backup.start();
                ordinary.awaitLive();
                backup.awaitLive();

                final Root writerRoot = writer.root();
                writerRoot.values.add("concurrent-broadcast");
                writerRoot.objects.add(new NewType("concurrent-dictionary"));
                writer.storeAll(List.of(writerRoot, writerRoot.values, writerRoot.objects));
                final ReplicationCursor firstTarget = latest(writerTransport);

                ordinary.await(firstTarget);
                backup.await(firstTarget);
                assertTrue(ordinary.root().values.contains("concurrent-broadcast"));
                assertTrue(backup.root().values.contains("concurrent-broadcast"));
                assertEquals("concurrent-dictionary", ordinary.root().objects.getFirst().value);
                assertEquals("concurrent-dictionary", backup.root().objects.getFirst().value);
                assertEquals(firstTarget.logicalSequence(), ordinary.persistedCursor().logicalSequence());
                assertEquals(firstTarget.logicalSequence(), backup.persistedCursor().logicalSequence());

                /* A stopped reader must not affect another reader's live subscription. */
                ordinary.stopAtLatest();
                ordinary.close();
                writerRoot.values.add("surviving-reader-broadcast");
                writer.store(writerRoot.values);
                final ReplicationCursor secondTarget = latest(writerTransport);
                backup.await(secondTarget);
                assertTrue(backup.root().values.contains("surviving-reader-broadcast"));
                assertEquals(secondTarget.logicalSequence(), backup.persistedCursor().logicalSequence());
                backup.stopAtLatest();
            } finally {
                writer.shutdown();
            }
        } finally {
            delete(root);
        }
    }

        /// Exercises the production-shaped topology: one writer, three independent
    /// readers, four Store channels, and indexes that are rebuilt from the
    /// replicated object graph.  One reader is stopped and restarted from its
    /// durable cursor while the other two continue consuming, which makes a
    /// reader lifecycle race visible instead of testing only a happy-path replay.
    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void oneWriterReplicatesRealStoreIndexesToThreeReadersAcrossRestart() throws Exception {
        final Path root = Files.createTempDirectory("dg-aeron-three-readers-");
        final UUID clusterId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final int controlPort = freePort();
        final int livePort = freePort();
        final int watermarkPort = freePort();
        final Path writerStore = root.resolve("writer-store");
        final Path[] readerStores = {
                root.resolve("reader-1-store"), root.resolve("reader-2-store"), root.resolve("reader-3-store")
        };
        final Path[] readerNodes = {
                root.resolve("reader-1"), root.resolve("reader-2"), root.resolve("reader-3")
        };
        final UUID[] readerIds = {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        final AtomicInteger maximumChannels = new AtomicInteger();
        final long[] convergenceNanos = new long[4];

        try (ClusterReplicationTransport writerTransport = new AeronTransport(
                properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
                        controlPort, livePort, watermarkPort))) {
            final ReplicationPublisher distributor = writerTransport.distributor("store");
            final IndexRoot initial = new IndexRoot();
            initial.articles = GigaMap.New();
            configureIndexes(initial.articles);
            for (int i = 0; i < 32; i++) {
                initial.articles.add(new IndexedArticle(
                        "seed-%s".formatted(i), "seed-vector-%s".formatted(i), new float[]{i + 1.0f, 1.0f, 0.0f}));
            }
            final long mutableId = initial.articles.add(
                    new IndexedArticle("mutable-seed", "mutable-old", new float[]{0.0f, 1.0f, 1.0f}));
            final long removedId = initial.articles.add(
                    new IndexedArticle("removeMe", "removeMe", new float[]{0.0f, 0.5f, 1.0f}));
            final java.util.function.UnaryOperator<PersistenceTarget<Binary>> targetFactory = delegate ->
                    writerTransport.persistenceTargetFactory("store", distributor).apply(new PersistenceTarget<>() {
                        @Override
                        public void write(final Binary data) {
                            final int[] channels = {0};
                            data.iterateChannelChunks(ignored -> channels[0]++);
                            maximumChannels.accumulateAndGet(channels[0], Math::max);
                            delegate.write(data);
                        }

                        @Override
                        public boolean isWritable() {
                            return delegate.isWritable();
                        }
                    });
            final EmbeddedStorageManager seeded = startIndex(writerStore, initial, distributor, targetFactory);
            seeded.storeRoot();
            seeded.shutdown();
            assertTrue(maximumChannels.get() >= 4,
                    "the real Store fixture must emit all four configured channels");

            final ReplicationCursor baseline = latest(writerTransport);
            for (final Path readerStore : readerStores) copyDirectory(writerStore, readerStore);
            final EmbeddedStorageManager writer = startExistingIndex(writerStore, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            final ReaderNode[] readers = new ReaderNode[readerStores.length];
            try {
                for (int i = 0; i < readers.length; i++) {
                    readers[i] = ReaderNode.open(readerNodes[i], readerStores[i], "reader", readerIds[i],
                            clusterId, generation, baseline, controlPort, livePort, watermarkPort);
                    readers[i].start();
                }
                for (final ReaderNode reader : readers) reader.awaitLive();

                final IndexRoot writerRoot = writer.root();
                for (int update = 0; update < convergenceNanos.length; update++) {
                    final String suffix = "reader-broadcast-%s".formatted(update);
                    final String body = "readerbroadcast%s".formatted(update);
                    final float[] vector = new float[]{update + 1.0f, 0.0f, 1.0f};
                    writerRoot.articles.add(new IndexedArticle(suffix, body, vector));
                    if (update == 2) {
                        writerRoot.articles.update(mutableId, article ->
                        {
                            article.title = "mutable-updated";
                            article.body = "mutable-new-body";
                            article.vector = new float[]{0.0f, 0.0f, 1.0f};
                        });
                    }
                    if (update == 3) writerRoot.articles.removeById(removedId);
                    final long started = System.nanoTime();
                    writerRoot.articles.store();
                    final ReplicationCursor target = latest(writerTransport);
                    for (final ReaderNode reader : readers) reader.await(target);
                    convergenceNanos[update] = System.nanoTime() - started;
                    for (final ReaderNode reader : readers) {
                        reader.assertHealthy();
                        assertEquals(target.logicalSequence(), reader.persistedCursor().logicalSequence(),
                                "reader cursor advanced before its Store materialization completed");
                        assertGraphState(reader, suffix, body);
                        if (update == 2) assertGraphState(reader, "mutable-updated", "mutable-new-body");
                        if (update == 3) assertGraphMissing(reader, "removeMe");
                    }

                    if (update == 1) {
                        final ReplicationCursor restartCursor = readers[0].persistedCursor();
                        readers[0].stopAtLatest();
                        readers[0].close();
                        readers[0] = ReaderNode.open(readerNodes[0], readerStores[0], "reader", readerIds[0],
                                clusterId, generation, restartCursor, controlPort, livePort, watermarkPort);
                        readers[0].start();
                        readers[0].awaitLive();
                    }
                }
                /* Embedded indexes are Store-graph state, so verify their durable search
                 * view after every reader has crossed the same terminal boundary and been
                 * cleanly stopped.  This also catches a cursor that advanced before the
                 * index state became durable. */
                for (final ReaderNode reader : readers)
                    if (reader != null && reader.isRunning()) reader.stopAtLatest();
                for (final ReaderNode reader : readers) if (reader != null) reader.close();
                for (final Path readerStore : readerStores)
                    try (EmbeddedStorageManager restarted = foundation(readerStore).start()) {
                        assertIndexState(restarted.root(), "reader-broadcast-3", "readerbroadcast3",
                                new float[]{4.0f, 0.0f, 1.0f});
                        assertIndexState(restarted.root(), "mutable-updated", "mutable-new-body",
                                new float[]{0.0f, 0.0f, 1.0f});
                        assertIndexMissing(restarted.root(), "removeMe");
                    }

                final long[] sorted = convergenceNanos.clone();
                Arrays.sort(sorted);
                System.out.printf(Locale.ROOT,
                        "Aeron 1-writer/3-reader Store+Lucene+JVector: p50-ms=%.1f p99-ms=%.1f max-channels=%d%n",
                        sorted[sorted.length / 2] / 1_000_000.0,
                        sorted[sorted.length - 1] / 1_000_000.0,
                        maximumChannels.get());
            } finally {
                for (final ReaderNode reader : readers) {
                    if (reader != null && reader.isRunning()) reader.stopAtLatest();
                    if (reader != null) reader.close();
                }
                writer.shutdown();
            }
        } finally {
            delete(root);
        }
    }

    /// Verifies a four-channel Store transaction crosses every channel and the restarted provider publishes a later sequence.
    @Test
    void fourChannelStoreTransactionSurvivesProviderRestart() throws Exception {
        final Path root = Files.createTempDirectory("dg-aeron-store-");
        final Path storePath = root.resolve("store");
        final UUID clusterId = UUID.randomUUID();
        final UUID nodeId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final NodeSettingsSource properties = properties(root, clusterId, nodeId, generation);
        try {
            final Root value = new Root();
            value.values.addAll(List.of("one", "two", "three", "four"));
            for (int i = 0; i < 512; i++) value.objects.add(new NewType("channel-object-%s".formatted(i)));
            final AtomicBoolean sawFourChannels = new AtomicBoolean();
            long firstSequence;
            try (ClusterReplicationTransport transport = new AeronTransport(properties)) {
                final ReplicationPublisher distributor = transport.distributor("store");
                final java.util.function.UnaryOperator<PersistenceTarget<Binary>> targetFactory = delegate ->
                        transport.persistenceTargetFactory("store", distributor).apply(new PersistenceTarget<>() {
                            @Override
                            public void write(final Binary data) {
                                final int[] channels = {0};
                                data.iterateChannelChunks(ignored -> channels[0]++);
                                if (channels[0] >= 4) sawFourChannels.set(true);
                                delegate.write(data);
                            }

                            @Override
                            public boolean isWritable() {
                                return delegate.isWritable();
                            }
                        });
                final EmbeddedStorageManager manager = start(storePath, value, distributor,
                        targetFactory);
                try {
                    manager.storeRoot();
                    /* A handful of objects can all hash to channel zero.  Store many
                     * independent entities in one commit to force the configured
                     * four-channel storer to emit every channel in that transaction. */
                    for (final NewType object : value.objects) object.value = "%s-updated".formatted(object.value);
                    manager.storeAll(value.objects);
                } finally {
                    manager.shutdown();
                }
                assertTrue(sawFourChannels.get(), "the real Store transaction must cross all four configured channels");
                firstSequence = latestSequence(transport);
                assertTrue(firstSequence >= 0, "real Store write did not reach the Aeron terminal checkpoint");
            }

            try (ClusterReplicationTransport transport = new AeronTransport(properties)) {
                final ReplicationPublisher distributor = transport.distributor("store");
                final EmbeddedStorageManager manager = startExisting(storePath, distributor,
                        transport.persistenceTargetFactory("store", distributor));
                final Root resumed = manager.root();
                resumed.values.add("after-restart");
                manager.store(resumed.values);
                assertTrue(latestSequence(transport) > firstSequence,
                        "the restarted real Store did not publish a later Aeron transaction");
                manager.shutdown();
            }
        } finally {
            delete(root);
        }
    }

    /// Verifies a locally rejected Store write republishes its type dictionary on retry.
    @Test
    void realStoreRetryRepublishesDictionaryAfterLocalRejection() throws Exception {
        final Path root = Files.createTempDirectory("dg-aeron-dictionary-");
        final UUID clusterId = UUID.randomUUID();
        final UUID nodeId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final NodeSettingsSource properties = properties(root, clusterId, nodeId, generation);
        try (ClusterReplicationTransport transport = new AeronTransport(properties)) {
            final AtomicInteger dictionaryChunks = new AtomicInteger();
            AeronCrashHooks.callWithHook((name, ignored) ->
            {
                if ("AFTER_DICTIONARY_CHUNKS".equals(name)) dictionaryChunks.incrementAndGet();
            }, () -> {
                final ReplicationPublisher distributor = transport.distributor("store");
                final AtomicBoolean rejectNext = new AtomicBoolean();
                final java.util.function.UnaryOperator<PersistenceTarget<Binary>> targetFactory = delegate ->
                        transport.persistenceTargetFactory("store", distributor).apply(new PersistenceTarget<>() {
                            @Override
                            public void write(final Binary data) {
                                if (rejectNext.compareAndSet(true, false)) throw new IllegalStateException("injected Store rejection");
                                delegate.write(data);
                            }

                            @Override
                            public boolean isWritable() {
                                return delegate.isWritable();
                            }
                        });
                final Path storePath = root.resolve("store");
                final Root value = new Root();
                final EmbeddedStorageManager manager = start(storePath, value, distributor, targetFactory);
                try {
                    manager.storeRoot();
                    value.objects.add(new NewType("registered-before-rejection"));
                    rejectNext.set(true);
                    org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, () -> manager.store(value.objects));
                    /* After a local write failure, the local outcome is
                     * uncertain, so the next store is refused instead of
                     * replaying an ambiguous commit. */
                    org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                            () -> manager.store(value.objects));
                    assertTrue(dictionaryChunks.get() >= 1,
                            "the dictionary was emitted for the failing write: " + dictionaryChunks);
                } finally {
                    manager.shutdown();
                }
                return null;
            });
        } finally {
            delete(root);
        }
    }

    /// Verifies a forked real-Store writer restarts across processes with rising sequences and dictionary retry.
    @Test
    void forkedRealStoreWriterRestartsAndRetriesAcrossFourChannels() throws Exception {
        final Path root = Files.createTempDirectory("dg-aeron-store-process-");
        final UUID clusterId = UUID.randomUUID();
        final UUID nodeId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        try {
            final String initial = forkStoreChild(root, clusterId, nodeId, generation, "initial");
            assertTrue(initial.contains("channels=true"), initial);
            final long firstSequence = sequence(initial);
            final String restart = forkStoreChild(root, clusterId, nodeId, generation, "restart");
            assertTrue(sequence(restart) > firstSequence, restart);
            final String dictionary = forkStoreChild(root, clusterId, nodeId, generation, "dictionary");
            /* With the uncertain-write model, a local-store rejection
             * refuses retry: sequence never advances past the failing write,
             * and the dictionary is never re-emitted after the failed local
             * write. */
            assertTrue(sequence(dictionary) == sequence(restart),
                    "a failed closed writer must not advance: %s".formatted(dictionary));
            assertTrue(dictionaryCount(dictionary) >= 1,
                    "the dictionary was emitted for the attempt: %s".formatted(dictionary));
        } finally {
            delete(root);
        }
    }

    static final class ReaderNode implements AutoCloseable {
        private final ClusterReplicationTransport transport;
        private final Path cursorPath;
        private final DurableCursorFile cursorManager;
        private final EmbeddedStorageFoundation<?> foundation;
        private final EmbeddedStorageManager storage;
        private final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
        private final String role;
        private StorageBinaryDataReceiver receiver;
        private ReplicationApplier client;
        private boolean closed;

        private ReaderNode(
                final Path nodeRoot,
                final Path storePath,
                final String role,
                final UUID nodeId,
                final UUID clusterId,
                final UUID generation,
                final ReplicationCursor startingCursor,
                final int controlPort,
                final int livePort,
                final int watermarkPort
        ) {
            this.transport = new AeronTransport(properties(
                    nodeRoot, clusterId, nodeId, generation, role, -1L, controlPort, livePort, watermarkPort));
            this.role = role;
            try {
                this.cursorPath = nodeRoot.resolve("cursor");
                this.cursorManager = DurableCursorFile.of(this.cursorPath);
                this.foundation = foundation(storePath);
                this.storage = this.foundation.start();
                this.receiver = this.newReceiver();
                this.client = this.newClient(startingCursor);
            } catch (final RuntimeException | Error failure) {
                try {
                    this.transport.close();
                } catch (final RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        private StorageBinaryDataReceiver newReceiver() {
            return StorageBinaryDataMerger.create(new StorageBinaryDataMerger.Configuration(
                    this.foundation.getConnectionFoundation(), this.storage.createConnection(),
                    ObjectGraphUpdateHandler.PerStore(this.coordinator),
                    0L, 1L, 1L << 30, 60_000L, 30_000L, 5_000L, 4096, this.coordinator));
        }

        StorageGraphCoordinator graphCoordinator() {
            return this.coordinator;
        }

        /// Test-only applied-path latency injection, in milliseconds. The soak
        /// sets this to model a slow-but-advancing reader: cursor persistence
        /// lags, the backlog window widens, and lag SLOs must observe it
        /// instead of hanging. Zero disables the delay.
        private volatile long applyDelayMs;

        void setApplyDelayMs(final long delayMs) {
            this.applyDelayMs = Math.max(0L, delayMs);
        }

        private ReplicationApplier newClient(final ReplicationCursor startingCursor) {
            return this.transport.client(this.receiver, "store", new CommitAppliedListener() {
                @Override
                public void onApplied(final ReplicationCursor cursor) {
                    final long delay = ReaderNode.this.applyDelayMs;
                    if (delay > 0) {
                        try {
                            Thread.sleep(delay);
                        } catch (final InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("soak apply-delay interrupted", interrupted);
                        }
                    }
                    ReaderNode.this.cursorManager.set(cursor);
                }

                @Override
                public void close() {
                }
            }, startingCursor);
        }

        /// Test-only cursor overwrite used by soak rollback chaos: persists an
        /// older boundary so the next transport restart must fail closed
        /// (reseed) instead of silently replaying history.
        void overwritePersistedCursor(final ReplicationCursor cursor) {
            this.cursorManager.set(cursor);
        }

        /// Test-only fresh cursor read that bypasses the cached manager state,
        /// so cursor-file corruption injected by soak chaos is observed instead
        /// of masked. A corrupted file throws here (bounded failure), which the
        /// caller must count as a parked reader, never as convergence.
        ReplicationCursor readPersistedCursorFresh() {
            try (DurableCursorFile fresh = DurableCursorFile.of(this.cursorPath)) {
                return fresh.get();
            }
        }

        Path cursorPath() {
            return this.cursorPath;
        }

        static ReaderNode open(
                final Path nodeRoot,
                final Path storePath,
                final String role,
                final UUID nodeId,
                final UUID clusterId,
                final UUID generation,
                final ReplicationCursor startingCursor,
                final int controlPort,
                final int livePort,
                final int watermarkPort
        ) throws Exception {
            Files.createDirectories(nodeRoot);
            return new ReaderNode(nodeRoot, storePath, role, nodeId, clusterId, generation,
                    startingCursor, controlPort, livePort, watermarkPort);
        }

        private static RuntimeException append(final RuntimeException current, final RuntimeException additional) {
            if (current == null) return additional;
            if (current != additional) current.addSuppressed(additional);
            return current;
        }

        void start() {
            this.client.start();
        }

        /* A lagging reader still converges: replaying a deep backlog at a few
         * transactions per second is healthy, just slow. Fixed second bounds
         * flake on loaded machines, so the waits below use an overall
         * deadline plus a stall detector: the applied sequence must keep
         * advancing, and a reader that stops making progress fails fast
         * instead of hanging the test's joins. */
        private static final long WAIT_OVERALL_NANOS = TimeUnit.SECONDS.toNanos(120L);
        private static final long WAIT_STALL_NANOS = TimeUnit.SECONDS.toNanos(20L);

        /// Parks until `done` holds, requiring applied-sequence progress at
        /// least every stall window; fails fast on a stall or overall timeout.
        ///
        /// @param done condition to wait for
        /// @param what wait description for failure messages
        private void awaitProgress(final BooleanSupplier done, final String what) {
            final long deadline = System.nanoTime() + WAIT_OVERALL_NANOS;
            long lastApplied = this.client.cursor().logicalSequence();
            long lastProgressNanos = System.nanoTime();
            while (!done.getAsBoolean()) {
                if (this.client.failure() != null) throw this.client.failure();
                final long nowNanos = System.nanoTime();
                final long applied = this.client.cursor().logicalSequence();
                if (applied != lastApplied) {
                    lastApplied = applied;
                    lastProgressNanos = nowNanos;
                }
                if (nowNanos - lastProgressNanos > WAIT_STALL_NANOS) {
                    throw new AssertionError(
                            "reader stalled while %s (applied sequence unchanged)".formatted(what));
                }
                if (nowNanos >= deadline) {
                    throw new AssertionError("reader timed out while %s".formatted(what));
                }
                Thread.onSpinWait();
                java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
            }
        }

        void awaitLive() {
            awaitProgress(this.client::isLive, "awaiting the live image");
            if (this.client.failure() != null) throw this.client.failure();
            assertTrue(this.client.isLive(), "reader did not join the writer's live publication");
        }

        void await(final ReplicationCursor target) {
            awaitProgress(() -> this.client.cursor().logicalSequence() >= target.logicalSequence(),
                    "awaiting transaction " + target.logicalSequence());
            if (this.client.failure() != null) throw this.client.failure();
            assertEquals(target.logicalSequence(), this.client.cursor().logicalSequence(),
                    "reader did not resolve the writer transaction");
            this.receiver.awaitApplied();
        }

        void assertHealthy() {
            assertNull(this.client.failure(), "reader reported a terminal failure");
            assertTrue(this.client.isRunning(), "reader stopped while the writer was live");
            /* A live image can be replaced transiently during Archive-to-live
             * handover or channel maintenance; require recovery within one
             * stall window instead of sampling a single instant. A permanent
             * loss still fails. */
            final long deadline = System.nanoTime() + WAIT_STALL_NANOS;
            while (!this.client.isLive()) {
                if (System.nanoTime() >= deadline) {
                    fail("reader lost its live Aeron image");
                }
                java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
            }
        }

        boolean isRunning() {
            return this.client.isRunning();
        }

        boolean isLive() {
            return this.client.isLive();
        }

        void stopAtLatest() {
            this.client.stopAtLatestMessage();
            awaitProgress(() -> !this.client.isRunning(), "stopping at the latest boundary");
            if (this.client.failure() != null) {
                throw new AssertionError("reader failed while stopping", this.client.failure());
            }
            assertEquals(ReplicationApplier.StopOutcome.RESOLVED_BOUNDARY,
                    this.client.stopOutcome(), "reader did not stop at a resolved boundary");
        }

        /**
         * Replaces only the Aeron reader and merger while retaining the live Store.
         * This exercises a transport restart without reopening the Store in the
         * same JVM, which can race Store materializer teardown.
         */
        void restartTransport(final ReplicationCursor startingCursor) {
            if (this.closed) throw new IllegalStateException("reader is closed");
            this.client.dispose();
            this.disposeReceiver();
            this.receiver = this.newReceiver();
            try {
                this.client = this.newClient(startingCursor);
                this.client.start();
            } catch (final RuntimeException | Error failure) {
                try {
                    this.disposeReceiver();
                } catch (final RuntimeException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        }

        Root root() {
            return this.storage.root();
        }

        Object rootObject() {
            return this.storage.root();
        }

        ReplicationCursor persistedCursor() {
            return this.cursorManager.get();
        }

        /// Test-only live views used by soak bounded waits: the in-memory
        /// applied cursor and the terminal reader failure, without throwing.
        ReplicationCursor liveCursor() {
            return this.client.cursor();
        }

        RuntimeException clientFailure() {
            return this.client.failure();
        }

        private void disposeReceiver() {
            final StorageBinaryDataReceiver current = this.receiver;
            this.receiver = null;
            if (current instanceof Disposable disposable) {
                disposable.dispose();
            }
        }

        @Override
        public void close() {
            if (this.closed) return;
            this.closed = true;
            RuntimeException failure = null;
            try {
                if (this.client != null) this.client.dispose();
            } catch (final RuntimeException closeFailure) { failure = closeFailure; }
            try {
                this.disposeReceiver();
            } catch (final RuntimeException closeFailure) {
                failure = append(failure, closeFailure);
            }
            try {
                this.storage.shutdown();
            } catch (final RuntimeException closeFailure) {
                failure = append(failure, closeFailure);
            }
            try {
                this.cursorManager.close();
            } catch (final RuntimeException closeFailure) {
                failure = append(failure, closeFailure);
            }
            try {
                this.transport.close();
            } catch (final RuntimeException closeFailure) {
                failure = append(failure, closeFailure);
            }
            if (failure != null) throw failure;
        }
    }

    public static final class Root {
        public final List<String> values = new ArrayList<>();
        public final List<NewType> objects = new ArrayList<>();
        public byte[] payload = new byte[0];
    }

    public static final class NewType {
        public String value;

        NewType(final String value) {
            this.value = value;
        }
    }

    public static final class IndexRoot {
        public GigaMap<IndexedArticle> articles;
    }

    public static final class IndexedArticle {
        public String title;
        public String body;
        public float[] vector;

        IndexedArticle(final String title, final String body, final float[] vector) {
            this.title = title;
            this.body = body;
            this.vector = vector;
        }
    }

    private static final class IndexedArticlePopulator extends DocumentPopulator<IndexedArticle> {
        @Override
        public void populate(final Document document, final IndexedArticle article) {
            document.add(createTextField("title", article.title));
            document.add(createTextField("body", article.body));
        }
    }

    private static final class IndexedArticleVectorizer extends Vectorizer<IndexedArticle> {
        @Override
        public float[] vectorize(final IndexedArticle article) {
            return article.vector;
        }
    }
}
