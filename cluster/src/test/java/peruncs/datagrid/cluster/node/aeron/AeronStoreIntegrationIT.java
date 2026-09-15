package peruncs.datagrid.cluster.node.aeron;

import org.apache.lucene.document.Document;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
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
import peruncs.datagrid.cluster.node.NodelibraryPropertiesProvider;
import peruncs.datagrid.cluster.node.replication.*;
import peruncs.datagrid.cluster.storage.index.ClusterStoreIndexes;
import peruncs.datagrid.cluster.storage.types.DistributedStorage;
import peruncs.datagrid.cluster.storage.types.ObjectGraphUpdateHandler;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Exercises the Aeron target with a real four-channel Embedded Store across a restart.
class AeronStoreIntegrationIT {
    private static ReplicationCursor latest(final ClusterReplicationTransport transport) throws Exception {
        final ReplicationPositionProvider positionProvider = transport.positionProvider("store");
        positionProvider.init();
        return positionProvider.latest();
    }

    private static long latestSequence(final ClusterReplicationTransport transport) throws Exception {
        final ReplicationPositionProvider positionProvider = transport.positionProvider("store");
        positionProvider.init();
        return positionProvider.latestSequence();
    }

    private static void configureIndexes(final GigaMap<IndexedArticle> articles) {
        ClusterStoreIndexes.registerLucene(articles, new IndexedArticlePopulator());
        final VectorIndices<IndexedArticle> vectors = articles.index().register(VectorIndices.Category());
        ClusterStoreIndexes.addVector(vectors, "articles", VectorIndexConfiguration.builder()
                .dimension(3).similarityFunction(VectorSimilarityFunction.COSINE).build(), new IndexedArticleVectorizer());
    }

    @SuppressWarnings("unchecked") // Lucene's class token cannot retain its entity type.
    private static LuceneIndex<IndexedArticle> luceneIndex(final GigaMap<IndexedArticle> articles) {
        return articles.index().get(LuceneIndex.class);
    }

    private static EmbeddedStorageManager startIndex(
            final Path path,
            final IndexRoot root,
            final StorageBinaryDataDistributor distributor,
            final java.util.function.UnaryOperator<PersistenceTarget<Binary>> targetFactory) {
        final EmbeddedStorageFoundation<?> foundation = foundation(path);
        DistributedStorage.configureWriting(foundation, distributor, targetFactory);
        return foundation.start(root);
    }

    private static EmbeddedStorageManager startExistingIndex(
            final Path path,
            final StorageBinaryDataDistributor distributor,
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
            final String retentionSecret,
            final Set<UUID> retentionReaders,
            final String expectedValue,
            final boolean expectDictionary
    ) throws Exception {
        Files.createDirectories(readerRoot);
        final Path cursorPath = readerRoot.resolve("cursor");
        try (ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider().create(
                properties(readerRoot, clusterId, nodeId, generation, role, -1L,
                        controlPort, livePort, watermarkPort, retentionSecret, retentionReaders));
             StoredReplicationCursorManager cursorManager = StoredReplicationCursorManager.NewAtomic(cursorPath)) {
            final EmbeddedStorageFoundation<?> readerFoundation = foundation(storePath);
            final EmbeddedStorageManager reader = readerFoundation.start();
            final ClusterStorageBinaryDataMerger merger = ClusterStorageBinaryDataMerger.New(
                    readerFoundation.getConnectionFoundation(), reader.createConnection(),
                    ObjectGraphUpdateHandler.Synchronized(), 0L, 1L);
            final ClusterStorageBinaryDataPacketAcceptor acceptor = ClusterStorageBinaryDataPacketAcceptor.New(merger);
            final ClusterStorageBinaryDataClient client = transport.client(acceptor, "store", new AfterDataMessageConsumedListener() {
                        @Override
                        public void onApplied(final ReplicationCursor cursor) {
                            cursorManager.set(cursor);
                        }

                        @Override
                        public void close() {
                        }
                    },
                    startingCursor, "backup-reader".equals(role));
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
                acceptor.awaitApplied();
                client.stopAtLatestMessage();
                final long stopDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (client.isRunning() && System.nanoTime() < stopDeadline) {
                    java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
                }
                assertEquals(StorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY,
                        client.stopOutcome(), "%s did not stop at a resolved transaction boundary".formatted(role));
            } finally {
                client.dispose();
                acceptor.dispose();
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
                assertEquals("dictionary-update", imported.objects.get(0).value,
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

    private static void assertIndexState(
            final IndexRoot imported, final String title, final String body, final float[] vector) {
        assertNotNull(imported.articles, "reader Store root lost its indexed GigaMap");
        final LuceneIndex<IndexedArticle> text = luceneIndex(imported.articles);
        assertEquals(1, text.query("body:%s".formatted(body)).size(),
                "reader Lucene index missed %s (articles=%s)".formatted(body, imported.articles.size()));
        final VectorIndices<IndexedArticle> vectors = imported.articles.index().get(VectorIndices.Category());
        final VectorSearchResult<IndexedArticle> nearest = vectors.get("articles").search(vector, 1);
        assertEquals(1, nearest.size(), "reader JVector index missed %s".formatted(body));
        assertEquals(title, nearest.toList().get(0).entity().title);
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
        final Process child = new ProcessBuilder(java, "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
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
            final StorageBinaryDataDistributor distributor,
            final java.util.function.UnaryOperator<PersistenceTarget<Binary>> targetFactory
    ) {
        final EmbeddedStorageFoundation<?> foundation = foundation(path);
        DistributedStorage.configureWriting(foundation, distributor, targetFactory);
        return foundation.start(root);
    }

    private static EmbeddedStorageManager startExisting(
            final Path path,
            final StorageBinaryDataDistributor distributor,
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

    static NodelibraryPropertiesProvider properties(
            final Path root, final UUID clusterId, final UUID nodeId, final UUID generation) {
        return properties(root, clusterId, nodeId, generation, "writer", -1L, 40124, 40123, 40125);
    }

    static NodelibraryPropertiesProvider properties(
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
                controlPort, livePort, watermarkPort, null, Set.of());
    }

    static NodelibraryPropertiesProvider properties(
            final Path root,
            final UUID clusterId,
            final UUID nodeId,
            final UUID generation,
            final String role,
            final long recordingId,
            final int controlPort,
            final int livePort,
            final int watermarkPort,
            final String retentionSecret,
            final Set<UUID> retentionReaders
    ) {
        return new NodelibraryPropertiesProvider.Env() {
            @Override
            public String replicationRole() {
                return role;
            }

            @Override
            public boolean replicationRoleConfigured() {
                return true;
            }

            @Override
            public String replicationProperty(final String name) {
                return switch (name) {
                    case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID" -> clusterId.toString();
                    case "ECLIPSE_DATAGRID_AERON_NODE_ID" -> nodeId.toString();
                    case "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> generation.toString();
                    case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> root.resolve("driver").toString();
                    case "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY" -> root.resolve("archive").toString();
                    case "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH" -> root.resolve("checkpoint/writer.checkpoint").toString();
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
                    case "ECLIPSE_DATAGRID_AERON_WATERMARK_CHANNEL" -> "aeron:udp?endpoint=localhost:%s".formatted(watermarkPort);
                    case "ECLIPSE_DATAGRID_AERON_RETENTION_SECRET" -> retentionSecret;
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
        try (ClusterReplicationTransport writerTransport = new AeronClusterReplicationTransportProvider().create(
                properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
                        controlPort, livePort, watermarkPort))) {
            final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
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
                    assertEquals("Aeron", nearest.toList().get(0).entity().title);
                }
            } finally {
                writer.shutdown();
            }
        } finally {
            delete(root);
        }
    }

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
        final String retentionSecret = Base64.getEncoder().encodeToString(
                "store-integration-retention-secret".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        final Path writerStore = root.resolve("writer-store");
        final Path ordinaryStore = root.resolve("ordinary-store");
        final Path backupStore = root.resolve("backup-store");
        try (ClusterReplicationTransport writerTransport = new AeronClusterReplicationTransportProvider().create(
                properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
                        controlPort, livePort, watermarkPort, retentionSecret, retentionReaders))) {
            final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
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
                    retentionSecret, retentionReaders, "ordinary-update", true);

            writerRoot.values.add("restart-update");
            writer.store(writerRoot.values);
            final ReplicationCursor secondTarget = latest(writerTransport);
            final Path ordinaryCursor = root.resolve("ordinary-reader/cursor");
            final ReplicationCursor persisted;
            try (StoredReplicationCursorManager cursorManager = StoredReplicationCursorManager.NewAtomic(ordinaryCursor)) {
                persisted = cursorManager.get();
            }
            replicateAndVerify(root.resolve("ordinary-reader"), ordinaryStore, "reader", ordinaryReaderId,
                    clusterId, generation, persisted, secondTarget, controlPort, livePort, watermarkPort,
                    retentionSecret, retentionReaders, "restart-update", false);

            replicateAndVerify(root.resolve("backup-reader"), backupStore, "backup-reader", backupReaderId,
                    clusterId, generation, baseline, secondTarget, controlPort, livePort, watermarkPort,
                    retentionSecret, retentionReaders, "restart-update", true);
            final long watermarkDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!writerTransport.retention().isSupported() && System.nanoTime() < watermarkDeadline) {
                java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
            }
            assertTrue(writerTransport.retention().isSupported(),
                    "writer did not durably assemble the ordinary+backup reader watermark quorum");
            assertEquals(
                    peruncs.datagrid.cluster.node.replication.ReplicationLogRetention.MaintenanceResult.Status.DELETED,
                    writerTransport.retention().deleteThrough(secondTarget).status(),
                    "authenticated quorum must permit online purge at a complete segment boundary");
            final ReplicationCursor postRetentionStart;
            try (StoredReplicationCursorManager cursorManager = StoredReplicationCursorManager.NewAtomic(ordinaryCursor)) {
                postRetentionStart = cursorManager.get();
            }
            writerRoot.values.add("post-retention-update");
            writer.store(writerRoot.values);
            final ReplicationCursor postRetentionTarget = latest(writerTransport);
            replicateAndVerify(root.resolve("ordinary-reader"), ordinaryStore, "reader", ordinaryReaderId,
                    clusterId, generation, postRetentionStart, postRetentionTarget, controlPort, livePort, watermarkPort,
                    retentionSecret, retentionReaders, "post-retention-update", false);
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
        try (ClusterReplicationTransport writerTransport = new AeronClusterReplicationTransportProvider().create(
                properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
                        controlPort, livePort, watermarkPort))) {
            final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
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
                assertEquals("concurrent-dictionary", ordinary.root().objects.get(0).value);
                assertEquals("concurrent-dictionary", backup.root().objects.get(0).value);
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

        try (ClusterReplicationTransport writerTransport = new AeronClusterReplicationTransportProvider().create(
                properties(root.resolve("writer"), clusterId, UUID.randomUUID(), generation, "writer", -1L,
                        controlPort, livePort, watermarkPort))) {
            final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
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

                final IndexRoot writerRoot = (IndexRoot) writer.root();
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

    @Test
    void fourChannelStoreTransactionSurvivesProviderRestart() throws Exception {
        final Path root = Files.createTempDirectory("dg-aeron-store-");
        final Path storePath = root.resolve("store");
        final UUID clusterId = UUID.randomUUID();
        final UUID nodeId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final NodelibraryPropertiesProvider properties = properties(root, clusterId, nodeId, generation);
        try {
            final Root value = new Root();
            value.values.addAll(List.of("one", "two", "three", "four"));
            for (int i = 0; i < 512; i++) value.objects.add(new NewType("channel-object-%s".formatted(i)));
            final AtomicBoolean sawFourChannels = new AtomicBoolean();
            long firstSequence;
            try (ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider().create(properties)) {
                final StorageBinaryDataDistributor distributor = transport.distributor("store", false);
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

            try (ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider().create(properties)) {
                final StorageBinaryDataDistributor distributor = transport.distributor("store", false);
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

    @Test
    void realStoreRetryRepublishesDictionaryAfterLocalRejection() throws Exception {
        final Path root = Files.createTempDirectory("dg-aeron-dictionary-");
        final UUID clusterId = UUID.randomUUID();
        final UUID nodeId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final NodelibraryPropertiesProvider properties = properties(root, clusterId, nodeId, generation);
        try (ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider().create(properties)) {
            final AtomicInteger dictionaryChunks = new AtomicInteger();
            AeronCrashHooks.install((name, ignored) ->
            {
                if ("AFTER_DICTIONARY_CHUNKS".equals(name)) dictionaryChunks.incrementAndGet();
            });
            try {
                final StorageBinaryDataDistributor distributor = transport.distributor("store", false);
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
                    manager.store(value.objects);
                    assertTrue(dictionaryChunks.get() >= 2,
                            "the new type dictionary must be published again on retry");
                } finally {
                    manager.shutdown();
                }
            } finally {
                AeronCrashHooks.clear();
            }
        } finally {
            delete(root);
        }
    }

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
            assertTrue(sequence(dictionary) > sequence(restart), dictionary);
            assertTrue(dictionaryCount(dictionary) >= 2,
                    "a rejected real-Store write must resend its dictionary on retry: %s".formatted(dictionary));
        } finally {
            delete(root);
        }
    }

    private static final class ReaderNode implements AutoCloseable {
        private final ClusterReplicationTransport transport;
        private final StoredReplicationCursorManager cursorManager;
        private final EmbeddedStorageManager storage;
        private final ClusterStorageBinaryDataPacketAcceptor acceptor;
        private final ClusterStorageBinaryDataClient client;
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
            this.transport = new AeronClusterReplicationTransportProvider().create(properties(
                    nodeRoot, clusterId, nodeId, generation, role, -1L, controlPort, livePort, watermarkPort));
            try {
                this.cursorManager = StoredReplicationCursorManager.NewAtomic(nodeRoot.resolve("cursor"));
                final EmbeddedStorageFoundation<?> foundation = foundation(storePath);
                this.storage = foundation.start();
                final ClusterStorageBinaryDataMerger merger = ClusterStorageBinaryDataMerger.New(
                        foundation.getConnectionFoundation(), this.storage.createConnection(),
                        ObjectGraphUpdateHandler.Synchronized(), 0L, 1L);
                this.acceptor = ClusterStorageBinaryDataPacketAcceptor.New(merger);
                this.client = this.transport.client(this.acceptor, "store", new AfterDataMessageConsumedListener() {
                    @Override
                    public void onApplied(final ReplicationCursor cursor) {
                        ReaderNode.this.cursorManager.set(cursor);
                    }

                    @Override
                    public void close() {
                    }
                }, startingCursor, "backup-reader".equals(role));
            } catch (final RuntimeException | Error failure) {
                try {
                    this.transport.close();
                } catch (final RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
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

        void awaitLive() {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
            while (!this.client.isLive() && this.client.failure() == null && System.nanoTime() < deadline) {
                Thread.onSpinWait();
                java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
            }
            if (this.client.failure() != null) throw this.client.failure();
            assertTrue(this.client.isLive(), "reader did not join the writer's live publication");
        }

        void await(final ReplicationCursor target) {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
            while (this.client.cursor().logicalSequence() < target.logicalSequence() &&
                   this.client.failure() == null && System.nanoTime() < deadline) {
                Thread.onSpinWait();
                java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
            }
            if (this.client.failure() != null) throw this.client.failure();
            assertEquals(target.logicalSequence(), this.client.cursor().logicalSequence(),
                    "reader did not resolve the writer transaction");
            this.acceptor.awaitApplied();
        }

        void assertHealthy() {
            assertNull(this.client.failure(), "reader reported a terminal failure");
            assertTrue(this.client.isRunning(), "reader stopped while the writer was live");
            assertTrue(this.client.isLive(), "reader lost its live Aeron image");
        }

        boolean isRunning() {
            return this.client.isRunning();
        }

        void stopAtLatest() {
            this.client.stopAtLatestMessage();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
            while (this.client.isRunning() && System.nanoTime() < deadline) {
                java.util.concurrent.locks.LockSupport.parkNanos(100_000L);
            }
            if (this.client.failure() != null) {
                throw new AssertionError("reader failed while stopping", this.client.failure());
            }
            assertEquals(StorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY,
                    this.client.stopOutcome(), "reader did not stop at a resolved boundary");
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

        @Override
        public void close() {
            if (this.closed) return;
            this.closed = true;
            RuntimeException failure = null;
            try {
                this.client.dispose();
            } catch (final RuntimeException closeFailure) {
                failure = closeFailure;
            }
            try {
                this.acceptor.dispose();
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
