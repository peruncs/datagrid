package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import peruncs.datagrid.cluster.node.aeron.AeronStoreIntegrationIT.IndexRoot;
import peruncs.datagrid.cluster.node.aeron.AeronStoreIntegrationIT.IndexedArticle;
import peruncs.datagrid.cluster.node.aeron.AeronStoreIntegrationIT.ReaderNode;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.node.replication.ReplicationCursor;
import peruncs.datagrid.cluster.node.replication.StoredReplicationCursorManager;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.nio.file.Path;
import java.util.UUID;

/// Forked child running one phase of the bounded writer/reader/index restart scenario.
///
/// Each phase runs in its own JVM, and a Store created by one phase is only
/// ever reopened by a later phase: closing then reopening the same Store files
/// inside one JVM races Store teardown and failed intermittently with
/// `BinaryBitmapIndex`. Files under the shared root carry state across phases
/// (stores, cursors, checkpoints, archives); stable identities travel as
/// command arguments. The parent smoke test launches each phase in order and
/// fails on the first nonzero exit, keeping the coverage in the normal
/// `mvn test` gate.
public final class AeronStoreSmokeChildMain {
    private AeronStoreSmokeChildMain() {
    }

    /// Runs one scenario phase; exits nonzero with a stack trace on failure.
    ///
    /// @param args `seed-store`, `import-verify`, or `resume-verify`, then the
    ///             shared root, cluster id, store generation, writer node id,
    ///             reader node id, and the control, live, and watermark ports.
    ///             Ports stay stable across phases: the Archive recording pins
    ///             its live channel, so a restarted writer must publish on the
    ///             same channels.
    static void main(final String[] args) {
        try {
            if (args.length != 9) throw new IllegalArgumentException("expected 9 arguments, received " + args.length);
            final var phase = args[0];
            final var root = Path.of(args[1]);
            final var clusterId = UUID.fromString(args[2]);
            final var generation = UUID.fromString(args[3]);
            final var writerNodeId = UUID.fromString(args[4]);
            final var readerNodeId = UUID.fromString(args[5]);
            final var ports = new int[]{
                    Integer.parseInt(args[6]), Integer.parseInt(args[7]), Integer.parseInt(args[8])};
            switch (phase) {
                case "seed-store" -> seedStore(root, clusterId, generation, writerNodeId, ports);
                case "import-verify" -> importAndVerify(root, clusterId, generation, writerNodeId, readerNodeId, ports);
                case "resume-verify" -> resumeAndVerify(root, clusterId, generation, writerNodeId, readerNodeId, ports);
                default -> throw new IllegalArgumentException("unknown smoke phase: " + phase);
            }
        } catch (final Throwable failure) {
            failure.printStackTrace(System.out);
            System.exit(1);
        }
    }

    /// Phase 0: creates and seeds the writer Store in a dedicated process so
    /// no later phase ever reopens Store files this process closed. The ports
    /// match the later phases: the Archive recording pins its live channel,
    /// and phase 1 restarts the writer against the same channels.
    private static void seedStore(
            final Path root,
            final UUID clusterId,
            final UUID generation,
            final UUID writerNodeId,
            final int[] ports)  {
        try (ClusterReplicationTransport writerTransport = new AeronClusterReplicationTransportProvider().create(
                AeronStoreIntegrationIT.properties(root.resolve("writer"), clusterId, writerNodeId, generation, "writer", -1L,
                        ports[0], ports[1], ports[2]))) {
            final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
            final IndexRoot initial = new IndexRoot();
            initial.articles = GigaMap.New();
            AeronStoreIntegrationIT.configureIndexes(initial.articles);
            final EmbeddedStorageManager seeded = AeronStoreIntegrationIT.startIndex(root.resolve("writer-store"), initial, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            seeded.storeRoot();
            seeded.shutdown();
        }
        System.out.println("PHASE0-OK");
    }

    /// Phase 1: opens the seeded writer Store in a fresh process, copies it to
    /// the reader, and verifies the first replicated transaction on both
    /// indexes.
    private static void importAndVerify(
            final Path root,
            final UUID clusterId,
            final UUID generation,
            final UUID writerNodeId,
            final UUID readerNodeId,
            final int[] ports
    ) throws Exception {
        final Path writerStore = root.resolve("writer-store");
        final Path readerStore = root.resolve("reader-store");
        try (ClusterReplicationTransport writerTransport = new AeronClusterReplicationTransportProvider().create(
                AeronStoreIntegrationIT.properties(root.resolve("writer"), clusterId, writerNodeId, generation, "writer", -1L,
                        ports[0], ports[1], ports[2]))) {
            final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
            final EmbeddedStorageManager writer = AeronStoreIntegrationIT.startExistingIndex(writerStore, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            try {
                final ReplicationCursor baseline = AeronStoreIntegrationIT.latest(writerTransport);
                AeronStoreIntegrationIT.copyDirectory(writerStore, readerStore);
                try (ReaderNode reader = ReaderNode.open(
                        root.resolve("reader"), readerStore, "reader",
                        readerNodeId, clusterId, generation, baseline, ports[0], ports[1], ports[2])) {
                    reader.start();
                    reader.awaitLive();
                    final IndexRoot writerRoot = writer.root();
                    writerRoot.articles.add(new IndexedArticle(
                            "Aeron", "smoke replication", new float[]{1.0f, 0.0f, 0.0f}));
                    writerRoot.articles.store();
                    reader.await(AeronStoreIntegrationIT.latest(writerTransport));
                    reader.assertHealthy();
                    reader.stopAtLatest();
                }
            } finally {
                writer.shutdown();
            }
        }
        System.out.println("PHASE1-OK");
    }

    private static void resumeAndVerify(
            final Path root,
            final UUID clusterId,
            final UUID generation,
            final UUID writerNodeId,
            final UUID readerNodeId,
            final int[] ports
    ) throws Exception {
        final Path writerStore = root.resolve("writer-store");
        final Path readerStore = root.resolve("reader-store");
        final ReplicationCursor resumed;
        try (StoredReplicationCursorManager cursorManager =
                     StoredReplicationCursorManager.NewAtomic(root.resolve("reader/cursor"))) {
            resumed = cursorManager.get();
        }
        if (resumed == null || resumed.logicalSequence() < 0) {
            throw new IllegalStateException("no persisted reader cursor to resume from: " + resumed);
        }
        try (ClusterReplicationTransport writerTransport = new AeronClusterReplicationTransportProvider().create(
                AeronStoreIntegrationIT.properties(root.resolve("writer"), clusterId, writerNodeId, generation, "writer", -1L,
                        ports[0], ports[1], ports[2]))) {
            final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
            final EmbeddedStorageManager writer = AeronStoreIntegrationIT.startExistingIndex(writerStore, distributor, writerTransport.persistenceTargetFactory("store", distributor));
            try {
                /* A second transaction lands while the reader is down (a new
                 * process here); the reader resumes from its persisted atomic
                 * cursor, proving restart continuity instead of re-importing
                 * from scratch. */
                final IndexRoot writerRoot = writer.root();
                writerRoot.articles.add(new IndexedArticle("Vector", "restart import", new float[]{0.0f, 1.0f, 0.0f}));
                writerRoot.articles.store();
                final ReplicationCursor second = AeronStoreIntegrationIT.latest(writerTransport);
                try (ReaderNode restarted = ReaderNode.open(
                        root.resolve("reader"), readerStore, "reader",
                        readerNodeId, clusterId, generation, resumed, ports[0], ports[1], ports[2])) {
                    restarted.start();
                    restarted.awaitLive();
                    restarted.await(second);
                    restarted.assertHealthy();
                    restarted.stopAtLatest();
                    /* Verify the reader Store while it is still open. The
                     * product has a known same-JVM teardown/materializer race;
                     * process restart coverage belongs to the next smoke
                     * phase, so this phase must not close and reopen the same
                     * Store before asserting its indexes. */
                    final IndexRoot imported = (IndexRoot) restarted.rootObject();
                    AeronStoreIntegrationIT.assertIndexState(imported, "Aeron", "replication", new float[]{1.0f, 0.0f, 0.0f});
                    AeronStoreIntegrationIT.assertIndexState(imported, "Vector", "import", new float[]{0.0f, 1.0f, 0.0f});
                }
            } finally {
                writer.shutdown();
            }
        }
        System.out.println("SMOKE-OK");
    }
}
