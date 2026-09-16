package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import peruncs.datagrid.cluster.node.aeron.AeronStoreIntegrationIT.*;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.node.replication.ReplicationCursor;
import peruncs.datagrid.cluster.node.replication.StoredReplicationCursorManager;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.nio.file.Path;
import java.util.UUID;

/// Forked child running one phase of the bounded writer/reader/index restart scenario.
///
/// Each phase runs in its own JVM: several Stores, drivers, and archives in
/// one process must not share a fork with the classpath suites, and closing
/// then reopening the same Store files inside one JVM races Store teardown.
/// Files under the shared root carry state across phases (stores, cursors,
/// checkpoints, archives); stable identities travel as command arguments.
/// The parent smoke test launches each phase in order and fails on the first
/// nonzero exit, keeping the coverage in the normal `mvn test` gate.
public final class AeronStoreSmokeChildMain {
    private AeronStoreSmokeChildMain() {
    }

    /// Runs one scenario phase; exits nonzero with a stack trace on failure.
    ///
    /// @param args `seed-import` or `resume-verify`, then the shared root,
    ///             cluster id, store generation, writer node id, reader node id,
    ///             and the control, live, and watermark ports. Ports stay stable
    ///             across phases: the Archive recording pins its live channel,
    ///             so a restarted writer must publish on the same channels.
    public static void main(final String[] args) {
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
                case "seed-import" -> seedAndImport(root, clusterId, generation, writerNodeId, readerNodeId, ports);
                case "resume-verify" -> resumeAndVerify(root, clusterId, generation, writerNodeId, readerNodeId, ports);
                default -> throw new IllegalArgumentException("unknown smoke phase: " + phase);
            }
        } catch (final Throwable failure) {
            failure.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private static void seedAndImport(
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
                properties(root.resolve("writer"), clusterId, writerNodeId, generation, "writer", -1L,
                        ports[0], ports[1], ports[2]))) {
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
            try {
                try (ReaderNode reader = ReaderNode.open(
                        root.resolve("reader"), readerStore, "reader",
                        readerNodeId, clusterId, generation, baseline, ports[0], ports[1], ports[2])) {
                    reader.start();
                    reader.awaitLive();
                    final IndexRoot writerRoot = writer.root();
                    writerRoot.articles.add(new IndexedArticle(
                            "Aeron", "smoke replication", new float[]{1.0f, 0.0f, 0.0f}));
                    writerRoot.articles.store();
                    reader.await(latest(writerTransport));
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
                properties(root.resolve("writer"), clusterId, writerNodeId, generation, "writer", -1L,
                        ports[0], ports[1], ports[2]))) {
            final StorageBinaryDataDistributor distributor = writerTransport.distributor("store", false);
            final EmbeddedStorageManager writer = startExistingIndex(writerStore, distributor,
                    writerTransport.persistenceTargetFactory("store", distributor));
            try {
                /* A second transaction lands while the reader is down (a new
                 * process here); the reader resumes from its persisted atomic
                 * cursor, proving restart continuity instead of re-importing
                 * from scratch. */
                final IndexRoot writerRoot = writer.root();
                writerRoot.articles.add(new IndexedArticle(
                        "Vector", "restart import", new float[]{0.0f, 1.0f, 0.0f}));
                writerRoot.articles.store();
                final ReplicationCursor second = latest(writerTransport);
                try (ReaderNode restarted = ReaderNode.open(
                        root.resolve("reader"), readerStore, "reader",
                        readerNodeId, clusterId, generation, resumed, ports[0], ports[1], ports[2])) {
                    restarted.start();
                    restarted.awaitLive();
                    restarted.await(second);
                    restarted.assertHealthy();
                    restarted.stopAtLatest();
                }
            } finally {
                writer.shutdown();
            }
        }
        /* Both indexes answer after the reader Store itself restarts. */
        try (EmbeddedStorageManager reopened = foundation(readerStore).start()) {
            final IndexRoot imported = reopened.root();
            assertIndexState(imported, "Aeron", "replication", new float[]{1.0f, 0.0f, 0.0f});
            assertIndexState(imported, "Vector", "import", new float[]{0.0f, 1.0f, 0.0f});
        }
        System.out.println("SMOKE-OK");
    }
}
