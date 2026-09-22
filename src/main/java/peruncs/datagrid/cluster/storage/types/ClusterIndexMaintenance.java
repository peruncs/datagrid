package peruncs.datagrid.cluster.storage.types;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import org.eclipse.serializer.exceptions.IORuntimeException;
import org.eclipse.store.gigamap.jvector.VectorIndex;
import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.gigamap.types.IndexGroup;
import org.eclipse.store.storage.types.StorageConnection;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

/// Reader-side index maintenance for replicated Store imports.
///
/// Imports materialize entities without calling the map's add/update/remove
/// API, so no index group observes the change and both search views freeze at
/// whatever the first query built. This class retires the Lucene view before
/// materialization and validates plus rebuilds the vector views after it, as
/// documented on [ClusterStoreIndexes].
final class ClusterIndexMaintenance {
    private ClusterIndexMaintenance() {
    }

        /// Reader-side maintenance entry: refreshes every replicated search view
        /// reachable from this Store's roots before a replicated batch is applied.
    ///
    /// Imports materialize entities without calling the map's add/update/remove
    /// API, so no index group observes the change and both search views freeze
    /// at whatever the first query built: Lucene's cached near-real-time reader
    /// reopens only when a write-path mutation marks it stale, and JVector's
    /// transient graph rebuilds exactly once after load. The chaos soak proved
    /// a commit-only refresh insufficient: the graph converged with zero misses
    /// while both indexes missed 327 of 370 live articles on every reader.
    ///
    /// The refresh is two-tracked because the two indexes replicate
    /// differently. Lucene's complete directory content ships inside the Store
    /// (graph directory committed at the writer's `store()` boundary), so the
    /// reader must only retire its cached handles: the next query reopens over
    /// the already-current files. A reader-side rollback or rebuild is not
    /// just unnecessary here, it is corrupting: rollback deletes the
    /// replicated commit point the writer never created, and reopening over
    /// the commit-less remainder wipes the rest. The retired writer is
    /// closed, never kept open across batches, so no file deleter ever spans
    /// an import swap. JVector instead keeps only vectors in the
    /// Store with a transient search graph, so the refresh resets that graph
    /// to its just-loaded state: the next access rebuilds it from the
    /// already-current vector store — the same lazy rebuild every restart
    /// performs, with no vectorize call and no per-entity graph surgery.
    ///
    /// Everything runs under the map monitor, the same lock queries use, and
    /// the merger holds one coordinator write section across retirement,
    /// materialization, and validation, so joined application reads observe
    /// either the pre-batch or the post-batch boundary — never a materialized
    /// graph with stale search views. Retirement precedes the swap: the
    /// merger calls this before materializing, so every close still observes
    /// the directory state its handles reference. Callers invoke this only
    /// for non-empty batches.
    ///
    /// @param storage             storage connection owning the materialized graph
    /// @param maxValidatedObjects object bound for the discovery scan
    static void refreshImportedIndexes(final StorageConnection storage, final int maxValidatedObjects) {
        Objects.requireNonNull(storage, "storage");
        final ClusterIndexValidation.ValidationScratch scratch = ClusterIndexValidation.scratch();
        collectMaps(storage, scratch, maxValidatedObjects);
        try {
            for (final GigaMap<?> map : scratch.maps) {
                ClusterIndexValidation.collectIndexGroups(map, scratch.groups);
                try {
                    for (final IndexGroup<?> group : scratch.groups) {
                        if (group instanceof VectorIndices<?> vectors) {
                            resetVectorSearchGraphs(vectors);
                        } else if (group instanceof LuceneIndex<?> lucene) {
                            retireLuceneView(lucene);
                        }
                        /* Bitmap groups carry no cached search views: their
                         * structural state ships inside the Store and
                         * materializes directly, so nothing needs refresh. */
                    }
                } finally {
                    scratch.groups.clear();
                }
            }
        } finally {
            scratch.maps.clear();
        }
    }

        /// Validates this Store's replicated index boundary and eagerly rebuilds
        /// every vector search graph the refresh cleared, in one traversal.
    ///
    /// The rebuild runs here — inside the merger's coordinator write section
    /// and under the map monitor — instead of lazily on the next query. A
    /// lazy rebuild scans the whole store while holding the map monitor and
    /// performs storage reads; racing it with the next batch's bulk
    /// materialization deadlocks the two (map monitor against the object
    /// registry) and reads torn entities (zeroed vectors, duplicated nodes).
    /// Rebuilding eagerly over the just-materialized boundary keeps every
    /// query on an already-built graph, so queries never rebuild and observe
    /// at most ordinary torn reads, never wedge the reader.
    ///
    /// The scan visits index metadata only, never entity payload. The rebuild
    /// is skipped entirely when no vector index was discovered, so a
    /// Lucene-only or bitmap-only store pays nothing for vectors.
    ///
    /// @param storage             storage connection owning the materialized graph
    /// @param maxValidatedObjects object bound for the scan
    /// @throws IllegalArgumentException if a root violates the index policy
    /// @throws IllegalStateException    if the scan cannot complete or the rebuild fails
    static void validateAndRebuildImportedIndexes(final StorageConnection storage, final int maxValidatedObjects) {
        Objects.requireNonNull(storage, "storage");
        final ClusterIndexValidation.ValidationScratch scratch = ClusterIndexValidation.scratch();
        scratch.vectorGroups.clear();
        try {
            ClusterIndexValidation.validateStorageRoots(storage, maxValidatedObjects, scratch.vectorGroups);
            for (final ClusterIndexValidation.VectorGroup group : scratch.vectorGroups) {
                ensureVectorSearchGraphs(group.vectors(), scratch);
            }
        } finally {
            scratch.vectorGroups.clear();
        }
    }

    static void collectMaps(final StorageConnection storage,
                            final ClusterIndexValidation.ValidationScratch scratch,
                            final int maxValidatedObjects) {
        scratch.seen.clear();
        scratch.queue.clear();
        scratch.maps.clear();
        try {
            storage.persistenceManager()
                    .viewRoots()
                    .iterateEntries((identifier, value) -> {
                        if (value != null && scratch.seen.put(value, Boolean.TRUE) == null) {
                            scratch.queue.add(value);
                        }
                    });
            int visited = 0;
            while (!scratch.queue.isEmpty()) {
                if (++visited > maxValidatedObjects) {
                    throw new IllegalStateException(
                            ("index refresh exceeded %s index-relevant objects; raise " +
                                    "StorageBinaryDataMerger.Configuration.maxValidatedIndexObjects or narrow " +
                                    "the index-relevant graph so the replication boundary can be proven")
                                    .formatted(maxValidatedObjects));
                }
                final Object current = scratch.queue.poll();
                if (current instanceof GigaMap<?> map) {
                    scratch.maps.add(map);
                } else {
                    ClusterIndexValidation.enqueueReachable(current, scratch.queue, scratch.seen);
                }
            }
        } finally {
            scratch.seen.clear();
            scratch.queue.clear();
        }
    }

        /// Resets every vector search graph in a group to its just-loaded state.
    ///
    /// The transient HNSW builder and graph are closed and dropped and the
    /// one-shot rebuild guard is cleared, so the next search or mutation
    /// re-initializes and rebuilds from the already-current vector store —
    /// the same lazy rebuild every restart performs, and the same shape the
    /// upstream close leaves behind. Deferred builder operations are dropped
    /// with the builder they were computed against; the rebuild recomputes
    /// graph state from the store. No entity is vectorized and no graph node
    /// is surgically mutated, so import batches converge without depending on
    /// writer-side graph invariants.
    ///
    /// This retire-and-rebuild is sound only for synchronously indexed
    /// graphs: background graph workers are rejected at registration and
    /// root validation (see [ClusterIndexValidation#validateVectorConfiguration]),
    /// because no public upstream lifecycle stops an in-flight worker before
    /// its builder is retired here.
    ///
    /// A layout this code no longer recognizes fails closed. Our own
    /// validation already rejects on-disk vector configurations, so the
    /// skipped-in-incremental-mode rebuild path cannot apply here: the rebuild
    /// always runs.
    ///
    /// @param vectors group whose search graphs to reset
    /// @throws IllegalStateException if the upstream field layout changed
    private static void resetVectorSearchGraphs(final VectorIndices<?> vectors) {
        Objects.requireNonNull(vectors, "vectors");
        final ArrayList<VectorIndex<?>> found = new ArrayList<>();
        vectors.accessIndices(indices -> indices.values().iterate(found::add));
        for (final VectorIndex<?> index : found) {
            resetVectorSearchGraph(index);
        }
    }

    private static void resetVectorSearchGraph(final VectorIndex<?> index) {
        final Object target = Objects.requireNonNull(index, "index");
        final StoreIndexReflection.VectorGraphFields fields =
                StoreIndexReflection.vectorGraphFields(target.getClass());
        final Field builderField = fields.builder();
        final Field graphField = fields.graph();
        final GraphIndexBuilder builder =
                (GraphIndexBuilder) StoreIndexReflection.read(target, builderField);
        final OnHeapGraphIndex graph =
                (OnHeapGraphIndex) StoreIndexReflection.read(target, graphField);
        final Object deferred = StoreIndexReflection.read(target, fields.deferred());
        /* Drop operations computed against the retired builder first: the
         * rebuild recomputes graph state from the store. */
        if (deferred instanceof ConcurrentLinkedQueue<?> queued) queued.clear();
        /* Same order as the upstream close: release the builder and the graph
         * eagerly instead of abandoning them, then clear the rebuild guard so
         * the next access re-initializes over current state. Only the map
         * monitor is held here, matching every other refresh mutation; the
         * merger's write section keeps joined reads out until this returns. */
        if (builder != null) {
            try {
                builder.close();
            } catch (final IOException failure) {
                throw new IllegalStateException(
                        "cannot close vector search builder on %s".formatted(target.getClass().getName()), failure);
            }
            StoreIndexReflection.write(target, builderField, null);
        }
        if (graph != null) {
            graph.close();
            StoreIndexReflection.write(target, graphField, null);
        }
        StoreIndexReflection.writeBoolean(target, fields.rebuilt(), false);
    }

        /// Rebuilds one vector group's search graphs eagerly after an import batch.
    ///
    /// The trigger is a trivial top-1 search: a search always initializes the
    /// index first, which rebuilds exactly when the refresh cleared the guard.
    /// The probe vector is all-ones so its norm can never be zero. The guard
    /// is read back after every probe, so an upstream version that decouples
    /// search from initialization fails this merger loudly instead of
    /// silently leaving the rebuild to the next query. The probe is reused
    /// per index across batches, so a steady import allocates no probe arrays.
    ///
    /// @param vectors vector group to rebuild
    /// @param scratch worker-local scratch owning the reused probes
    private static void ensureVectorSearchGraphs(final VectorIndices<?> vectors,
                                                  final ClusterIndexValidation.ValidationScratch scratch) {
        scratch.vectorIndexes.clear();
        try {
            vectors.accessIndices(indices -> indices.values().iterate(scratch.vectorIndexes::add));
            for (final VectorIndex<?> index : scratch.vectorIndexes) {
                try {
                    final float[] probe = scratch.vectorProbes.computeIfAbsent(
                            index, key -> ones(key.configuration().dimension()));
                    index.search(probe, 1);
                } catch (final RuntimeException | Error rebuildFailure) {
                    throw new IllegalStateException("Store graph vector-index rebuild failed", rebuildFailure);
                }
                /* Prove the probe rebuilt: if a future Store version decouples
                 * search from lazy initialization, the cleared guard survives
                 * this call and the next query would wedge against a batch again.
                 * Failing here keeps that regression loud — a failed merger —
                 * instead of a silent return of the deadlock. On-disk
                 * configurations, whose rebuild upstream skips, are rejected by
                 * validation, so the guard must be set for every index seen here,
                 * including an empty store (the flag is set even when the rebuild
                 * finds no entries). */
                final Field rebuilt = StoreIndexReflection
                        .vectorGraphFields(index.getClass())
                        .rebuilt();
                if (!StoreIndexReflection.readBoolean(index, rebuilt)) {
                    throw new IllegalStateException(
                            "vector search graph rebuild did not run on %s; unsupported Store version"
                                    .formatted(index.getClass().getName()));
                }
            }
        } finally {
            scratch.vectorIndexes.clear();
        }
    }

    /// Builds an all-ones probe vector; its norm can never be zero.
    private static float[] ones(final int dimension) {
        final float[] probe = new float[dimension];
        Arrays.fill(probe, 1.0f);
        return probe;
    }

        /// Retires a reader's cached Lucene handles, so the next query reopens
        /// over the replicated files.
    ///
    /// Upstream {@link LuceneIndex#close()} is the supported seam: it is
    /// explicitly documented as re-usable after close, with lazy
    /// initialization rebuilding the transient view from the current state
    /// on the next query. No reflection or offset-based field access is used
    /// here. The writer is closed — never rolled back: rollback would delete
    /// every directory file the writer did not create, which on a reader is
    /// exactly the replicated commit point; a reader issues no writes, so
    /// closing commits nothing new. Closing the directory is safe: the graph
    /// directory's files live in the persisted file-entries registry, which
    /// close does not touch, so the next query recreates the directory over
    /// the same current files. The map monitor is already held by the caller,
    /// matching the synchronization upstream close performs.
    ///
    /// @param lucene index whose cached view to retire
    /// @throws IllegalStateException if retiring the view fails
    private static void retireLuceneView(final LuceneIndex<?> lucene) {
        Objects.requireNonNull(lucene, "lucene");
        try {
            lucene.close();
        } catch (final IORuntimeException failure) {
            throw new IllegalStateException(
                    "cannot retire reader Lucene view on %s"
                            .formatted(lucene.getClass().getName()), failure);
        }
    }
}
