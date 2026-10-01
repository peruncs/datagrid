package peruncs.cluster.storage.index;

import org.eclipse.serializer.collections.Set_long;
import org.eclipse.serializer.exceptions.IORuntimeException;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTypeHandlerManager;
import org.eclipse.store.gigamap.jvector.VectorIndex;
import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.gigamap.types.IndexGroup;
import org.eclipse.store.storage.types.StorageConnection;

import java.nio.ByteBuffer;
import java.util.*;


/// Reader-side index maintenance for replicated Store imports.
///
/// Imports bypass map update methods. Lucene views are retired before the
/// object swap; changed vector graphs are invalidated during apply and warmed
/// afterward only when their persisted change counter advanced.
public final class ClusterIndexMaintenance {
    private final ArrayList<GigaMap<?>> cachedMaps = new ArrayList<>();
    private final ClusterIndexValidation.ValidationScratch scratch;
    private final IdentityHashMap<GigaMap<?>, Boolean> knownMaps = new IdentityHashMap<>();
    private final Set_long reachableIds = Set_long.New();
    private final Map<String, Object> rootValues = new HashMap<>();
    private final EntityHeaders.EntityVisitor importedIdCheck = (_, objectId) -> {
        if (this.reachableIds.contains(objectId)) {
            this.reachabilityChanged = true;
        }
    };
    private boolean initialized;
    private boolean reachabilityChanged;
    private boolean rootsDiffer;
    private int rootsSeen;

    /// Creates import maintenance with the Store's supported reference walker.
    ///
    /// @param typeHandlers manager owning the Store's runtime type handlers
    public ClusterIndexMaintenance(final PersistenceTypeHandlerManager<Binary> typeHandlers) {
        this.scratch = new ClusterIndexValidation.ValidationScratch(typeHandlers);
    }

    /// Records the indexes and root links that may change in the incoming batch.
    public void beforeApply(final StorageConnection storage, final ByteBuffer[] buffers,
                     final int length, final int maxValidatedObjects) {
        ClusterStoreIndexes.withRegistrationRead(() -> {
            if (!this.initialized || this.rootsChanged(storage)) this.scanRoots(storage, maxValidatedObjects);
            this.reachabilityChanged = false;
            for (int index = 0; index < length; index++) {
                final ByteBuffer buffer = buffers[index];
                if (buffer == null) throw new IllegalArgumentException("index maintenance received a null buffer");
                if (buffer.limit() != 0) {
                    EntityHeaders.forEach(buffer, this.importedIdCheck);
                }
            }
            /* Lucene views are cached NRT readers built lazily per index, not per
             * entity: even a batch that touched no known-reachable entity must
             * retire them, or the next query reopens over the *pre-import* files
             * (torn reads). The Lucene index files are ordinary replicated entities of
             * internal types that cannot be told apart from plain data by their type, so
             * no per-batch type test can prove a batch left them alone: retiring is
             * unconditional, and it costs only the lazy reopen of an index that exists. */
            refreshMaps(this.cachedMaps, this.scratch, maxValidatedObjects);
        });
    }

    /// Validates changed roots and invalidates only changed vector graphs.
    ///
    /// If any graph changed, the caller should run [#warmupVectorSearchGraphs()] after
    /// the write section: the invalidated graphs rebuild lazily and exactly once on their
    /// first search anyway, so warming them up only moves that cost off the first query.
    ///
    /// @return whether a vector graph should be warmed up
    public boolean afterApply(final StorageConnection storage, final int maxValidatedObjects) {
        ClusterStoreIndexes.withRegistrationRead(() -> {
            final ClusterIndexValidation.ValidationScratch scratch = this.scratch;
            scratch.vectorGroups.clear();
            scratch.rebuiltGroups.clear();
            scratch.dirtyVectorIndexes.clear();
            scratch.vectorProbes.clear();
            try {
                if (this.reachabilityChanged || this.rootsChanged(storage)) {
                    this.scanRoots(storage, maxValidatedObjects);
                } else {
                    for (final GigaMap<?> map : this.cachedMaps) {
                        ClusterIndexValidation.validateMap(map, scratch.vectorGroups, scratch, maxValidatedObjects);
                    }
                }
                for (final ClusterIndexValidation.VectorGroup group : scratch.vectorGroups) {
                    if (scratch.rebuiltGroups.put(group.vectors(), Boolean.TRUE) == null) {
                        resetChangedVectorSearchGraphs(group.vectors(), scratch);
                    }
                }
            } finally {
                scratch.vectorGroups.clear();
                scratch.rebuiltGroups.clear();
                scratch.vectorModCounts.clear();
            }
        });
        return !this.scratch.dirtyVectorIndexes.isEmpty();
    }

    /// Rebuilds invalidated vector graphs ahead of the first application query.
    public void warmupVectorSearchGraphs() {
        final ClusterIndexValidation.ValidationScratch scratch = this.scratch;
        if (scratch.dirtyVectorIndexes.isEmpty()) {
            scratch.vectorProbes.clear();
            return;
        }
        try {
            ClusterStoreIndexes.withRegistrationRead(
                    () -> ensureVectorSearchGraphs(scratch.dirtyVectorIndexes, scratch));
        } finally {
            scratch.dirtyVectorIndexes.clear();
            scratch.vectorProbes.clear();
        }
    }

    /// Drops all per-batch scratch after a failed phase.
    ///
    /// The owning merger latches its terminal failure on any mismanaged batch,
    /// so nothing material runs again — but until disposal the scratch arrays
    /// must not pin the half-planned indexes and their reachable graphs.
    /// (Caller-owned failure recovery; package-visibility would leave the
    /// binary merger in a different package unable to reach it.)
    public void resetScratch() {
        final ClusterIndexValidation.ValidationScratch scratch = this.scratch;
        scratch.vectorGroups.clear();
        scratch.rebuiltGroups.clear();
        scratch.vectorModCounts.clear();
        scratch.vectorProbes.clear();
        scratch.vectorIndexes.clear();
        scratch.dirtyVectorIndexes.clear();
        scratch.groups.clear();
        scratch.maps.clear();
        scratch.groupQueue.clear();
        scratch.groupSeen.clear();
    }

    private boolean rootsChanged(final StorageConnection storage) {
        this.rootsSeen = 0;
        this.rootsDiffer = false;
        storage.persistenceManager().viewRoots().iterateEntries((identifier, value) -> {
            this.rootsSeen++;
            if (!this.rootValues.containsKey(identifier) || this.rootValues.get(identifier) != value) {
                this.rootsDiffer = true;
            }
        });
        return this.rootsDiffer || this.rootsSeen != this.rootValues.size();
    }

    private void scanRoots(final StorageConnection storage, final int maxValidatedObjects) {
        ClusterStoreIndexes.withRegistrationRead(() -> {
            final var manager = storage.persistenceManager();
            final ClusterIndexValidation.ValidationScratch scratch = this.scratch;
            this.cachedMaps.clear();
            this.knownMaps.clear();
            this.reachableIds.truncate();
            scratch.vectorGroups.clear();
            ClusterIndexValidation.validateStorageRoots(storage, maxValidatedObjects, scratch.vectorGroups, current -> {
                if (current instanceof GigaMap<?> map && this.knownMaps.put(map, Boolean.TRUE) == null) {
                    this.cachedMaps.add(map);
                }
                final long id = manager.lookupObjectId(current);
                if (id > 0L) this.reachableIds.add(id);
            }, scratch);
            this.rootValues.clear();
            manager.viewRoots().iterateEntries(this.rootValues::put);
            this.initialized = true;
        });
    }

    /// Compatibility entry for refreshing views discovered from Store roots.
    ///
    /// Imports materialize entities without calling the map's add/update/remove
    /// API, so no index group observes the change and both search views freeze
    /// at whatever the first query built: Lucene's cached near-real-time reader
    /// reopens only when a write-path mutation marks it stale, and JVector's
    /// transient graph rebuilds exactly once after load. A commit-only
    /// refresh is therefore insufficient by construction — retirement and
    /// rebuild are mandatory on the import path.
    ///
    /// The refresh is two-tracked because the two indexes replicate
    /// differently. Lucene's complete directory content ships inside the Store
    /// (graph directory committed at the writer's `store()` boundary), so the
    /// reader must only retire its cached handles: the next query reopens over
    /// the already-current files. A reader-side rollback or rebuild is not
    /// just unnecessary here, it is corrupting: rollback deletes the
    /// replicated commit point the writer never created, and reopening over
    /// the commit-less remainder wipes the rest. JVector stores its vectors
    /// but not its search graph; the refresh records its change counter so the
    /// post-import pass can rebuild only graphs that changed.
    ///
    /// This compatibility helper only discovers maps, snapshots vector
    /// change counters, and retires Lucene views. The production merger uses
    /// [#beforeApply(StorageConnection, ByteBuffer[], int, int)] and
    /// [#afterApply(StorageConnection, int)] instead; its coordinator write
    /// side covers import, materialization, validation, graph invalidation,
    /// and vector warmup. No map monitor is taken.
    ///
    /// @param storage             storage connection whose roots and indexes are inspected
    /// @param maxValidatedObjects object bound for the discovery scan
    static ClusterIndexValidation.ValidationScratch refreshImportedIndexes(
            final StorageConnection storage, final PersistenceTypeHandlerManager<Binary> typeHandlers,
            final int maxValidatedObjects) {
        Objects.requireNonNull(storage, "storage");
        return ClusterStoreIndexes.withRegistrationRead(() -> {
            final ClusterIndexValidation.ValidationScratch scratch =
                    new ClusterIndexValidation.ValidationScratch(typeHandlers);
            collectMaps(storage, scratch, maxValidatedObjects);
            try {
                refreshMaps(scratch.maps, scratch, maxValidatedObjects);
            } finally {
                scratch.maps.clear();
            }
            return scratch;
        });
    }

    private static void refreshMaps(final ArrayList<GigaMap<?>> maps,
                                    final ClusterIndexValidation.ValidationScratch scratch,
                                    final int maxValidatedObjects) {
        scratch.vectorModCounts.clear();
        for (final GigaMap<?> map : maps) {
            ClusterIndexValidation.collectIndexGroups(map.index(), scratch, maxValidatedObjects);
            try {
                for (final IndexGroup<?> group : scratch.groups) {
                    if (group instanceof VectorIndices<?> vectors) {
                        vectors.accessIndices(indices -> indices.values().iterate(index -> {
                            if (index instanceof VectorIndex.Default<?> known) {
                                scratch.vectorModCounts.put(index, known.getStructuralModCount());
                            }
                        }));
                    } else if (group instanceof LuceneIndex<?> lucene) {
                        retireLuceneView(lucene);
                    }
                    /* Bitmap groups carry no cached search views. */
                }
            } finally {
                scratch.groups.clear();
            }
        }
    }

    /// Validates imported index metadata and warms changed vector graphs.
    ///
    /// Standalone validation helper. The merger uses [#afterApply(StorageConnection, int)]
    /// followed by [#warmupVectorSearchGraphs()] before leaving the write side.
    ///
    /// @param storage             connection owning the imported Store
    /// @param maxValidatedObjects maximum roots to inspect
    /// @param scratch             reusable scan state
    /// @throws IllegalArgumentException if an imported root violates index policy
    /// @throws IllegalStateException if validation or graph rebuilding cannot complete
    static void validateAndRebuildImportedIndexes(final StorageConnection storage, final int maxValidatedObjects,
                                                  final ClusterIndexValidation.ValidationScratch scratch) {
        Objects.requireNonNull(storage, "storage");
        ClusterStoreIndexes.withRegistrationRead(() -> {
            scratch.vectorGroups.clear();
            scratch.rebuiltGroups.clear();
            scratch.dirtyVectorIndexes.clear();
            scratch.vectorProbes.clear();
            try {
                ClusterIndexValidation.validateStorageRoots(
                        storage, maxValidatedObjects, scratch.vectorGroups, null, scratch);
                for (final ClusterIndexValidation.VectorGroup group : scratch.vectorGroups) {
                    if (scratch.rebuiltGroups.put(group.vectors(), Boolean.TRUE) == null) {
                        resetChangedVectorSearchGraphs(group.vectors(), scratch);
                    }
                }
                ensureVectorSearchGraphs(scratch.dirtyVectorIndexes, scratch);
            } finally {
                scratch.vectorGroups.clear();
                scratch.rebuiltGroups.clear();
                scratch.vectorModCounts.clear();
                scratch.vectorProbes.clear();
                scratch.dirtyVectorIndexes.clear();
            }
        });
    }

    private static void resetChangedVectorSearchGraphs(final VectorIndices<?> vectors,
                                                       final ClusterIndexValidation.ValidationScratch scratch) {
        scratch.vectorIndexes.clear();
        try {
            vectors.accessIndices(indices -> indices.values().iterate(scratch.vectorIndexes::add));
            for (final VectorIndex<?> index : scratch.vectorIndexes) {
                final Long before = scratch.vectorModCounts.get(index);
                if (!(index instanceof VectorIndex.Default<?> known) || before == null ||
                    before != known.getStructuralModCount()) {
                    /* Retire the transient graph so the next search rebuilds it lazily from the current vector
                     * store, as after a restart. Sound only for synchronously indexed graphs: background graph
                     * workers are rejected at registration (ClusterIndexValidation#validateVectorConfiguration),
                     * and on-disk configurations are rejected too, so the incremental-mode refusal cannot apply. */
                    index.invalidateGraph();
                    scratch.dirtyVectorIndexes.add(index);
                }
            }
        } finally {
            scratch.vectorIndexes.clear();
        }
    }

    static void collectMaps(final StorageConnection storage,
                            final ClusterIndexValidation.ValidationScratch scratch,
                            final int maxValidatedObjects) {
        scratch.seen.clear();
        scratch.queue.clear();
        scratch.maps.clear();
        scratch.scanWork = 0;
        try {
            storage.persistenceManager()
                    .viewRoots()
                    .iterateEntries((_, value) -> {
                        if (value != null && scratch.seen.put(value, Boolean.TRUE) == null) {
                            scratch.queue.add(value);
                        }
                    });
            while (!scratch.queue.isEmpty()) {
                ClusterIndexValidation.countScanWork(scratch, maxValidatedObjects);
                final Object current = scratch.queue.poll();
                if (current instanceof GigaMap<?> map) {
                    scratch.maps.add(map);
                } else {
                    ClusterIndexValidation.enqueueReachable(current, scratch, maxValidatedObjects);
                }
            }
        } finally {
            scratch.seen.clear();
            scratch.queue.clear();
        }
    }

    /// Rebuilds changed vector search graphs before application reads resume.
    ///
    /// A trivial top-1 search initializes and rebuilds each changed graph.
    /// The all-ones probe has a non-zero norm. It is reused per dimension and
    /// dropped with the batch, so the scratch never retains a retired index.
    ///
    /// @param dirtyIndexes dirty vector index groups to rebuild
    /// @param scratch worker-local scratch owning the reused probes
    private static void ensureVectorSearchGraphs(final ArrayList<VectorIndex<?>> dirtyIndexes,
                                                  final ClusterIndexValidation.ValidationScratch scratch) {
        for (final VectorIndex<?> index : dirtyIndexes) {
            try {
                final float[] probe = scratch.vectorProbes.computeIfAbsent(
                        index.configuration().dimension(), ClusterIndexMaintenance::ones);
                index.search(probe, 1);
            } catch (final RuntimeException | Error rebuildFailure) {
                throw new IllegalStateException("Store graph vector-index rebuild failed", rebuildFailure);
            }
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
    /// the same current files. The production import caller holds the
    /// coordinator write side. No map monitor is taken, because this code
    /// performs no map mutation upstream close would race.
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
