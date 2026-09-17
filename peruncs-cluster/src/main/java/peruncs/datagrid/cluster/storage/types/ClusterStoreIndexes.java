package peruncs.datagrid.cluster.storage.types;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.eclipse.serializer.concurrency.LockedExecutor;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.typing.KeyValue;
import org.eclipse.store.gigamap.jvector.VectorIndex;
import org.eclipse.store.gigamap.jvector.VectorIndexConfiguration;
import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.jvector.Vectorizer;
import org.eclipse.store.gigamap.lucene.AnalyzerCreator;
import org.eclipse.store.gigamap.lucene.DocumentPopulator;
import org.eclipse.store.gigamap.lucene.LuceneContext;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.BitmapIndices;
import org.eclipse.store.gigamap.types.GigaIndices;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.gigamap.types.IndexGroup;
import org.eclipse.store.storage.types.StorageConnection;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/// Keeps clustered text and vector search inside the Store object graph and
/// registers the index types that are safe to replicate with Data Grid.
///
/// A replicated Store transaction is the only source of truth. An index
/// directory outside that transaction can advance independently, so it cannot
/// be made correct by copying or naming the directory. Registration therefore
/// offers exactly the two supported paths — embedded Lucene and in-graph
/// JVector — and validation rejects everything else. Lucene uses an embedded
/// GraphDirectory with manual commit at the `GigaMap.store()` boundary;
/// JVector uses its persisted vector store, and its transient search graph is
/// rebuilt locally by each reader.
///
/// The class is intentionally small. It does not copy, mirror, or repair
/// files. It makes the transaction boundary explicit: Lucene uses an embedded
/// context with manual Store-boundary commits, while JVector keeps its source
/// vectors in the Store and rebuilds its transient graph on each node.
///
/// # What validation actually covers
///
/// Direct registrations bypass [#registerLucene] and [#registerVector], so
/// enforcement also scans reachable index metadata ([validateGraph],
/// [validateStorageRoots], [validateForPublication]). The scan never descends
/// into GigaMap entity payloads, and it never expands objects whose class
/// cannot reach index metadata: a per-class relevance analysis (cached, and
/// conservative — anything unprovable stays relevant) prunes ordinary entity
/// graphs, so a root holding thousands of plain objects validates without
/// touching the object bound. The bound therefore counts only index-relevant
/// objects and still fails closed whenever an upstream layout prevents proving
/// that every index group was checked. The field reads use Store's own
/// offset-based memory accessor, so no runtime `--add-opens` flag is
/// required. JDK references cannot be persisted by Store; validation inspects
/// direct index referents but prunes other runtime referents to avoid scanning
/// Store's bookkeeping graph. Other opaque JDK holders with state fail closed.
///
/// Validation is scoped to the Store being written or read: the root scan
/// discovers the maps belonging to that Store, so a still-referenced map from
/// another Store or stream never blocks an unrelated writer. A GigaMap is
/// validated wherever the scan meets it; registration exists to build the
/// supported index kinds, not to track maps.
public final class ClusterStoreIndexes {
    private static final String EXTERNAL_LUCENE_MESSAGE = "Cluster replication supports only embedded Lucene indexes; external directories are not supported";
    private static final String EXTERNAL_VECTOR_MESSAGE = "Cluster replication supports only in-graph JVector indexes; external index directories are not supported";
    private static final String BACKGROUND_VECTOR_MESSAGE = "Cluster replication supports only synchronous JVector indexing; background graph workers cannot be retired safely on import";
    private static final String UNKNOWN_INDEX_MESSAGE = "Cluster replication supports only embedded Lucene, in-graph JVector, and core bitmap indexes";
        /* Registration check-then-act must not lock on the foreign index object:
         * any other code synchronizing on it could deadlock with registration,
         * and nothing else honors that monitor. One executor guards both. */
    private static final LockedExecutor REGISTRATION = LockedExecutor.New();

        /* Bounds one graph validation so the reader hook never pays for the data
         * set: only index-relevant objects are visited (ordinary entity graphs
         * are pruned by class before they are enqueued), and the scan stops
         * here. */
    private static final int MAX_VALIDATED_OBJECTS = 4096;

        /* Per-class index-relevance cache backing the traversal prune. A class
         * is relevant when its instances could reach index metadata; anything
         * unprovable (interfaces, abstract types, `Object` fields, JDK state,
         * reflection failures) stays relevant so the walk fails closed rather
         * than skipping unknown state. Deterministic per class, so concurrent
         * duplicate analyses are harmless. */
    /* ClassValue lets application classes unload with their class loader. A
     * process that creates and retires many Store class loaders must not keep
     * every analyzed Class strongly reachable forever. */
    private static final ClassValue<Boolean> INDEX_RELEVANT = new ClassValue<>() {
        @Override
        protected Boolean computeValue(final Class<?> type) {
            return analyzeIndexRelevant(type, new HashSet<>());
        }
    };

        /* Worker-local validation scratch: the seen set and the traversal queue
         * are reused across scans instead of allocating an IdentityHashMap and
         * an ArrayDeque per root on every replicated batch. Each thread holds
         * its own scratch, and every entry point clears it before and after
         * use so traversed graph state is never retained. The scan itself is
         * iterative, so one scratch is never needed reentrantly. */
    private static final ThreadLocal<ValidationScratch> SCRATCH =
            ThreadLocal.withInitial(ValidationScratch::new);

        /* Resolved upstream vector-graph internals, cached per index class.
         * `ClassValue` like [#INDEX_RELEVANT] so index classes unload with
         * their Store class loader. Resolution runs once per class, not once
         * per index per batch; an unrecognized layout throws instead of
         * caching, so the next batch fails closed again. */
    private record VectorGraphFields(Field builder, Field graph, Field rebuilt, Field deferred) {
    }

    private static final ClassValue<VectorGraphFields> VECTOR_GRAPH_FIELDS = new ClassValue<>() {
        @Override
        protected VectorGraphFields computeValue(final Class<?> type) {
            Field builderField = null;
            Field graphField = null;
            Field rebuiltField = null;
            Field deferredField = null;
            for (Class<?> current = type; current != null && current != Object.class;
                 current = current.getSuperclass()) {
                for (final Field field : current.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers())) continue;
                    switch (field.getName()) {
                        case "builder" -> {
                            if (builderField == null && GraphIndexBuilder.class.isAssignableFrom(field.getType())) {
                                builderField = field;
                            }
                        }
                        case "index" -> {
                            if (graphField == null && OnHeapGraphIndex.class.isAssignableFrom(field.getType())) {
                                graphField = field;
                            }
                        }
                        case "graphRebuilt" -> {
                            if (rebuiltField == null && field.getType() == boolean.class) {
                                rebuiltField = field;
                            }
                        }
                        case "deferredBuilderOps" -> {
                            if (deferredField == null
                                    && ConcurrentLinkedQueue.class.isAssignableFrom(field.getType())) {
                                deferredField = field;
                            }
                        }
                        default -> {
                        }
                    }
                }
                if (builderField != null && graphField != null
                        && rebuiltField != null && deferredField != null) break;
            }
            if (builderField == null || graphField == null || rebuiltField == null || deferredField == null) {
                throw new IllegalStateException(
                        "cannot reset vector search graph on %s; unsupported Store version"
                                .formatted(type.getName()));
            }
            return new VectorGraphFields(builderField, graphField, rebuiltField, deferredField);
        }
    };

    private static final class ValidationScratch {
        final IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
        final ArrayDeque<Object> queue = new ArrayDeque<>();
        final ArrayList<IndexGroup<?>> groups = new ArrayList<>();
        final ArrayList<GigaMap<?>> maps = new ArrayList<>();
    }

    private ClusterStoreIndexes() {
    }

        /// Creates the only Lucene context supported by clustered storage.
    ///
    /// @param <E>               entity type
    /// @param documentPopulator document mapping
    /// @return an embedded, Store-boundary-committed context
    public static <E> LuceneContext<E> embeddedLuceneContext(final DocumentPopulator<E> documentPopulator) {
        return LuceneContext.New(
                null,
                AnalyzerCreator.Standard(),
                Objects.requireNonNull(documentPopulator, "documentPopulator"),
                false
        );
    }

        /// Registers an embedded Lucene index on a map.
    ///
    /// @param <E>               entity type
    /// @param map               target map
    /// @param documentPopulator document mapping
    /// @return the newly registered index
    /// @throws IllegalStateException if the map already has a Lucene index
    public static <E> LuceneIndex<E> registerLucene(final GigaMap<E> map, final DocumentPopulator<E> documentPopulator) {
        final GigaMap<E> checkedMap = Objects.requireNonNull(map, "map");
        final DocumentPopulator<E> checkedPopulator = Objects.requireNonNull(documentPopulator, "documentPopulator");
        return REGISTRATION.write(() -> registerLuceneLocked(checkedMap, checkedPopulator));
    }

    @SuppressWarnings("unchecked") // Lucene's class token cannot retain its entity type.
    private static <E> LuceneIndex<E> luceneIndex(final GigaMap<E> map) {
        return map.index().get(LuceneIndex.class);
    }

    private static <E> LuceneIndex<E> registerLuceneLocked(
            final GigaMap<E> map,
            final DocumentPopulator<E> documentPopulator) {
        if (luceneIndex(map) != null) {
            throw new IllegalStateException("a clustered map already has a Lucene index");
        }
        try {
            final LuceneIndex<E> registered = map.index().register( LuceneIndex.Category(embeddedLuceneContext(documentPopulator)));
            if (registered == null) {
                /* Upstream reports a lost registration race with a null
                 * return rather than a throw: name the duplicate when one is
                 * now present instead of mislabeling it as an opaque failure. */
                if (luceneIndex(map) != null) {
                    throw new IllegalStateException("a clustered map already has a Lucene index");
                }
                throw new IllegalStateException("failed to register clustered Lucene index");
            }
            return registered;
        } catch (final RuntimeException raced) {
            /* A foreign registration slipped in between the check and the
             * register. Normalize to the documented duplicate failure instead
             * of leaking the upstream error type, mirroring
             * [#addVectorLocked]. */
            if (luceneIndex(map) != null) {
                throw new IllegalStateException(
                        "a clustered map already has a Lucene index", raced);
            }
            throw raced;
        }
    }

        /// Rejects a Lucene context that stores files outside the Store graph.
    ///
    /// @param context context to check
    /// @throws IllegalArgumentException if the context creates an external directory
    public static void validateLuceneContext(final LuceneContext<?> context) {
        if (Objects.requireNonNull(context, "context").directoryCreator() != null) {
            throw new IllegalArgumentException(EXTERNAL_LUCENE_MESSAGE);
        }
    }

        /// Adds an in-graph vector index to a map.
    ///
    /// @param <E>           entity type
    /// @param indices       vector index group
    /// @param name          index name
    /// @param configuration vector configuration
    /// @param vectorizer    entity-to-vector mapping
    /// @return the new vector index
    /// @throws IllegalStateException if an index with the same name is already registered
    public static <E> VectorIndex<E> addVector(
            final VectorIndices<E> indices,
            final String name,
            final VectorIndexConfiguration configuration,
            final Vectorizer<? super E> vectorizer) {
        validateVectorConfiguration(configuration);
        final VectorIndices<E> checkedIndices = Objects.requireNonNull(indices, "indices");
        final String checkedName = Objects.requireNonNull(name, "name");
        final Vectorizer<? super E> checkedVectorizer = Objects.requireNonNull(vectorizer, "vectorizer");
        return REGISTRATION.write(() -> addVectorLocked(checkedIndices, checkedName, configuration, checkedVectorizer));
    }

        /// Registers an in-graph vector index on a map, creating its index group once.
    ///
    /// @param <E>           entity type
    /// @param map           target map
    /// @param name          index name
    /// @param configuration vector configuration
    /// @param vectorizer    entity-to-vector mapping
    /// @return the new vector index
    /// @throws IllegalStateException if an index with the same name is already registered
    public static <E> VectorIndex<E> registerVector(
            final GigaMap<E> map,
            final String name,
            final VectorIndexConfiguration configuration,
            final Vectorizer<? super E> vectorizer) {
        final GigaMap<E> checkedMap = Objects.requireNonNull(map, "map");
        validateVectorConfiguration(configuration);
        final String checkedName = Objects.requireNonNull(name, "name");
        final Vectorizer<? super E> checkedVectorizer = Objects.requireNonNull(vectorizer, "vectorizer");
        return REGISTRATION.write(() -> {
            VectorIndices<E> indices = checkedMap.index().get(VectorIndices.Category());
            if (indices == null) {
                indices = checkedMap.index().register(VectorIndices.Category());
            }
            return addVectorLocked(indices, checkedName, configuration, checkedVectorizer);
        });
    }

    private static <E> VectorIndex<E> addVectorLocked(
            final VectorIndices<E> indices,
            final String name,
            final VectorIndexConfiguration configuration,
            final Vectorizer<? super E> vectorizer) {
        if (indices.get(name) != null) {
            throw new IllegalStateException("a vector index named \"" + name + "\" is already registered");
        }
        try {
            return indices.add(name, configuration, vectorizer);
        } catch (final RuntimeException raced) {
            /* A foreign registration slipped in between the check and the add.
             * Normalize to the documented duplicate failure instead of leaking
             * the upstream error type. */
            if (indices.get(name) != null) {
                throw new IllegalStateException(
                        "a vector index named \"" + name + "\" is already registered", raced);
            }
            throw raced;
        }
    }

        /// Rejects any JVector configuration that replication cannot carry.
    ///
    /// External directories never reach a reader, and background graph
    /// workers (eventual indexing, background optimization) cannot be
    /// retired safely: the import refresh closes and nulls the builder and
    /// graph, and no public upstream lifecycle stops an in-flight worker
    /// first, so a worker could use a retired builder mid-import.
    ///
    /// @param configuration configuration to check
    /// @throws IllegalArgumentException if on-disk mode, a directory, or a
    ///                                  background graph mode is configured
    public static void validateVectorConfiguration(final VectorIndexConfiguration configuration) {
        final VectorIndexConfiguration checked = Objects.requireNonNull(configuration, "configuration");
        if (checked.onDisk() || checked.indexDirectory() != null) {
            throw new IllegalArgumentException(EXTERNAL_VECTOR_MESSAGE);
        }
        if (checked.eventualIndexing() || checked.backgroundOptimization()) {
            throw new IllegalArgumentException(BACKGROUND_VECTOR_MESSAGE);
        }
    }

        /// Validates all vector indexes already registered on a map.
    ///
    /// This is useful after Store deserialization, when the index group was
    /// created by a persistence handler rather than by application code.
    ///
    /// @param map map to validate
    /// @throws IllegalArgumentException if a vector index uses external
    ///                                  storage or a background graph mode
    public static void validateVectorIndexes(final GigaMap<?> map) {
        final VectorIndices<?> indices = Objects.requireNonNull(map, "map").index().get(VectorIndices.Category());
        if (indices == null) {
            return;
        }
        validateVectorIndicesGroup(indices);
    }

    /// Validates every index attached to one map.
    ///
    /// Every registered index group is enumerated: embedded Lucene and
    /// in-graph vector groups are validated, the core bitmap group is
    /// accepted as in-graph by construction, and any other group fails with
    /// [IllegalArgumentException] — an unknown category may keep state the
    /// replication cannot carry, so it fails closed instead of being assumed
    /// safe. If group enumeration cannot be proved under JPMS, validation throws
    /// [IllegalStateException] rather than assuming unknown state is safe.
    ///
    /// @param map map to validate
    /// @throws IllegalArgumentException if any attached index uses external
    ///                                  storage or belongs to an unknown category
    /// @throws IllegalStateException    if index groups cannot be enumerated completely
    public static void validateMap(final GigaMap<?> map) {
        final GigaMap<?> checked = Objects.requireNonNull(map, "map");
        final ValidationScratch scratch = SCRATCH.get();
        scratch.groups.clear();
        try {
            collectIndexGroups(checked, scratch.groups);
            for (final IndexGroup<?> group : scratch.groups) {
                if (!(group instanceof BitmapIndices)) {
                    if (group instanceof LuceneIndex<?> lucene) {
                        validateLuceneIndex(lucene);
                    } else if (group instanceof VectorIndices<?> vectors) {
                        validateVectorIndicesGroup(vectors);
                    } else {
                        throw new IllegalArgumentException(
                                "%s; found unsupported index group: %s".formatted(
                                        UNKNOWN_INDEX_MESSAGE, group.getClass().getName()));
                    }
                }
            }
        } finally {
            scratch.groups.clear();
        }
    }

        /// Snapshots every index group registered on a map into `collected`.
    ///
    /// The upstream index API exposes lookup by category but no group
/// enumeration, so the groups are located reflectively and read through
/// Store's offset-based memory accessor. Any denied access or unrecognized
/// layout fails closed because a partial list cannot prove replication safety.
    ///
    /// @param map       map whose groups to snapshot
    /// @param collected snapshot destination, cleared first
    private static void collectIndexGroups(final GigaMap<?> map, final ArrayList<IndexGroup<?>> collected) {
        collected.clear();
        final GigaIndices<?> indices = map.index();
        for (Class<?> type = indices.getClass(); type != null && type != Object.class; type = type.getSuperclass()) {
            for (final Field field : type.getDeclaredFields()) {
                if (!"indexGroups".equals(field.getName()) || !Iterable.class.isAssignableFrom(field.getType())) {
                    continue;
                }
                try {
                    final Object value = XMemory.getObject(indices, XMemory.objectFieldOffset(field));
                    if (!(value instanceof Iterable<?> groups)) {
                        throw new IllegalStateException(
                                "GigaMap indexGroups on %s has an unsupported layout"
                                        .formatted(indices.getClass().getName()));
                    }
                    for (final Object group : groups) {
                        if (!(group instanceof IndexGroup<?> indexGroup)) {
                            throw new IllegalStateException(
                                    "GigaMap indexGroups on %s contains an unsupported element: %s"
                                            .formatted(indices.getClass().getName(),
                                                    group == null ? "null" : group.getClass().getName()));
                        }
                        collected.add(indexGroup);
                    }
                    return;
                } catch (final RuntimeException denied) {
                    throw new IllegalStateException(
                            "cannot enumerate GigaMap index groups on %s"
                                    .formatted(indices.getClass().getName()), denied);
                }
            }
        }
        throw new IllegalStateException(
                "cannot enumerate GigaMap index groups on %s; unsupported Store version"
                        .formatted(indices.getClass().getName()));
    }

        /// Enforcement entry point: scans a Store root object graph and rejects any
    /// index configuration that replication cannot carry.
    ///
    /// Replication ships the object graph, so an external Lucene directory or a
    /// non-persisted (on-disk or directory-backed) vector index reachable from the
    /// root would diverge across nodes. This scan catches such indexes even when
    /// they were registered directly, bypassing [#registerLucene] and
    /// [#registerVector]. It stays cheap by design: attached GigaMap entity
    /// payload is never descended into (only index metadata is checked), the scan
/// stops after a bounded number of objects, and fields that cannot be read by
/// the Store memory accessor fail closed because an incomplete scan is not a
/// proof of safety. A violation fails closed with [IllegalArgumentException]. The
/// traversal reuses worker-local scratch instead of allocating per scan.
    ///
    /// @param root Store root to validate
    /// @throws IllegalArgumentException if an external Lucene directory reference or a
    ///                                  non-persisted vector index is reachable from the root
    /// @throws IllegalStateException    if a large index-relevant graph cannot be inspected completely
    public static void validateGraph(final Object root) {
        validateGraphInternal(root);
    }

        /// Validates one root's reachable index metadata.
    private static void validateGraphInternal(final Object root) {
        if (root == null) return;
        final ValidationScratch scratch = SCRATCH.get();
        scratch.seen.clear();
        scratch.queue.clear();
        try {
            scratch.seen.put(root, Boolean.TRUE);
            scratch.queue.add(root);
            int visited = 0;
            while (!scratch.queue.isEmpty()) {
                if (++visited > MAX_VALIDATED_OBJECTS) {
                    throw new IllegalStateException(
                            "index validation exceeded %s index-relevant objects; refusing an unprovable replication boundary"
                                    .formatted(MAX_VALIDATED_OBJECTS));
                }
                final Object current = scratch.queue.poll();
                switch (current) {
                    case GigaMap<?> map ->
                        /* Index metadata only: descending into entity payload would make
                         * every reader batch pay for the whole data set. Objects
                         * whose class cannot reach index metadata were pruned
                         * before enqueueing, so only relevant objects count above. */
                            validateMap(map);
                    case LuceneIndex<?> lucene -> validateLuceneIndex(lucene);
                    case LuceneContext<?> context -> validateLuceneContext(context);
                    case VectorIndices<?> group -> validateVectorIndicesGroup(group);
                    case VectorIndex<?> index -> validateVectorConfiguration(index.configuration());
                    case VectorIndexConfiguration configuration -> validateVectorConfiguration(configuration);
                    case null, default -> enqueueReachable(current, scratch.queue, scratch.seen);
                }
            }
        } finally {
            /* Never retain traversed graph state on the worker thread. */
            scratch.seen.clear();
            scratch.queue.clear();
        }
    }

        /// Validates every Store root held by a storage connection.
    ///
    /// This is the reader materialization hook: it runs after a replicated batch
    /// is applied, so a writer that smuggled an external index past registration
    /// fails the reader closed instead of diverging it. One shared consumer
    /// performs the scan without allocating per root.
    ///
    /// @param storage storage connection owning the materialized graph
    /// @throws IllegalArgumentException if any root violates the index policy
    /// @throws IllegalStateException    if a root cannot be inspected completely
    public static void validateStorageRoots(final StorageConnection storage) {
        Objects.requireNonNull(storage, "storage")
                .persistenceManager()
                .viewRoots()
                .iterateEntries((identifier, value) -> {
                    if (value != null) validateGraph(value);
                });
    }

        /// Reader-side maintenance entry: refreshes every replicated search view
    /// reachable from this Store's roots after a replicated batch is applied.
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
    /// Per-entity graph surgery during import is deliberately avoided: the
    /// group's update and re-insert entries assume writer-side invariants
    /// (repaired neighbor lists, warmed builder state) that do not hold for a
    /// just-materialized graph, and bisecting a native allocator abort in the
    /// reader soak isolated the crash to those calls. The reset uses only the
    /// lifecycle the index already exercises on every load and close. The
    /// merger rebuilds the graphs eagerly after each batch (see
    /// [#rebuildVectorSearchGraphs(StorageConnection)]): leaving the cleared
    /// guard for the next query would race a full store-scan rebuild against
    /// the following batch's materialization.
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
    /// @param storage storage connection owning the materialized graph
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter") // map is the shared GigaMap monitor, not a local lock
    public static void refreshImportedIndexes(final StorageConnection storage) {
        Objects.requireNonNull(storage, "storage");
        final ValidationScratch scratch = SCRATCH.get();
        collectMaps(storage, scratch);
        try {
            for (final GigaMap<?> map : scratch.maps) {
                synchronized (map) {
                    collectIndexGroups(map, scratch.groups);
                    try {
                        for (final IndexGroup<?> group : scratch.groups) {
                            if (group instanceof VectorIndices<?> vectors) {
                                resetVectorSearchGraphs(vectors);
                            } else if (group instanceof LuceneIndex<?> lucene) {
                                invalidateLuceneView(lucene);
                            }
                            /* Bitmap groups carry no cached search views: their
                             * structural state ships inside the Store and
                             * materializes directly, so nothing needs refresh. */
                        }
                    } finally {
                        scratch.groups.clear();
                    }
                }
            }
        } finally {
            scratch.maps.clear();
        }
    }

    private static void collectMaps(final StorageConnection storage, final ValidationScratch scratch) {
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
                if (++visited > MAX_VALIDATED_OBJECTS) {
                    throw new IllegalStateException(
                            "index refresh exceeded %s index-relevant objects; refusing an unprovable replication boundary"
                                    .formatted(MAX_VALIDATED_OBJECTS));
                }
                final Object current = scratch.queue.poll();
                if (current instanceof GigaMap<?> map) {
                    scratch.maps.add(map);
                } else {
                    enqueueReachable(current, scratch.queue, scratch.seen);
                }
            }
            scratch.maps.sort(Comparator.comparingInt(System::identityHashCode));
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
    /// root validation (see [#validateVectorConfiguration]), because no
    /// public upstream lifecycle stops an in-flight worker before its
    /// builder is retired here.
    ///
    /// The transient fields are located reflectively and retired through
    /// Store's offset-based memory accessor — the same technique as
    /// [#collectIndexGroups] and [#invalidateLuceneView] — so no runtime
    /// `--add-opens` flag is required. A layout this code no longer
    /// recognizes fails closed. Our own validation already rejects on-disk
    /// vector configurations, so the skipped-in-incremental-mode rebuild path
    /// cannot apply here: the rebuild always runs.
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
        final VectorGraphFields fields = VECTOR_GRAPH_FIELDS.get(target.getClass());
        final Field builderField = fields.builder();
        final Field graphField = fields.graph();
        final Field rebuiltField = fields.rebuilt();
        final Field deferredField = fields.deferred();
        final GraphIndexBuilder builder =
                (GraphIndexBuilder) XMemory.getObject(target, XMemory.objectFieldOffset(builderField));
        final OnHeapGraphIndex graph =
                (OnHeapGraphIndex) XMemory.getObject(target, XMemory.objectFieldOffset(graphField));
        final Object deferred = XMemory.getObject(target, XMemory.objectFieldOffset(deferredField));
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
            XMemory.setObject(target, XMemory.objectFieldOffset(builderField), null);
        }
        if (graph != null) {
            graph.close();
            XMemory.setObject(target, XMemory.objectFieldOffset(graphField), null);
        }
        XMemory.set_byte(target, XMemory.objectFieldOffset(rebuiltField), (byte) 0);
    }

        /// Rebuilds every vector search graph eagerly after an import batch.
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
    /// The trigger is a trivial top-1 search: a search always initializes the
    /// index first, which rebuilds exactly when the refresh cleared the guard.
    /// The probe vector is all-ones so its norm can never be zero. The guard
    /// is read back after every probe, so an upstream version that decouples
    /// search from initialization fails this merger loudly instead of
    /// silently leaving the rebuild to the next query.
    ///
    /// @param storage storage connection owning the materialized graph
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter") // map is the shared GigaMap monitor, not a local lock
    static void rebuildVectorSearchGraphs(final StorageConnection storage) {
        Objects.requireNonNull(storage, "storage");
        final ValidationScratch scratch = SCRATCH.get();
        collectMaps(storage, scratch);
        try {
            for (final GigaMap<?> map : scratch.maps) {
                synchronized (map) {
                    collectIndexGroups(map, scratch.groups);
                    try {
                        for (final IndexGroup<?> group : scratch.groups) {
                            if (group instanceof VectorIndices<?> vectors) {
                                ensureVectorSearchGraphs(vectors);
                            }
                        }
                    } finally {
                        scratch.groups.clear();
                    }
                }
            }
        } finally {
            scratch.maps.clear();
        }
    }

    private static void ensureVectorSearchGraphs(final VectorIndices<?> vectors) {
        final ArrayList<VectorIndex<?>> found = new ArrayList<>();
        vectors.accessIndices(indices -> indices.values().iterate(found::add));
        for (final VectorIndex<?> index : found) {
            final float[] probe = new float[index.configuration().dimension()];
            Arrays.fill(probe, 1.0f);
            index.search(probe, 1);
            /* Prove the probe rebuilt: if a future Store version decouples
             * search from lazy initialization, the cleared guard survives
             * this call and the next query would wedge against a batch again.
             * Failing here keeps that regression loud — a failed merger —
             * instead of a silent return of the deadlock. On-disk
             * configurations, whose rebuild upstream skips, are rejected by
             * validation, so the guard must be set for every index seen here,
             * including an empty store (the flag is set even when the rebuild
             * finds no entries). */
            final Field rebuilt = VECTOR_GRAPH_FIELDS.get(index.getClass()).rebuilt();
            if (XMemory.get_byte(index, XMemory.objectFieldOffset(rebuilt)) == 0) {
                throw new IllegalStateException(
                        "vector search graph rebuild did not run on %s; unsupported Store version"
                                .formatted(index.getClass().getName()));
            }
        }
    }

        /// Retires a reader's cached Lucene handles without committing, so the
    /// next query reopens over the replicated files.
    ///
    /// The transient fields are located reflectively and retired through
    /// Store's offset-based memory accessor — the same technique as
    /// [#collectIndexGroups] — so no runtime `--add-opens` flag is required.
    /// A layout this code no longer recognizes fails closed: an unprovable
    /// refresh is reported instead of silently keeping a stale search view.
    ///
    /// Every handle is released, not abandoned: the reader, the writer, the
    /// directory, and the analyzer are all closed. The writer is closed
    /// rather than rolled back: rollback deletes every directory file the
    /// writer did not create — on a reader, the replicated commit point
    /// itself — while a reader issues no writes, so closing commits nothing
    /// new. The searcher object itself owns no
    /// resources beyond its reader (it is always built over the reader
    /// field), so dropping the reference frees it. Closing the directory is
    /// safe: the graph directory's files live in the persisted file-entries
    /// registry, which is a separate field the refresh never touches, so the
    /// next query recreates the directory over the same current files.
    ///
    /// @param lucene index whose cached view to retire
    /// @throws IllegalStateException if the upstream field layout changed
    private static void invalidateLuceneView(final LuceneIndex<?> lucene) {
        final Object target = Objects.requireNonNull(lucene, "lucene");
        Field directoryField = null;
        Field writerField = null;
        Field readerField = null;
        Field searcherField = null;
        Field analyzerField = null;
        Field readerStaleField = null;
        for (Class<?> type = target.getClass(); type != null && type != Object.class; type = type.getSuperclass()) {
            for (final Field field : type.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) continue;
                switch (field.getName()) {
                    case "directory" -> {
                        if (directoryField == null) directoryField = field;
                    }
                    case "writer" -> {
                        if (writerField == null) writerField = field;
                    }
                    case "reader" -> {
                        if (readerField == null) readerField = field;
                    }
                    case "searcher" -> {
                        if (searcherField == null) searcherField = field;
                    }
                    case "analyzer" -> {
                        if (analyzerField == null) analyzerField = field;
                    }
                    case "readerStale" -> {
                        if (readerStaleField == null && field.getType() == boolean.class) {
                            readerStaleField = field;
                        }
                    }
                    default -> {
                    }
                }
            }
            if (directoryField != null && writerField != null && readerField != null &&
                searcherField != null && analyzerField != null) break;
        }
        if (directoryField == null || writerField == null || readerField == null ||
            searcherField == null || analyzerField == null) {
            throw new IllegalStateException(
                    "cannot invalidate Lucene view on %s; unsupported Store version"
                            .formatted(target.getClass().getName()));
        }
        final Analyzer analyzer = (Analyzer) XMemory.getObject(target, XMemory.objectFieldOffset(analyzerField));
        final Closeable writer = (Closeable) XMemory.getObject(target, XMemory.objectFieldOffset(writerField));
        final DirectoryReader reader =
                (DirectoryReader) XMemory.getObject(target, XMemory.objectFieldOffset(readerField));
        final Directory directory =
                (Directory) XMemory.getObject(target, XMemory.objectFieldOffset(directoryField));
        /* Same order as the upstream close: analyzer, writer, reader,
         * directory. The writer is closed — never rolled back: rollback
         * deletes every directory file the writer did not create, which on a
         * reader is exactly the replicated commit point, and the next query
         * would reopen over a commit-less directory and wipe the rest. A
         * reader issues no writes, so closing commits nothing new; it only
         * releases the writer so the next query reopens over the current
         * replicated files. The writer is never kept open across batches, so
         * no file deleter ever spans an import swap. */
        closeQuietly(target, analyzer, analyzerField);
        closeQuietly(target, writer, writerField);
        closeQuietly(target, reader, readerField);
        closeQuietly(target, directory, directoryField);
        XMemory.setObject(target, XMemory.objectFieldOffset(searcherField), null);
        if (readerStaleField != null) {
            XMemory.set_byte(target, XMemory.objectFieldOffset(readerStaleField), (byte) 0);
        }
    }

    private static void closeQuietly(final Object target, final Closeable handle, final Field field) {
        if (handle == null) return;
        try {
            handle.close();
        } catch (final AlreadyClosedException alreadyClosed) {
            // A previous refresh already retired it; the field below still needs nulling.
        } catch (final IOException failure) {
            throw new IllegalStateException(
                    "cannot close reader Lucene handle on %s".formatted(target.getClass().getName()), failure);
        }
        XMemory.setObject(target, XMemory.objectFieldOffset(field), null);
    }

    /// Writer-side enforcement entry: validates this Store's roots before publication.
    ///
    /// Call this at writer startup and from the writer commit path, so an index
    /// registered directly — bypassing [#registerLucene] and [#registerVector] —
    /// fails the writer before the diverging transaction is published instead of
    /// failing every reader after the fact.
    ///
    /// Validation is scoped to the Store being written: the bounded, fail-closed
    /// root scan discovers exactly the maps reachable from this Store's roots
    /// and validates each one, so a still-referenced map from another Store or
    /// stream never blocks an unrelated writer. The scan never descends into
    /// entity payloads, so a steady-state write never pays for the
    /// application's data set (see the class javadoc).
    ///
    /// @param storage storage connection owning the writer graph
    /// @throws IllegalArgumentException if any root of this Store violates the index policy
    /// @throws IllegalStateException    if a root cannot be inspected completely
    public static void validateForPublication(final StorageConnection storage) {
        Objects.requireNonNull(storage, "storage");
        storage.persistenceManager()
                .viewRoots()
                .iterateEntries((identifier, value) -> {
                    if (value != null) validateGraph(value);
                });
    }

    private static void validateVectorIndicesGroup(final VectorIndices<?> group) {
        final List<KeyValue<String, ? extends VectorIndex<?>>> snapshot = new ArrayList<>();
        for (final KeyValue<String, ? extends VectorIndex<?>> entry : group) snapshot.add(entry);
        for (final KeyValue<String, ? extends VectorIndex<?>> entry : snapshot) {
            validateVectorConfiguration(entry.value().configuration());
        }
    }

        /// Validates an attached Lucene index by reading back the context it was
/// registered with. The upstream index type exposes no public context
/// accessor, so this locates its `LuceneContext` field reflectively and reads
/// it through Store's offset-based memory accessor. If that layout changes,
/// validation fails closed rather than allowing an unverified external directory.
    ///
    /// @param index attached Lucene index
    /// @throws IllegalArgumentException if the index uses an external directory
    private static void validateLuceneIndex(final LuceneIndex<?> index) {
        final LuceneContext<?> context = luceneContext(index);
        if (context != null) validateLuceneContext(context);
    }

    private static LuceneContext<?> luceneContext(final LuceneIndex<?> index) {
        for (Class<?> type = index.getClass(); type != null && type != Object.class; type = type.getSuperclass()) {
            for (final Field field : type.getDeclaredFields()) {
                if (!LuceneContext.class.isAssignableFrom(field.getType())) continue;
                try {
                    return (LuceneContext<?>) XMemory.getObject(index, XMemory.objectFieldOffset(field));
                } catch (final RuntimeException denied) {
                    throw new IllegalStateException(
                            "cannot inspect Lucene context on %s"
                                    .formatted(index.getClass().getName()), denied);
                }
            }
        }
        throw new IllegalStateException(
                "cannot inspect Lucene context on %s; unsupported Store version"
                        .formatted(index.getClass().getName()));
    }

    private static void enqueueReachable(final Object current, final ArrayDeque<Object> queue,
                                         final IdentityHashMap<Object, Boolean> seen) {
        if (current == null) return;
        if (isLeaf(current)) return;
        final Class<?> type = current.getClass();
        if (type.isArray()) {
            if (!type.componentType().isPrimitive()) {
                for (final Object element : (Object[]) current) offer(element, queue, seen);
            }
            return;
        }
        switch (current) {
            case Iterable<?> iterable -> {
                for (final Object element : iterable) offer(element, queue, seen);
                return;
            }
            case Map<?, ?> map -> {
                for (final Map.Entry<?, ?> entry : map.entrySet()) {
                    offer(entry.getKey(), queue, seen);
                    offer(entry.getValue(), queue, seen);
                }
                return;
            }
            case Map.Entry<?, ?> entry -> {
                offer(entry.getKey(), queue, seen);
                offer(entry.getValue(), queue, seen);
                return;
            }
            case Optional<?> optional -> {
                optional.ifPresent(value -> offer(value, queue, seen));
                return;
            }
            case AtomicReference<?> reference -> {
                offer(reference.get(), queue, seen);
                return;
            }
            case Reference<?> reference -> {
                /* Store runtime roots keep weak/soft handles over a large live
                 * graph that is not persistence state; following every referent
                 * would blow the object bound on ordinary Stores. Only a referent
                 * that is itself index metadata can hide an external index behind
                 * one indirection, so just that case is inspected — everything else
                 * stays pruned like before. Store rejects persistent JDK references,
                 * so a holder behind one cannot enter a durable application root.
                 * `LuceneIndex` and `VectorIndices` need no explicit branch: both
                 * extend `IndexGroup`, so that disjunct already covers them. */
                final Object referent = reference.get();
                if (referent instanceof GigaMap<?> || referent instanceof LuceneContext<?> ||
                    referent instanceof VectorIndex<?> || referent instanceof VectorIndexConfiguration ||
                    referent instanceof IndexGroup<?>) {
                    offer(referent, queue, seen);
                }
                return;
            }
            case ReferenceQueue<?> referenceQueue -> {
                return;
            }
            case Thread thread -> {
                return;
            }
            case ThreadGroup threadGroup -> {
                return;
            }
            case ClassLoader classLoader -> {
                return;
            }
            default -> {
            }
        }
        if (type.getPackageName().startsWith("java.")) {
            /* Only the explicit wrappers above are safe to unwrap. Pruning an
            * arbitrary JDK holder would turn an opaque reference to an
             * external index into a false validation success. Stateless JDK
             * implementation objects (for example Collections' comparators)
             * carry no reachable graph and can be ignored; stateful holders
             * fail closed. */
            for (Class<?> cursor = type; cursor != null && cursor != Object.class; cursor = cursor.getSuperclass()) {
                for (final Field field : cursor.getDeclaredFields()) {
                    if (!Modifier.isStatic(field.getModifiers()) && !field.getType().isPrimitive()) {
                        throw new IllegalStateException(
                                "opaque java.* holder cannot be proven index-free: " + type.getName());
                    }
                }
            }
            return;
        }
        for (Class<?> cursor = type; cursor != null && cursor != Object.class; cursor = cursor.getSuperclass()) {
            for (final Field field : cursor.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) || field.getType().isPrimitive()) continue;
                try {
                    offer(XMemory.getObject(current, XMemory.objectFieldOffset(field)), queue, seen);
                } catch (final RuntimeException denied) {
                    throw new IllegalStateException(
                            "cannot inspect reachable field %s.%s during index validation"
                                    .formatted(cursor.getName(), field.getName()), denied);
                }
            }
        }
    }

    private static void offer(final Object value, final ArrayDeque<Object> queue,
                              final IdentityHashMap<Object, Boolean> seen) {
        if (value == null || isLeaf(value)) return;
        /* Prune objects whose class cannot reach index metadata before they
         * are enqueued: a root holding thousands of plain entities validates
         * without touching the object bound. Anything unprovable stays
         * relevant, so the prune can only skip provably index-free graphs. */
        if (!isIndexRelevant(value.getClass())) return;
        if (seen.putIfAbsent(value, Boolean.TRUE) == null) queue.add(value);
    }

        /// Reports whether instances of a class could reach index metadata.
    ///
    /// Results are cached per class; the analysis is deterministic, so
    /// concurrent duplicate analyses are harmless. Metadata types themselves
    /// are relevant; leaf values are not (they never reach this gate, but the
    /// explicit branch keeps the analysis total). Interfaces, abstract types,
    /// `Object`, JDK state, and reflection failures are all relevant: the walk
    /// must inspect — or fail closed on — state it cannot prove index-free.
    ///
    /// @param type class to classify
    /// @return `true` when its instances must be traversed
    private static boolean isIndexRelevant(final Class<?> type) {
        return INDEX_RELEVANT.get(type);
    }

    private static boolean isIndexMetadataType(final Class<?> type) {
        return GigaMap.class.isAssignableFrom(type)
                || GigaIndices.class.isAssignableFrom(type)
                || IndexGroup.class.isAssignableFrom(type)
                || LuceneIndex.class.isAssignableFrom(type)
                || LuceneContext.class.isAssignableFrom(type)
                || VectorIndices.class.isAssignableFrom(type)
                || VectorIndex.class.isAssignableFrom(type)
                || VectorIndexConfiguration.class.isAssignableFrom(type);
    }

    private static boolean analyzeIndexRelevant(final Class<?> type, final HashSet<Class<?>> inProgress) {
        if (type.isPrimitive() || isLeafValue(type)) return false;
        if (isIndexMetadataType(type)) return true;
        if (type.isArray()) {
            final Class<?> component = type.componentType();
            return !component.isPrimitive() && analyzeIndexRelevant(component, inProgress);
        }
        if (type.isInterface() || Modifier.isAbstract(type.getModifiers())) return true;
        if (type.getPackageName().startsWith("java.")) return true;
        if (!inProgress.add(type)) return false;
        try {
            for (Class<?> cursor = type; cursor != null && cursor != Object.class; cursor = cursor.getSuperclass()) {
                final Field[] fields;
                try {
                    fields = cursor.getDeclaredFields();
                } catch (final RuntimeException denied) {
                    return true;
                }
                for (final Field field : fields) {
                    if (Modifier.isStatic(field.getModifiers()) || field.getType().isPrimitive()) continue;
                    final Class<?> fieldType = field.getType();
                    if (isLeafValue(fieldType)) continue;
                    if (isIndexMetadataType(fieldType)) return true;
                    if (fieldType.isArray()) {
                        final Class<?> component = fieldType.componentType();
                        if (!component.isPrimitive() && analyzeIndexRelevant(component, inProgress)) return true;
                        continue;
                    }
                    if (fieldType.isInterface() || Modifier.isAbstract(fieldType.getModifiers())
                        || fieldType.getPackageName().startsWith("java.")
                        || analyzeIndexRelevant(fieldType, inProgress)) {
                        return true;
                    }
                }
            }
            return false;
        } finally {
            inProgress.remove(type);
        }
    }

    private static boolean isLeafValue(final Class<?> type) {
        final String pkg = type.getPackageName();
        return type == String.class
                || Number.class.isAssignableFrom(type)
                || type == Boolean.class
                || type == Character.class
                || Enum.class.isAssignableFrom(type)
                || type == Class.class
                || type == UUID.class
                /* Immutable `java.time` value types: their fields are
                 * primitives, strings, or other immutable `java.time` types,
                 * so they provably cannot reach index metadata. Without this,
                 * a realistic entity graph with date/time fields would stay
                 * relevant and could exhaust the validation bound. */
                || pkg.equals("java.time")
                || pkg.startsWith("java.time.");
    }

    private static boolean isLeaf(final Object value) {
        if (value instanceof String
                || value instanceof Number
                || value instanceof Boolean
                || value instanceof Character
                || value instanceof Enum<?>
                || value instanceof Class<?>
                || value instanceof UUID) {
            return true;
        }
        final String pkg = value.getClass().getPackageName();
        return pkg.equals("java.time") || pkg.startsWith("java.time.");
    }
}
