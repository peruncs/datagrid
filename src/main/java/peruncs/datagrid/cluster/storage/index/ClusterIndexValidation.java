package peruncs.datagrid.cluster.storage.index;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.typing.KeyValue;
import org.eclipse.store.gigamap.jvector.VectorIndex;
import org.eclipse.store.gigamap.jvector.VectorIndexConfiguration;
import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.lucene.LuceneContext;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.BitmapIndices;
import org.eclipse.store.gigamap.types.GigaIndices;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.gigamap.types.IndexGroup;
import org.eclipse.store.storage.types.StorageConnection;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/// The embedded-index validation policy: which Store index kinds replication
/// can carry, and the bounded root-graph scan that proves it.
///
/// Direct registrations bypass the [ClusterStoreIndexes] registration API, so
/// enforcement also scans reachable index metadata. The scan never descends
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
final class ClusterIndexValidation {
    static final String EXTERNAL_LUCENE_MESSAGE = "Cluster replication supports only embedded Lucene indexes; external directories are not supported";
    static final String EXTERNAL_VECTOR_MESSAGE = "Cluster replication supports only in-graph JVector indexes; external index directories are not supported";
    static final String BACKGROUND_VECTOR_MESSAGE = "Cluster replication supports only synchronous JVector indexing; background graph workers cannot be retired safely on import";
    static final String UNKNOWN_INDEX_MESSAGE = "Cluster replication supports only embedded Lucene, in-graph JVector, and core bitmap indexes";

        /* Bounds one graph scan so the reader hook never pays for the data
         * set: only index-relevant objects are visited (ordinary entity graphs
         * are pruned by class before they are enqueued), and the scan stops
         * here. The merger overrides this through its configuration. */
    static final int DEFAULT_MAX_VALIDATED_OBJECTS = 4096;

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

    private ClusterIndexValidation() {
    }

    /// Reusable caller-owned scan state and per-batch discovery sinks.
    static final class ValidationScratch {
        final IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
        final ArrayDeque<Object> queue = new ArrayDeque<>();
        final ArrayList<IndexGroup<?>> groups = new ArrayList<>();
        final ArrayList<GigaMap<?>> maps = new ArrayList<>();
        /* Rebuild plan for the merged validate-and-rebuild pass. Entries carry
         * the owning map so the rebuild can hold the same monitor queries use. */
        final ArrayList<VectorGroup> vectorGroups = new ArrayList<>();
        /* One all-ones probe per discovered vector index, reused across
         * batches: the probe only triggers lazy initialization, so its vector
         * never needs recreating. Entries stay with the owning merger. */
        final IdentityHashMap<VectorIndex<?>, float[]> vectorProbes = new IdentityHashMap<>();
        final IdentityHashMap<VectorIndex<?>, Long> vectorModCounts = new IdentityHashMap<>();
        /* Reused index enumeration scratch for one vector group. */
        final ArrayList<VectorIndex<?>> vectorIndexes = new ArrayList<>();
        final ArrayList<VectorIndex<?>> dirtyVectorIndexes = new ArrayList<>();
        final IdentityHashMap<VectorIndices<?>, Boolean> rebuiltGroups = new IdentityHashMap<>();
    }

        /// One vector index group and the map that owns it.
    record VectorGroup(GigaMap<?> map, VectorIndices<?> vectors) {
    }

        /// Rejects a Lucene context that stores files outside the Store graph.
    ///
    /// @param context context to check
    /// @throws IllegalArgumentException if the context creates an external directory
    static void validateLuceneContext(final LuceneContext<?> context) {
        if (Objects.requireNonNull(context, "context").directoryCreator() != null) {
            throw new IllegalArgumentException(EXTERNAL_LUCENE_MESSAGE);
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
    static void validateVectorConfiguration(final VectorIndexConfiguration configuration) {
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
    static void validateVectorIndexes(final GigaMap<?> map) {
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
    /// @param map        map to validate
    /// @param vectorSink optional destination collecting validated vector
    ///                   groups for a later rebuild, or `null`
    /// @throws IllegalArgumentException if any attached index uses external
    ///                                  storage or belongs to an unknown category
    /// @throws IllegalStateException    if index groups cannot be enumerated completely
    static void validateMap(final GigaMap<?> map, final List<VectorGroup> vectorSink) {
        validateMap(map, vectorSink, new ValidationScratch());
    }

        /// Validates one map's registered index groups against the policy.
    ///
    /// The scratch is passed in by the graph walk, which already owns one for
    /// the whole scan.
    ///
    /// @param map        map whose groups to validate
    /// @param vectorSink optional destination collecting validated vector
    ///                   groups, or `null`
    /// @param scratch    caller-owned validation scratch
    static void validateMap(final GigaMap<?> map, final List<VectorGroup> vectorSink,
                             final ValidationScratch scratch) {
        final GigaMap<?> checked = Objects.requireNonNull(map, "map");
        Objects.requireNonNull(scratch, "scratch");
        scratch.groups.clear();
        try {
            collectIndexGroups(checked, scratch.groups);
            for (final IndexGroup<?> group : scratch.groups) {
                if (group instanceof BitmapIndices) continue;
                if (group instanceof LuceneIndex<?> lucene) {
                    validateLuceneIndex(lucene);
                } else if (group instanceof VectorIndices<?> vectors) {
                    validateVectorIndicesGroup(vectors);
                    if (vectorSink != null) vectorSink.add(new VectorGroup(checked, vectors));
                } else {
                    throw new IllegalArgumentException(
                            "%s; found unsupported index group: %s".formatted(
                                    UNKNOWN_INDEX_MESSAGE, group.getClass().getName()));
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
    static void collectIndexGroups(final GigaMap<?> map, final ArrayList<IndexGroup<?>> collected) {
        collected.clear();
        final GigaIndices<?> indices = map.index();
        final Field field = StoreIndexReflection.indexGroupsField(indices.getClass());
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
        } catch (final RuntimeException denied) {
            throw new IllegalStateException(
                    "cannot enumerate GigaMap index groups on %s"
                            .formatted(indices.getClass().getName()), denied);
        }
    }

    /// Enforcement entry point: scans a Store root object graph and rejects any
    /// index configuration that replication cannot carry.
    ///
    /// Replication ships the object graph, so an external Lucene directory or a
    /// non-persisted (on-disk or directory-backed) vector index reachable from the
    /// root would diverge across nodes. This scan catches such indexes even when
    /// they were registered directly, bypassing the facade registration methods.
    /// It stays cheap by design: attached GigaMap entity payload is never
    /// descended into (only index metadata is checked), the scan stops after a
    /// bounded number of objects, and fields that cannot be read by the Store
    /// memory accessor fail closed because an incomplete scan is not a proof of
    /// safety. A violation fails closed with [IllegalArgumentException]. The
    /// Merger traversal reuses caller-owned scratch instead of allocating per scan.
    ///
    /// @param root               Store root to validate
    /// @param maxValidatedObjects object bound for the scan
    /// @param vectorSink         optional destination collecting validated vector
    ///                           groups, or `null`
    /// @throws IllegalArgumentException if an external Lucene directory reference or a
    ///                                  non-persisted vector index is reachable from the root
    /// @throws IllegalStateException    if a large index-relevant graph cannot be inspected completely
    static void validateGraph(final Object root, final int maxValidatedObjects,
                              final List<VectorGroup> vectorSink) {
        validateGraph(root, maxValidatedObjects, vectorSink, null);
    }

    static void validateGraph(final Object root, final int maxValidatedObjects,
                              final List<VectorGroup> vectorSink, final Consumer<Object> visitedSink) {
        validateGraph(root, maxValidatedObjects, vectorSink, visitedSink, new ValidationScratch());
    }

    static void validateGraph(final Object root, final int maxValidatedObjects,
                              final List<VectorGroup> vectorSink, final Consumer<Object> visitedSink,
                              final ValidationScratch scratch) {
        if (root == null) return;
        scratch.seen.clear();
        scratch.queue.clear();
        try {
            scratch.seen.put(root, Boolean.TRUE);
            scratch.queue.add(root);
            int visited = 0;
            while (!scratch.queue.isEmpty()) {
                if (++visited > maxValidatedObjects) {
                    throw new IllegalStateException(
                            ("index validation exceeded %s index-relevant objects; raise " +
                                    "StorageBinaryDataMerger.Configuration.maxValidatedIndexObjects or narrow " +
                                    "the index-relevant graph so the replication boundary can be proven")
                                    .formatted(maxValidatedObjects));
                }
                final Object current = scratch.queue.poll();
                if (visitedSink != null) visitedSink.accept(current);
                switch (current) {
                    case GigaMap<?> map ->
                        /* Index metadata only: descending into entity payload would make
                         * every reader batch pay for the whole data set. Objects
                         * whose class cannot reach index metadata were pruned
                         * before enqueueing, so only relevant objects count above. */
                            validateMap(map, vectorSink, scratch);
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
    /// @param storage             storage connection owning the materialized graph
    /// @param maxValidatedObjects object bound for each root scan
    /// @param vectorSink          optional destination collecting validated vector
    ///                            groups, or `null`
    /// @throws IllegalArgumentException if any root violates the index policy
    /// @throws IllegalStateException    if a root cannot be inspected completely
    static void validateStorageRoots(final StorageConnection storage, final int maxValidatedObjects,
                                     final List<VectorGroup> vectorSink) {
        validateStorageRoots(storage, maxValidatedObjects, vectorSink, null);
    }

    static void validateStorageRoots(final StorageConnection storage, final int maxValidatedObjects,
                                     final List<VectorGroup> vectorSink, final Consumer<Object> visitedSink) {
        validateStorageRoots(storage, maxValidatedObjects, vectorSink, visitedSink, new ValidationScratch());
    }

    static void validateStorageRoots(final StorageConnection storage, final int maxValidatedObjects,
                                     final List<VectorGroup> vectorSink, final Consumer<Object> visitedSink,
                                     final ValidationScratch scratch) {
        final StorageConnection checked = Objects.requireNonNull(storage, "storage");
        checked.persistenceManager()
                .viewRoots()
                .iterateEntries((identifier, value) -> {
                    if (value != null) validateGraph(value, maxValidatedObjects, vectorSink, visitedSink, scratch);
                });
    }

    static void validateVectorIndicesGroup(final VectorIndices<?> group) {
        final List<KeyValue<String, ? extends VectorIndex<?>>> snapshot = new ArrayList<>();
        for (final KeyValue<String, ? extends VectorIndex<?>> entry : group) snapshot.add(entry);
        for (final KeyValue<String, ? extends VectorIndex<?>> entry : snapshot) {
            validateVectorConfiguration(entry.value().configuration());
        }
    }

        /// Validates an attached Lucene index by reading back the context it was
        /// registered with. The upstream index type exposes no public context
        /// accessor, so this locates its `LuceneContext` field reflectively and
        /// reads it through Store's offset-based memory accessor. If that layout
        /// changes, validation fails closed rather than allowing an unverified
        /// external directory.
    ///
    /// @param index attached Lucene index
    /// @throws IllegalArgumentException if the index uses an external directory
    static void validateLuceneIndex(final LuceneIndex<?> index) {
        final LuceneContext<?> context = luceneContext(index);
        if (context == null) {
            throw new IllegalArgumentException("Lucene index has no embedded context");
        }
        validateLuceneContext(context);
    }

    static LuceneContext<?> luceneContext(final LuceneIndex<?> index) {
        final Field field = StoreIndexReflection.luceneContextField(index.getClass());
        try {
            return (LuceneContext<?>) StoreIndexReflection.read(index, field);
        } catch (final RuntimeException denied) {
            throw new IllegalStateException(
                    "cannot inspect Lucene context on %s"
                            .formatted(index.getClass().getName()), denied);
        }
    }

    static void enqueueReachable(final Object current, final ArrayDeque<Object> queue,
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
            case ReferenceQueue<?> _, Thread _, ThreadGroup _, ClassLoader _ -> {
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
            if (!StoreIndexReflection.reachableFields(type).isEmpty()) {
                throw new IllegalStateException(
                        "opaque java.* holder cannot be proven index-free: " + type.getName());
            }
            return;
        }
        for (final Field field : StoreIndexReflection.reachableFields(type)) {
            try {
                offer(XMemory.getObject(current, XMemory.objectFieldOffset(field)), queue, seen);
            } catch (final RuntimeException denied) {
                throw new IllegalStateException(
                        "cannot inspect reachable field %s.%s during index validation"
                                .formatted(field.getDeclaringClass().getName(), field.getName()), denied);
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

        /// Reports whether a class is an immutable value with no reachable
    /// index metadata.
    ///
    /// @param type class to classify
    /// @return `true` for scalar, enum, class token, UUID, and `java.time` values
    static boolean isLeafValue(final Class<?> type) {
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

    /// Reports whether one value is an immutable leaf, without a second type
    /// dispatch: the object form is exactly the class form.
    ///
    /// @param value value to classify
    /// @return `true` for leaf values
    static boolean isLeaf(final Object value) {
        return value != null && isLeafValue(value.getClass());
    }
}
