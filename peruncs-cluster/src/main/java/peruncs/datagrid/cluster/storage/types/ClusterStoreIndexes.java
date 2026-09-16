package peruncs.datagrid.cluster.storage.types;

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

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
/// Every map that passes through registration or validation is also tracked
/// in an explicit weak registry ([registerMap]). The writer entry point
/// validates tracked maps directly and uses the pruned walk only to discover
/// and check untracked metadata, so a steady-state write never pays for the
/// application's data set.
public final class ClusterStoreIndexes {
    private static final String EXTERNAL_LUCENE_MESSAGE = "Cluster replication supports only embedded Lucene indexes; external directories are not supported";
    private static final String EXTERNAL_VECTOR_MESSAGE = "Cluster replication supports only in-graph JVector indexes; external index directories are not supported";
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

        /* Explicit registry of GigaMap instances known to the cluster index
         * boundary: maps registered through this class, maps that passed
         * validation, and maps applications register directly. The registry
         * is process-wide and the index policy is cluster-wide, so one
         * writer entry validates every tracked map; only proven maps are
         * ever added. Weak keys keep tracking from retaining application
         * state; all access synchronizes on the set itself. */
    private static final Set<GigaMap<?>> TRACKED_MAPS =
            Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

        /* Per-class index-relevance cache backing the traversal prune. A class
         * is relevant when its instances could reach index metadata; anything
         * unprovable (interfaces, abstract types, `Object` fields, JDK state,
         * reflection failures) stays relevant so the walk fails closed rather
         * than skipping unknown state. Deterministic per class, so concurrent
         * duplicate analyses are harmless. */
    private static final ConcurrentHashMap<Class<?>, Boolean> INDEX_RELEVANT = new ConcurrentHashMap<>();

        /* Worker-local validation scratch: the seen set and the traversal queue
         * are reused across scans instead of allocating an IdentityHashMap and
         * an ArrayDeque per root on every replicated batch. Each thread holds
         * its own scratch, and every entry point clears it before and after
         * use so traversed graph state is never retained. The scan itself is
         * iterative, so one scratch is never needed reentrantly. */
    private static final ThreadLocal<ValidationScratch> SCRATCH =
            ThreadLocal.withInitial(ValidationScratch::new);

    private static final class ValidationScratch {
        final IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
        final ArrayDeque<Object> queue = new ArrayDeque<>();
        final ArrayList<IndexGroup<?>> groups = new ArrayList<>();
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
            final LuceneIndex<E> registered = map.index().register(
                    LuceneIndex.Category(embeddedLuceneContext(documentPopulator))
            );
            if (registered == null) {
                /* Upstream reports a lost registration race with a null
                 * return rather than a throw: name the duplicate when one is
                 * now present instead of mislabeling it as an opaque failure. */
                if (luceneIndex(map) != null) {
                    throw new IllegalStateException("a clustered map already has a Lucene index");
                }
                throw new IllegalStateException("failed to register clustered Lucene index");
            }
            /* Track only proven maps: a failed registration must never enter
             * the registry, or every later writer entry would re-fail on it;
             * the root walk still covers untracked maps. */
            TRACKED_MAPS.add(map);
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
            final VectorIndex<E> added = addVectorLocked(indices, checkedName, configuration, checkedVectorizer);
            /* Track only proven maps: see [#registerLuceneLocked]. */
            TRACKED_MAPS.add(checkedMap);
            return added;
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

        /// Rejects any JVector configuration that uses an external directory.
    ///
    /// @param configuration configuration to check
    /// @throws IllegalArgumentException if on-disk mode or a directory is configured
    public static void validateVectorConfiguration(final VectorIndexConfiguration configuration) {
        final VectorIndexConfiguration checked = Objects.requireNonNull(configuration, "configuration");
        if (checked.onDisk() || checked.indexDirectory() != null) {
            throw new IllegalArgumentException(EXTERNAL_VECTOR_MESSAGE);
        }
    }

        /// Validates all vector indexes already registered on a map.
    ///
    /// This is useful after Store deserialization, when the index group was
    /// created by a persistence handler rather than by application code.
    ///
    /// @param map map to validate
    /// @throws IllegalArgumentException if a vector index uses external storage
    public static void validateVectorIndexes(final GigaMap<?> map) {
        final VectorIndices<?> indices = Objects.requireNonNull(map, "map").index().get(VectorIndices.Category());
        if (indices == null) {
            return;
        }
        validateVectorIndicesGroup(indices);
    }

    /// Tracks a map with the cluster index boundary without registering an index.
    ///
    /// The writer entry point ([validateForPublication]) validates tracked
    /// maps directly and walks Store roots only for untracked metadata, so
    /// registering the application's maps keeps every distributed write
    /// cheap. Maps registered through [#registerLucene] and [#registerVector]
    /// are tracked automatically; use this for maps that carry no cluster
    /// index, or whose indexes were attached before this class was adopted.
    /// Tracking never validates: it only names the map for later checks.
    ///
    /// @param map map to track
    public static void registerMap(final GigaMap<?> map) {
        TRACKED_MAPS.add(Objects.requireNonNull(map, "map"));
    }

    /// Validates every index attached to one map; validated maps join the tracking set.
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
        /* Track only proven maps: a map that fails validation must never
         * enter the registry, or every later writer entry would re-fail on
         * another store's rejected map. */
        TRACKED_MAPS.add(checked);
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
        validateGraphInternal(root, null);
    }

        /// Validates one root, skipping maps the caller already checked.
    ///
    /// @param root              Store root to validate
    /// @param alreadyValidated  maps to skip, or `null` to check every map
    private static void validateGraphInternal(final Object root, final Set<GigaMap<?>> alreadyValidated) {
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
                if (current instanceof GigaMap<?> map) {
                    /* Index metadata only: descending into entity payload would make
                     * every reader batch pay for the whole data set. Objects
                     * whose class cannot reach index metadata were pruned
                     * before enqueueing, so only relevant objects count above. */
                    if (alreadyValidated == null || !alreadyValidated.contains(map)) {
                        validateMap(map);
                    }
                } else if (current instanceof LuceneIndex<?> lucene) {
                    validateLuceneIndex(lucene);
                } else if (current instanceof LuceneContext<?> context) {
                    validateLuceneContext(context);
                } else if (current instanceof VectorIndices<?> group) {
                    validateVectorIndicesGroup(group);
                } else if (current instanceof VectorIndex<?> index) {
                    validateVectorConfiguration(index.configuration());
                } else if (current instanceof VectorIndexConfiguration configuration) {
                    validateVectorConfiguration(configuration);
                } else {
                    enqueueReachable(current, scratch.queue, scratch.seen);
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

        /// Writer-side enforcement entry: validates every Store root before publication.
    ///
    /// Call this at writer startup and from the writer commit path, so an index
    /// registered directly — bypassing [#registerLucene] and [#registerVector] —
    /// fails the writer before the diverging transaction is published instead of
    /// failing every reader after the fact. Tracked maps (see [registerMap])
    /// are validated directly from the registry; the bounded, fail-closed walk
    /// then covers only untracked metadata, so a steady-state write never pays
    /// for the application's data set (see the class javadoc).
    ///
    /// @param storage storage connection owning the writer graph
    /// @throws IllegalArgumentException if any root violates the index policy
    /// @throws IllegalStateException    if a root cannot be inspected completely
    public static void validateForPublication(final StorageConnection storage) {
        Objects.requireNonNull(storage, "storage");
        final Set<GigaMap<?>> validated = Collections.newSetFromMap(new IdentityHashMap<>());
        synchronized (TRACKED_MAPS) {
            validated.addAll(TRACKED_MAPS);
        }
        for (final GigaMap<?> tracked : validated) {
            validateMap(tracked);
        }
        storage.persistenceManager()
                .viewRoots()
                .iterateEntries((identifier, value) -> {
                    if (value != null) validateGraphInternal(value, validated);
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
        if (current instanceof Iterable<?> iterable) {
            for (final Object element : iterable) offer(element, queue, seen);
            return;
        }
        if (current instanceof Map<?, ?> map) {
            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                offer(entry.getKey(), queue, seen);
                offer(entry.getValue(), queue, seen);
            }
            return;
        }
        if (current instanceof Map.Entry<?, ?> entry) {
            offer(entry.getKey(), queue, seen);
            offer(entry.getValue(), queue, seen);
            return;
        }
        if (current instanceof Optional<?> optional) {
            optional.ifPresent(value -> offer(value, queue, seen));
            return;
        }
        if (current instanceof AtomicReference<?> reference) {
            offer(reference.get(), queue, seen);
            return;
        }
        if (current instanceof Reference<?> reference) {
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
        if (current instanceof ReferenceQueue<?>) return;
        if (current instanceof Thread || current instanceof ThreadGroup || current instanceof ClassLoader) return;
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
        final Boolean cached = INDEX_RELEVANT.get(type);
        if (cached != null) return cached;
        final boolean relevant = analyzeIndexRelevant(type, new HashSet<>());
        INDEX_RELEVANT.putIfAbsent(type, relevant);
        return relevant;
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
                    if (fieldType.isPrimitive() || isLeafValue(fieldType)) continue;
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
