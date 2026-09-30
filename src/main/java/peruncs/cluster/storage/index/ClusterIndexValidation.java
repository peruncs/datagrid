package peruncs.cluster.storage.index;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceFunction;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinition;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinitionMember;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.persistence.types.PersistenceTypeHandler;
import org.eclipse.serializer.persistence.types.PersistenceTypeHandlerManager;
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
import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/// The embedded-index validation policy: which Store index kinds replication
/// can carry, and the bounded root-graph scan that proves it.
///
/// Direct registrations bypass the [ClusterStoreIndexes] registration API, so
/// enforcement also scans reachable index metadata. The scan uses Serializer's
/// registered type handlers, prunes types that cannot reach index metadata, and
/// never descends into GigaMap entity payloads. Unknown layouts fail closed.
/// JDK references cannot be persisted by Store; validation inspects direct index
/// referents but prunes other runtime referents to avoid Store bookkeeping.
final class ClusterIndexValidation {
    static final String EXTERNAL_LUCENE_MESSAGE = "Cluster replication supports only embedded Lucene indexes; external directories are not supported";
    static final String EXTERNAL_VECTOR_MESSAGE = "Cluster replication supports only in-graph JVector indexes; external index directories are not supported";
    static final String BACKGROUND_VECTOR_MESSAGE = "Cluster replication supports only synchronous JVector indexing; background graph workers cannot be retired safely on import";
    static final String UNKNOWN_INDEX_MESSAGE = "Cluster replication supports only embedded Lucene, in-graph JVector, and core bitmap indexes";

    /* Bounds one graph scan so ordinary entity data is pruned before enqueue. */
    static final int DEFAULT_MAX_VALIDATED_OBJECTS = NodeConfig.Limits.DEFAULT_MAX_VALIDATED_INDEX_OBJECTS;

    private ClusterIndexValidation() {
    }

    /// Caches index reachability from Serializer's runtime type descriptions.
    private static final class TypeRelevance {
        private PersistenceTypeDictionary dictionary;
        private long definitionCount = -1L;
        private long runtimeDefinitionCount;
        private long countedRuntimeDefinitions;
        private final HashMap<Class<?>, PersistenceTypeDefinition> definitions = new HashMap<>();
        private final HashMap<Class<?>, List<PersistenceTypeDefinition>> assignable = new HashMap<>();
        private final HashMap<Class<?>, Boolean> relevant = new HashMap<>();
        private final HashSet<Class<?>> resolving = new HashSet<>();
        private final Consumer<PersistenceTypeDefinition> definitionCounter = this::countRuntimeDefinition;

        boolean bind(final PersistenceTypeDictionary current) {
            /* Type ids are append-only; table size detects new classes without
             * walking the dictionary on every writer commit. Unseen classes
             * stay relevant until a full scan refreshes runtime bindings. */
            final long currentCount = current == null ? 0L : current.allTypeDefinitions().size();
            if (current == this.dictionary && currentCount == this.definitionCount) return false;
            this.load(current, currentCount);
            return true;
        }

        void refresh(final PersistenceTypeDictionary current) {
            /* A lineage can gain a runtime definition without changing the
             * dictionary's identity or number of type ids. Count runtime
             * bindings on graph scans, but rebuild caches only when they change. */
            final long currentCount = current == null ? 0L : current.allTypeDefinitions().size();
            this.countedRuntimeDefinitions = 0L;
            if (current != null) current.iterateRuntimeDefinitions(this.definitionCounter);
            if (current == this.dictionary && currentCount == this.definitionCount &&
                this.countedRuntimeDefinitions == this.runtimeDefinitionCount) return;
            this.load(current, currentCount);
        }

        private void load(final PersistenceTypeDictionary current, final long currentCount) {
            this.dictionary = current;
            this.definitionCount = currentCount;
            this.runtimeDefinitionCount = 0L;
            this.definitions.clear();
            this.assignable.clear();
            this.relevant.clear();
            this.resolving.clear();
            if (current != null) {
                current.iterateRuntimeDefinitions(definition -> {
                    if (definition != null && definition.type() != null) {
                        this.runtimeDefinitionCount++;
                        this.definitions.put(definition.type(), definition);
                    }
                });
            }
        }

        private void countRuntimeDefinition(final PersistenceTypeDefinition definition) {
            if (definition != null && definition.type() != null) this.countedRuntimeDefinitions++;
        }

        boolean isRelevant(final Class<?> type) {
            if (type == null) return true;
            if (isLeafValue(type) || type.isPrimitive()) return false;
            if (isIndexMetadata(type)) return true;
            if (type.isArray()) return this.isRelevant(type.componentType());
            if (Iterable.class.isAssignableFrom(type) || Map.class.isAssignableFrom(type) ||
                Optional.class.isAssignableFrom(type) || AtomicReference.class.isAssignableFrom(type) ||
                Reference.class.isAssignableFrom(type)) return true;
            final Boolean cached = this.relevant.get(type);
            if (cached != null) return cached;
            if (!this.resolving.add(type)) return false;
            boolean result = false;
            try {
                final List<PersistenceTypeDefinition> candidates = this.assignable.computeIfAbsent(type, declared ->
                        this.definitions.entrySet().stream()
                                .filter(entry -> declared.isAssignableFrom(entry.getKey()))
                                .map(Map.Entry::getValue)
                                .toList());
                if (candidates.isEmpty()) {
                    result = true;
                }
                for (final PersistenceTypeDefinition definition : candidates) {
                    if (isIndexMetadata(definition.type())) {
                        result = true;
                        break;
                    }
                    boolean hasMembers = false;
                    for (final PersistenceTypeDefinitionMember member : definition.instanceMembers()) {
                        hasMembers = true;
                        if (this.isRelevant(member.type())) {
                            result = true;
                            break;
                        }
                    }
                    /* Empty custom definitions can still expose references
                     * through their handler, so do not prune them by metadata. */
                    if (!hasMembers && !isLeafValue(definition.type())) result = true;
                    if (result) break;
                }
            } finally {
                this.resolving.remove(type);
            }
            this.relevant.put(type, result);
            return result;
        }

        private static boolean isIndexMetadata(final Class<?> type) {
            return GigaMap.class.isAssignableFrom(type) || IndexGroup.class.isAssignableFrom(type) ||
                    LuceneContext.class.isAssignableFrom(type) || LuceneIndex.class.isAssignableFrom(type) ||
                    VectorIndices.class.isAssignableFrom(type) || VectorIndex.class.isAssignableFrom(type) ||
                    VectorIndexConfiguration.class.isAssignableFrom(type);
        }
    }

    /// Reusable caller-owned scan state and per-batch discovery sinks.
    static final class ValidationScratch {
        final PersistenceTypeHandlerManager<Binary> typeHandlers;
        final IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
        final ArrayDeque<Object> queue = new ArrayDeque<>();
        final IdentityHashMap<Object, Boolean> groupSeen = new IdentityHashMap<>();
        final ArrayDeque<Object> groupQueue = new ArrayDeque<>();
        final ArrayList<IndexGroup<?>> groups = new ArrayList<>();
        final TypeRelevance typeRelevance = new TypeRelevance();
        int scanWork;
        int groupWork;
        final ArrayList<GigaMap<?>> maps = new ArrayList<>();
        /* Rebuild plan for the merged validate-and-rebuild pass. Entries carry
         * the owning map so the rebuild can hold the same monitor queries use. */
        final ArrayList<VectorGroup> vectorGroups = new ArrayList<>();
        /* One all-ones probe per encountered vector dimension, not per index:
         * the probe only triggers lazy initialization, so its vector never
         * needs recreating. Keying by dimension keeps no index reachable from
         * the scratch — a per-index cache would retain a rebuilt-away index
         * and, through it, its parent map and reachable graph state. */
        final HashMap<Integer, float[]> vectorProbes = new HashMap<>();
        final IdentityHashMap<VectorIndex<?>, Long> vectorModCounts = new IdentityHashMap<>();
        /* Reused index enumeration scratch for one vector group. */
        final ArrayList<VectorIndex<?>> vectorIndexes = new ArrayList<>();
        final ArrayList<VectorIndex<?>> dirtyVectorIndexes = new ArrayList<>();
        final IdentityHashMap<VectorIndices<?>, Boolean> rebuiltGroups = new IdentityHashMap<>();
        final Consumer<Object> enqueueReferenceVisitor = this::enqueueReference;
        final Consumer<Object> luceneContextVisitor = this::findLuceneContext;
        final PersistenceFunction referenceWalker = new PersistenceFunction() {
            @Override
            public <T> long apply(final T reference) {
                ValidationScratch.this.referenceVisitor.accept(reference);
                return 0L;
            }
        };
        Consumer<Object> referenceVisitor;
        ArrayDeque<Object> referenceQueue;
        IdentityHashMap<Object, Boolean> referenceSeen;
        int referenceMax;
        boolean referenceGroupWalk;
        LuceneContext<?> luceneContext;
        boolean duplicateLuceneContexts;

        ValidationScratch(final PersistenceTypeHandlerManager<Binary> typeHandlers) {
            this.typeHandlers = Objects.requireNonNull(typeHandlers, "typeHandlers");
            this.refreshTypeDefinitions();
        }

        void refreshTypeDefinitions() {
            this.typeRelevance.refresh(this.typeHandlers.typeDictionary());
        }

        void enqueueReference(final Object reference) {
            countWork(this, this.referenceMax, this.referenceGroupWalk);
            offer(reference, this, this.referenceQueue, this.referenceSeen);
        }

        void findLuceneContext(final Object reference) {
            if (reference instanceof LuceneContext<?> context) {
                if (this.luceneContext != null) this.duplicateLuceneContexts = true;
                else this.luceneContext = context;
            }
        }
    }

    /// Reusable per-writer commit-type filter state; do not share across threads.
    static final class CommitPrefilterScratch {
        /* This one-type cache is reset per commit because definitions can bind in place. */
        private PersistenceTypeDictionary dictionary;
        private final TypeRelevance typeRelevance = new TypeRelevance();
        private long lastTypeId;
        private boolean hasLastType;
        private boolean lastTypeRelevant;
        private boolean touchesIndexes;
        /* The node registers the reserved mark root before Store startup, so
         * its type id is stable before this writer accepts a commit. */
        private long replicationMarkTypeId;
        private boolean hasReplicationMarkType;
        private long expectedObjectId;
        private boolean containsObjectId;
        final EntityHeaders.TypeIdVisitor visitor = this::checkType;
        final EntityHeaders.EntityVisitor entityVisitor = this::checkEntity;

        private void checkEntity(final long typeId, final long objectId) {
            if (this.expectedObjectId >= 0L && objectId == this.expectedObjectId) this.containsObjectId = true;
            this.checkType(typeId);
        }

        private void checkType(final long typeId) {
            if (this.touchesIndexes) return;
            if (!this.hasLastType || this.lastTypeId != typeId) {
                final PersistenceTypeDefinition definition = this.dictionary == null
                        ? null : this.dictionary.lookupTypeById(typeId);
                this.lastTypeId = typeId;
                this.hasLastType = true;
                this.lastTypeRelevant = definition == null || definition.type() == null ||
                this.typeRelevance.isRelevant(definition.type());
            }
            this.touchesIndexes = this.lastTypeRelevant;
        }
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

    /// Validates one map's registered index groups against the policy.
    ///
    /// Embedded Lucene and in-graph vector groups are accepted when valid;
    /// bitmap indexes are in-graph by construction. Unknown groups and
    /// incomplete enumeration fail closed.
    ///
    /// @param map                  map whose groups to validate
    /// @param vectorSink           optional destination collecting vector groups
    /// @param scratch              caller-owned validation scratch
    /// @param maxValidatedObjects bound for visited objects and entries
    /// @throws IllegalArgumentException if an attached index is unsupported
    /// @throws IllegalStateException if traversal exceeds the configured bound
    static void validateMap(final GigaMap<?> map, final List<VectorGroup> vectorSink,
                            final ValidationScratch scratch, final int maxValidatedObjects) {
        final GigaMap<?> checked = Objects.requireNonNull(map, "map");
        Objects.requireNonNull(scratch, "scratch");
        scratch.groups.clear();
        try {
            collectIndexGroups(checked.index(), scratch, maxValidatedObjects);
            for (final IndexGroup<?> group : scratch.groups) {
                if (group instanceof BitmapIndices) continue;
                if (group instanceof LuceneIndex<?> lucene) {
                    validateLuceneIndex(lucene, scratch);
                } else if (group instanceof VectorIndices<?> vectors) {
                    validateVectorIndicesGroup(vectors, scratch, maxValidatedObjects);
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

    /// Snapshots every index group registered on a map.
    ///
    /// The upstream index API exposes lookup by category but no group
    /// enumeration. Serializer's handler supplies the references without
    /// accessing Store internals.
    ///
    /// @param indices    map index component whose groups to snapshot
    /// @param scratch    caller-owned traversal state
    /// @param maxObjects traversal work bound
    static void collectIndexGroups(final GigaIndices<?> indices,
                                           final ValidationScratch scratch, final int maxValidatedObjects) {
        scratch.groupQueue.clear();
        scratch.groupSeen.clear();
        scratch.groupWork = 0;
        scratch.groupQueue.add(indices);
        scratch.groupSeen.put(indices, Boolean.TRUE);
        try {
            while (!scratch.groupQueue.isEmpty()) {
                countGroupWork(scratch, maxValidatedObjects);
                final Object current = scratch.groupQueue.poll();
                if (current instanceof IndexGroup<?> group) {
                    scratch.groups.add(group);
                } else if (!(current instanceof GigaMap<?>)) {
                    enqueueReachable(current, scratch, maxValidatedObjects, true);
                }
            }
        } finally {
            scratch.groupQueue.clear();
            scratch.groupSeen.clear();
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
    /// bounded number of objects, and unknown Serializer descriptions fail
    /// closed because an incomplete scan is not a proof of safety. A violation
    /// fails with [IllegalArgumentException]. The
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
                              final List<VectorGroup> vectorSink, final Consumer<Object> visitedSink,
                              final ValidationScratch scratch) {
        if (root == null) return;
        scratch.refreshTypeDefinitions();
        scratch.seen.clear();
        scratch.queue.clear();
        scratch.scanWork = 0;
        try {
            scratch.seen.put(root, Boolean.TRUE);
            scratch.queue.add(root);
            while (!scratch.queue.isEmpty()) {
                countScanWork(scratch, maxValidatedObjects);
                final Object current = scratch.queue.poll();
                if (visitedSink != null) visitedSink.accept(current);
                switch (current) {
                    case GigaMap<?> map ->
                        /* Index metadata only: descending into entity payload would make
                         * every reader batch pay for the whole data set. Objects
                         * whose class cannot reach index metadata were pruned
                         * before enqueueing, so only relevant objects count above. */
                            validateMap(map, vectorSink, scratch, maxValidatedObjects);
                    case LuceneIndex<?> lucene -> validateLuceneIndex(lucene, scratch);
                    case LuceneContext<?> context -> validateLuceneContext(context);
                    case VectorIndices<?> group ->
                            validateVectorIndicesGroup(group, scratch, maxValidatedObjects);
                    case VectorIndex<?> index -> validateVectorConfiguration(index.configuration());
                    case VectorIndexConfiguration configuration -> validateVectorConfiguration(configuration);
                    case null, default -> enqueueReachable(current, scratch, maxValidatedObjects);
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
                                     final List<VectorGroup> vectorSink, final Consumer<Object> visitedSink,
                                     final ValidationScratch scratch) {
        final StorageConnection checked = Objects.requireNonNull(storage, "storage");
        if (maxValidatedObjects <= 0) throw new IllegalArgumentException("maxValidatedObjects must be positive");
        checked.persistenceManager()
                .viewRoots()
                .iterateEntries((identifier, value) -> {
                    if (value != null) validateGraph(value, maxValidatedObjects, vectorSink, visitedSink, scratch);
                });
    }

    /// Checks the changed entity types before paying for a full root-graph scan.
    static boolean commitTouchesIndexes(final Binary binary, final PersistenceTypeDictionary dictionary,
                                        final CommitPrefilterScratch scratch) {
        Objects.requireNonNull(scratch, "scratch");
        bindDictionary(scratch, dictionary);
        scratch.touchesIndexes = false;
        scratch.hasLastType = false;
        EntityHeaders.forEachTypeId(binary, scratch.visitor);
        return scratch.touchesIndexes;
    }

    static int inspectWriterCommit(final Binary binary, final PersistenceTypeDictionary dictionary,
                                   final CommitPrefilterScratch scratch, final long markObjectId) {
        Objects.requireNonNull(scratch, "scratch");
        bindDictionary(scratch, dictionary);
        scratch.touchesIndexes = false;
        scratch.hasLastType = false;
        scratch.expectedObjectId = markObjectId;
        scratch.containsObjectId = false;
        if (dictionary == null) {
            EntityHeaders.forEach(binary, scratch.entityVisitor);
        } else if (scratch.hasReplicationMarkType) {
            EntityHeaders.forEachWriterCommit(binary, scratch.entityVisitor, scratch.replicationMarkTypeId);
        } else {
            EntityHeaders.forEach(binary, scratch.entityVisitor);
        }
        return (scratch.touchesIndexes ? ClusterStoreIndexes.COMMIT_TOUCHES_INDEXES : 0) |
                (scratch.containsObjectId ? ClusterStoreIndexes.COMMIT_HAS_REPLICATION_MARK : 0);
    }

    private static void bindDictionary(final CommitPrefilterScratch scratch,
                                        final PersistenceTypeDictionary dictionary) {
        if (!scratch.typeRelevance.bind(dictionary)) return;
        scratch.dictionary = dictionary;
        scratch.hasLastType = false;
        final PersistenceTypeDefinition mark = dictionary == null
                ? null : dictionary.lookupTypeByName(ReplicationMark.class.getName());
        scratch.hasReplicationMarkType = mark != null;
        if (mark != null) scratch.replicationMarkTypeId = mark.typeId();
    }

    static void validateVectorIndicesGroup(final VectorIndices<?> group) {
        validateVectorIndicesGroup(group, null, DEFAULT_MAX_VALIDATED_OBJECTS);
    }

    static void validateVectorIndicesGroup(
            final VectorIndices<?> group,
            final ValidationScratch scratch,
            final int maximum) {
        group.accessIndices(indices -> {
            final long remaining = scratch == null ? maximum : maximum - (long) scratch.scanWork;
            if (indices.size() > remaining) throw scanLimitExceeded(maximum);
            indices.values().iterate(index -> {
                if (scratch != null) countScanWork(scratch, maximum);
                validateVectorConfiguration(index.configuration());
            });
        });
    }

    /// Validates an attached Lucene index using its registered Serializer handler.
    ///
    /// @param index attached Lucene index
    /// @throws IllegalArgumentException if the index uses an external directory
    static void validateLuceneIndex(final LuceneIndex<?> index, final ValidationScratch scratch) {
        scratch.luceneContext = null;
        scratch.duplicateLuceneContexts = false;
        LuceneContext<?> context = null;
        boolean duplicate = false;
        try {
            iterateReferences(index, scratch, scratch.luceneContextVisitor);
        } catch (final RuntimeException failure) {
            throw new IllegalStateException(
                    "cannot inspect Lucene context on %s".formatted(index.getClass().getName()), failure);
        } finally {
            context = scratch.luceneContext;
            duplicate = scratch.duplicateLuceneContexts;
            scratch.luceneContext = null;
            scratch.duplicateLuceneContexts = false;
        }
        if (duplicate) throw new IllegalStateException("Lucene index has multiple contexts");
        if (context == null) throw new IllegalArgumentException("Lucene index has no embedded context");
        validateLuceneContext(context);
    }

    static void enqueueReachable(final Object current, final ValidationScratch scratch,
                                 final int maxValidatedObjects) {
        enqueueReachable(current, scratch, maxValidatedObjects, false);
    }

    private static void enqueueReachable(final Object current, final ValidationScratch scratch,
                                         final int maxValidatedObjects, final boolean groupWalk) {
        final var queue = groupWalk ? scratch.groupQueue : scratch.queue;
        final var seen = groupWalk ? scratch.groupSeen : scratch.seen;
        if (current == null || isLeaf(current)) return;
        final Class<?> type = current.getClass();
        if (type.isArray()) {
            if (!type.componentType().isPrimitive()) {
                for (final Object element : (Object[]) current) {
                    countWork(scratch, maxValidatedObjects, groupWalk);
                    offer(element, scratch, queue, seen);
                }
            }
            return;
        }
        switch (current) {
            case Iterable<?> iterable -> {
                for (final Object element : iterable) {
                    countWork(scratch, maxValidatedObjects, groupWalk);
                    offer(element, scratch, queue, seen);
                }
                return;
            }
            case Map<?, ?> map -> {
                for (final Map.Entry<?, ?> entry : map.entrySet()) {
                    countWork(scratch, maxValidatedObjects, groupWalk);
                    countWork(scratch, maxValidatedObjects, groupWalk);
                    offer(entry.getKey(), scratch, queue, seen);
                    offer(entry.getValue(), scratch, queue, seen);
                }
                return;
            }
            case Map.Entry<?, ?> entry -> {
                countWork(scratch, maxValidatedObjects, groupWalk);
                countWork(scratch, maxValidatedObjects, groupWalk);
                offer(entry.getKey(), scratch, queue, seen);
                offer(entry.getValue(), scratch, queue, seen);
                return;
            }
            case Optional<?> optional -> {
                optional.ifPresent(value -> offer(value, scratch, queue, seen));
                return;
            }
            case AtomicReference<?> reference -> {
                offer(reference.get(), scratch, queue, seen);
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
                    offer(referent, scratch, queue, seen);
                }
                return;
            }
            case ReferenceQueue<?> _, Thread _, ThreadGroup _, ClassLoader _ -> {
                return;
            }
            default -> {
            }
        }
        /* Use Serializer's supported handler for JDK values too. It reports
         * stored references without opening their implementation fields. */
        scratch.referenceQueue = queue;
        scratch.referenceSeen = seen;
        scratch.referenceMax = maxValidatedObjects;
        scratch.referenceGroupWalk = groupWalk;
        try {
            iterateReferences(current, scratch, scratch.enqueueReferenceVisitor);
        } finally {
            scratch.referenceQueue = null;
            scratch.referenceSeen = null;
        }
    }

    @SuppressWarnings("unchecked")
    private static void iterateReferences(final Object source, final ValidationScratch scratch,
                                          final Consumer<Object> visitor) {
        scratch.referenceVisitor = visitor;
        try {
            final PersistenceTypeHandler<Binary, Object> handler =
                    (PersistenceTypeHandler<Binary, Object>) (PersistenceTypeHandler<?, ?>)
                            scratch.typeHandlers.ensureTypeHandler(source.getClass());
            handler.iterateInstanceReferences(source, scratch.referenceWalker);
        } finally {
            scratch.referenceVisitor = null;
        }
    }

    private static boolean isLeaf(final Object value) {
        return value != null && isLeafValue(value.getClass());
    }

    private static boolean isLeafValue(final Class<?> type) {
        final String pkg = type.getPackageName();
        return type == String.class || type == Byte.class || type == Short.class || type == Integer.class
                || type == Long.class || type == Float.class || type == Double.class
                || type == BigInteger.class || type == BigDecimal.class || type == Boolean.class
                || type == Character.class || type == Class.class || type == UUID.class
                || pkg.equals("java.time") || pkg.startsWith("java.time.");
    }

    static void countScanWork(final ValidationScratch scratch, final int maximum) {
        checkScanWork(++scratch.scanWork, maximum);
    }

    private static void checkScanWork(final int work, final int maximum) {
        if (work > maximum) throw scanLimitExceeded(maximum);
    }

    private static IllegalStateException scanLimitExceeded(final int maximum) {
        return new IllegalStateException(
                ("index validation exceeded %s objects and collection elements; raise " +
                        NodeConfig.Setting.INDEX_VALIDATION_MAX_OBJECTS.key() + " or narrow " +
                        "the index-relevant graph")
                        .formatted(maximum));
    }

    private static void countGroupWork(final ValidationScratch scratch, final int maximum) {
        if (++scratch.groupWork > maximum) {
            throw new IllegalStateException("index group enumeration exceeded %s objects".formatted(maximum));
        }
    }

    private static void countWork(final ValidationScratch scratch, final int maximum, final boolean groupWalk) {
        if (groupWalk) countGroupWork(scratch, maximum);
        else countScanWork(scratch, maximum);
    }

    private static void offer(final Object value, final ValidationScratch scratch,
                              final ArrayDeque<Object> queue, final IdentityHashMap<Object, Boolean> seen) {
        if (value == null || isLeaf(value)) return;
        /* Prune objects whose class cannot reach index metadata before they
         * are enqueued: a root holding thousands of plain entities validates
         * without touching the object bound. Anything unprovable stays
         * relevant, so the prune can only skip provably index-free graphs. */
        if (!scratch.typeRelevance.isRelevant(value.getClass())) return;
        if (seen.putIfAbsent(value, Boolean.TRUE) == null) queue.add(value);
    }

}
