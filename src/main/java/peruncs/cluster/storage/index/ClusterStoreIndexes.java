package peruncs.cluster.storage.index;

import org.eclipse.serializer.concurrency.LockedExecutor;
import org.eclipse.store.gigamap.jvector.VectorIndex;
import org.eclipse.store.gigamap.jvector.VectorIndexConfiguration;
import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.jvector.Vectorizer;
import org.eclipse.store.gigamap.lucene.AnalyzerCreator;
import org.eclipse.store.gigamap.lucene.DocumentPopulator;
import org.eclipse.store.gigamap.lucene.LuceneContext;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.cluster.storage.binary.StorageBinaryDataMerger;

import java.util.Objects;

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
/// The facade only assembles index configurations and routes enforcement
/// calls. The validation policy lives in [ClusterIndexValidation], the reader
/// refresh/rebuild lifecycle in [ClusterIndexMaintenance], and the upstream
/// reflective field layout in [StoreIndexReflection]; those types are
/// package-private because they are implementation seams, not application
/// API.
///
/// # Validation entry points
///
/// Validation is scoped to the Store being written or read: the bounded,
/// fail-closed root scan discovers the maps belonging to that Store, so a
/// still-referenced map from another Store or stream never blocks an
/// unrelated writer. A GigaMap is validated wherever the scan meets it;
/// registration exists to build the supported index kinds, not to track maps.
///
/// Reader and writer both validate the same way. [#validateStorageRoots] is
/// the single enforcement name: the reader materialization hook, node
/// startup, and the writer commit gate all call it.
///
/// See [ClusterIndexValidation] for what the scan covers and why it fails
/// closed.
public final class ClusterStoreIndexes {
        /* Registration check-then-act must not lock on the foreign index object:
         * any other code synchronizing on it could deadlock with registration,
         * and nothing else honors that monitor. One executor guards both. */
    private static final LockedExecutor REGISTRATION = LockedExecutor.New();

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
        ClusterIndexValidation.validateLuceneContext(context);
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
    /// retired safely; see [ClusterIndexValidation] for the full policy.
    ///
    /// @param configuration configuration to check
    /// @throws IllegalArgumentException if on-disk mode, a directory, or a
    ///                                  background graph mode is configured
    public static void validateVectorConfiguration(final VectorIndexConfiguration configuration) {
        ClusterIndexValidation.validateVectorConfiguration(configuration);
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
        ClusterIndexValidation.validateVectorIndexes(map);
    }

    /// Validates every index attached to one map.
    ///
    /// Every registered index group is enumerated: embedded Lucene and
    /// in-graph vector groups are validated, the core bitmap group is
    /// accepted as in-graph by construction, and any other group fails with
    /// [IllegalArgumentException]. If group enumeration cannot be proved
    /// under JPMS, validation throws [IllegalStateException] rather than
    /// assuming unknown state is safe.
    ///
    /// @param map map to validate
    /// @throws IllegalArgumentException if any attached index uses external
    ///                                  storage or belongs to an unknown category
    /// @throws IllegalStateException    if index groups cannot be enumerated completely
    public static void validateMap(final GigaMap<?> map) {
        ClusterIndexValidation.validateMap(map, null);
    }

    /// Enforcement entry point: scans a Store root object graph and rejects any
    /// index configuration that replication cannot carry.
    ///
    /// Replication ships the object graph, so an external Lucene directory or a
    /// non-persisted (on-disk or directory-backed) vector index reachable from the
    /// root would diverge across nodes. This scan catches such indexes even when
    /// they were registered directly, bypassing [#registerLucene] and
    /// [#registerVector]. See [ClusterIndexValidation] for the scan's coverage
    /// and its fail-closed rules.
    ///
    /// @param root Store root to validate
    /// @throws IllegalArgumentException if an external Lucene directory reference or a
    ///                                  non-persisted vector index is reachable from the root
    /// @throws IllegalStateException    if a large index-relevant graph cannot be inspected completely
    public static void validateGraph(final Object root) {
        ClusterIndexValidation.validateGraph(root, ClusterIndexValidation.DEFAULT_MAX_VALIDATED_OBJECTS, null);
    }

        /// Validates every Store root held by a storage connection.
    ///
    /// This is the reader materialization hook and the canonical writer
    /// startup/commit check: it runs so a writer that smuggled an external
    /// index past registration, or a reader applying a replicated batch,
    /// fails closed instead of diverging the cluster. One shared consumer
    /// performs the scan without allocating per root.
    ///
    /// @param storage storage connection owning the materialized graph
    /// @throws IllegalArgumentException if any root violates the index policy
    /// @throws IllegalStateException    if a root cannot be inspected completely
    public static void validateStorageRoots(final StorageConnection storage) {
        ClusterIndexValidation.validateStorageRoots(
                storage, ClusterIndexValidation.DEFAULT_MAX_VALIDATED_OBJECTS, null);
    }

        /// Reader-side maintenance: retires cached search views before an import
    /// batch is materialized.
    ///
    /// Package-private because only [StorageBinaryDataMerger] runs it, inside
    /// the merger's coordinator write section and before materialization. See
    /// [ClusterIndexMaintenance#refreshImportedIndexes] for the lifecycle
    /// rationale.
    ///
    /// @param storage storage connection owning the materialized graph
    static void refreshImportedIndexes(final StorageConnection storage) {
        refreshImportedIndexes(storage, ClusterIndexValidation.DEFAULT_MAX_VALIDATED_OBJECTS);
    }

        /// Reader-side maintenance: retires cached search views before an import
    /// batch is materialized, with an explicit scan bound.
    ///
    /// Package-private because only [StorageBinaryDataMerger] runs it, inside
    /// the merger's coordinator write section and before materialization. See
    /// [ClusterIndexMaintenance#refreshImportedIndexes] for the lifecycle
    /// rationale.
    ///
    /// @param storage             storage connection owning the materialized graph
    /// @param maxValidatedObjects object bound for the discovery scan
    static void refreshImportedIndexes(final StorageConnection storage, final int maxValidatedObjects) {
        ClusterIndexMaintenance.refreshImportedIndexes(storage, maxValidatedObjects);
    }

        /// Validates this Store's replicated index boundary after an import batch
        /// and eagerly rebuilds any vector search graph the refresh cleared.
    ///
    /// Package-private because only [StorageBinaryDataMerger] runs it, inside
    /// the merger's coordinator write section. See
    /// [ClusterIndexMaintenance#validateAndRebuildImportedIndexes] for the
    /// deadlock-avoidance invariant that requires the eager rebuild.
    ///
    /// @param storage             storage connection owning the materialized graph
    /// @param maxValidatedObjects object bound for the scan
    static void validateAndRebuildImportedIndexes(final StorageConnection storage, final int maxValidatedObjects) {
        ClusterIndexMaintenance.validateAndRebuildImportedIndexes(
                storage, maxValidatedObjects, new ClusterIndexValidation.ValidationScratch());
    }
}
