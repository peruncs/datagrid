package peruncs.datagrid.cluster.storage.index;

import org.eclipse.serializer.concurrency.LockedExecutor;
import org.eclipse.serializer.typing.KeyValue;
import org.eclipse.store.gigamap.jvector.VectorIndex;
import org.eclipse.store.gigamap.jvector.VectorIndexConfiguration;
import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.jvector.Vectorizer;
import org.eclipse.store.gigamap.lucene.AnalyzerCreator;
import org.eclipse.store.gigamap.lucene.DocumentPopulator;
import org.eclipse.store.gigamap.lucene.LuceneContext;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.GigaMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/// Registers Store indexes that are safe to replicate with Data Grid.
///
/// The class is intentionally small. It does not copy, mirror, or repair
/// files. It makes the transaction boundary explicit: Lucene uses an embedded
/// context with manual Store-boundary commits, while JVector keeps its source
/// vectors in the Store and rebuilds its transient graph on each node.
public final class ClusterStoreIndexes {
    private static final String EXTERNAL_LUCENE_MESSAGE = "Cluster replication supports only embedded Lucene indexes; external directories are not supported";
    private static final String EXTERNAL_VECTOR_MESSAGE = "Cluster replication supports only in-graph JVector indexes; external index directories are not supported";
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
    @SuppressWarnings("unchecked") // Lucene's class token cannot retain its entity type.
    public static <E> LuceneIndex<E> registerLucene(final GigaMap<E> map, final DocumentPopulator<E> documentPopulator) {
        final GigaMap<E> checkedMap = Objects.requireNonNull(map, "map");
        return REGISTRATION.write(() ->
        {
            if (checkedMap.index().get(LuceneIndex.class) != null) {
                throw new IllegalStateException("a clustered map already has a Lucene index");
            }
            final LuceneIndex<E> registered = checkedMap.index().register(
                    LuceneIndex.Category(embeddedLuceneContext(documentPopulator))
            );
            if (registered == null) throw new IllegalStateException("failed to register clustered Lucene index");
            return registered;
        });
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
        final List<KeyValue<String, ? extends VectorIndex<?>>> snapshot = new ArrayList<>();
        for (final KeyValue<String, ? extends VectorIndex<?>> entry : indices) snapshot.add(entry);
        for (final KeyValue<String, ? extends VectorIndex<?>> entry : snapshot) {
            validateVectorConfiguration(entry.value().configuration());
        }
    }
}
