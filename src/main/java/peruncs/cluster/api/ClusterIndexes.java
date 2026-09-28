package peruncs.cluster.api;

import org.eclipse.store.gigamap.jvector.VectorIndex;
import org.eclipse.store.gigamap.jvector.VectorIndexConfiguration;
import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.jvector.Vectorizer;
import org.eclipse.store.gigamap.lucene.DocumentPopulator;
import org.eclipse.store.gigamap.lucene.LuceneContext;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.GigaMap;
import peruncs.cluster.storage.index.ClusterStoreIndexes;

/// Provides the supported embedded Lucene and in-graph vector indexes.
///
/// @since 1.0
public final class ClusterIndexes {
    private ClusterIndexes() {
    }

    /// Creates a Lucene context whose index is stored in the object graph.
    ///
    /// @param <E> entity type
    /// @param populator converts one entity into a Lucene document
    /// @return the embedded Lucene context
    public static <E> LuceneContext<E> embeddedLuceneContext(final DocumentPopulator<E> populator) {
        return ClusterStoreIndexes.embeddedLuceneContext(populator);
    }

    /// Registers the supported Lucene index on a GigaMap.
    ///
    /// @param <E> entity type
    /// @param map target map
    /// @param populator converts one entity into a Lucene document
    /// @return the registered index
    public static <E> LuceneIndex<E> registerLucene(final GigaMap<E> map, final DocumentPopulator<E> populator) {
        return ClusterStoreIndexes.registerLucene(map, populator);
    }

    /// Adds an in-graph vector index to a map's vector index group.
    ///
    /// @param <E> entity type
    /// @param indices map-owned vector index group
    /// @param name index name
    /// @param configuration vector index configuration
    /// @param vectorizer creates a vector from an entity
    /// @return the registered vector index
    public static <E> VectorIndex<E> addVector(
            final VectorIndices<E> indices,
            final String name,
            final VectorIndexConfiguration configuration,
            final Vectorizer<? super E> vectorizer) {
        return ClusterStoreIndexes.addVector(indices, name, configuration, vectorizer);
    }
}
