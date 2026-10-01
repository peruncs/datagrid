package peruncs.cluster.storage.index;

import org.eclipse.store.gigamap.jvector.VectorIndex;
import org.eclipse.store.gigamap.jvector.VectorIndexConfiguration;
import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.jvector.VectorSimilarityFunction;
import org.eclipse.store.gigamap.jvector.Vectorizer;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.gigamap.types.IndexLocation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies the Store's public graph invalidation works on the index states a reader meets.
class VectorGraphInvalidationTest {
    @TempDir
    Path storagePath;

    static final class Article {
        final String title;
        float[] vector;

        Article(final String title, final float[] vector) {
            this.title = title;
            this.vector = vector;
        }
    }

    static final class Root {
        GigaMap<Article> articles;
    }

    private static final class ArticleVectorizer extends Vectorizer<Article> {
        @Override
        public float[] vectorize(final Article article) {
            return article.vector;
        }
    }

    private static VectorIndexConfiguration configuration() {
        return VectorIndexConfiguration.builder().dimension(3)
                .similarityFunction(VectorSimilarityFunction.COSINE).build();
    }

    /// A loaded index that was never searched has no graph yet; invalidating it must be harmless and the
    /// next search must see the entities imported meanwhile.
    @Test
    void invalidatingAColdLoadedIndexIsHarmlessAndTheNextSearchRebuilds() {
        final Root root = new Root();
        root.articles = GigaMap.New();
        ClusterStoreIndexes.registerVector(root.articles, "v", configuration(), new ArticleVectorizer());
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(root, this.storagePath)) {
            for (int i = 0; i < 10; i++) root.articles.add(new Article("a" + i, new float[]{i + 1f, 1f, 0f}));
            root.articles.store();
            storage.storeRoot();
        }
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(this.storagePath)) {
            final Root cold = storage.root();
            final VectorIndex<Article> index = cold.articles.index().<VectorIndices<Article>>get(
                    VectorIndices.Category()).get("v");
            index.invalidateGraph();
            index.invalidateGraph();
            assertEquals(3, index.search(new float[]{5f, 1f, 0f}, 3).toList().size());
        }
    }

    /// A named location keeps index files outside the Store, which a replica never receives.
    @Test
    void aNamedIndexLocationIsRejected() {
        final VectorIndexConfiguration named = VectorIndexConfiguration.builder().dimension(3)
                .similarityFunction(VectorSimilarityFunction.COSINE)
                .indexLocation(IndexLocation.Named("cluster-vectors")).build();
        assertThrows(IllegalArgumentException.class, () -> ClusterIndexValidation.validateVectorConfiguration(named));
    }
}
