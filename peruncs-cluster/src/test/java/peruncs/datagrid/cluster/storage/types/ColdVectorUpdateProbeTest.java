package peruncs.datagrid.cluster.storage.types;

import org.eclipse.store.gigamap.jvector.VectorIndexConfiguration;
import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.jvector.VectorSimilarityFunction;
import org.eclipse.store.gigamap.jvector.Vectorizer;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/// Scratch probe: repeated remove+add churn on one entity.
class ColdVectorUpdateProbeTest {
    @TempDir
    Path storagePath;

    static final class Article {
        String title;
        float[] vector;
        Article(final String title, final float[] vector) {
            this.title = title;
            this.vector = vector;
        }
    }

    static final class Root {
        GigaMap<Article> articles;
    }

    private static final class CountingVectorizer extends Vectorizer<Article> {
        int calls;
        @Override
        public float[] vectorize(final Article article) {
            this.calls++;
            return article.vector;
        }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void repeatedRemoveAddChurn() {
        final CountingVectorizer vectorizer = new CountingVectorizer();
        final Root root = new Root();
        root.articles = GigaMap.New();
        ClusterStoreIndexes.registerVector(root.articles, "probe-vectors",
                VectorIndexConfiguration.builder()
                        .dimension(3)
                        .similarityFunction(VectorSimilarityFunction.COSINE)
                        .build(),
                vectorizer);
        final List<Long> ids = new ArrayList<>();
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(root, this.storagePath)) {
            for (int i = 0; i < 40; i++) {
                ids.add(root.articles.add(new Article("a" + i, new float[]{i + 1.0f, 1.0f, 0.0f})));
            }
            root.articles.store();
            storage.storeRoot();
        }
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(this.storagePath)) {
            final StorageConnection connection = storage.createConnection();
            final Root cold = (Root) storage.root();
            final VectorIndices<Article> groups = cold.articles.index().get(VectorIndices.Category());
            final long id = 31L;
            for (int round = 0; round < 30; round++) {
                final Article entity = cold.articles.get(id);
                entity.vector = new float[]{round + 0.5f, 1.0f, 0.0f};
                ClusterStoreIndexes.refreshImportedIndexes(connection);
            }
            System.out.println("CHURN-SURVIVED vectorize=" + vectorizer.calls);
        }
    }
}
