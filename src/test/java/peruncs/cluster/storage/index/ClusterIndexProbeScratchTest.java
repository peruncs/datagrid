package peruncs.cluster.storage.index;

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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the maintenance scratch never retains a vector index — or the
/// graph reachable through it — once a rebuild pass ends. Rebuild probes are
/// keyed by vector dimension, not by index instance, so a replaced or removed
/// index becomes collectible.
class ClusterIndexProbeScratchTest {
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

    /// A rebuild pass must leave no probe state behind: a per-index probe
    /// cache would strongly retain every index it ever probed, including ones
    /// whose search graph was reset or whose owning map later disappeared.
    @Test
    void rebuildPassRetainsNoProbePerIndex() throws Exception {
        final Root root = new Root();
        root.articles = GigaMap.New();
        ClusterStoreIndexes.registerVector(root.articles, "article-vectors",
                VectorIndexConfiguration.builder()
                        .dimension(3)
                        .similarityFunction(VectorSimilarityFunction.COSINE)
                        .build(),
                new ArticleVectorizer());
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(root, this.storagePath)) {
            final long id = root.articles.add(new Article("a", new float[]{1, 0, 0}));
            storage.storeRoot();
            final StorageConnection connection = storage.createConnection();
            final ClusterIndexMaintenance maintenance = new ClusterIndexMaintenance();
            /* Two passes: the first discovers and probes the index, the second
             * proves the probe did not accumulate a stale entry for the same
             * (still reachable) index across batches. */
            for (int pass = 0; pass < 2; pass++) {
                final float marker = pass + 2.0f;
                root.articles.update(id, article -> article.vector = new float[]{marker, 0, 0});
                root.articles.store();
                maintenance.beforeApply(connection, new ByteBuffer[0], 0,
                        ClusterIndexValidation.DEFAULT_MAX_VALIDATED_OBJECTS);
                maintenance.afterApply(connection, ClusterIndexValidation.DEFAULT_MAX_VALIDATED_OBJECTS);
                final ClusterIndexValidation.ValidationScratch scratch = scratchOf(maintenance);
                assertTrue(scratch.vectorProbes.isEmpty(),
                        "probe scratch must be dropped with the batch, found " + scratch.vectorProbes.keySet());
                assertTrue(scratch.vectorModCounts.isEmpty(),
                        "structural mod counts must be dropped with the batch");
                assertTrue(scratch.vectorGroups.isEmpty());
                assertTrue(scratch.rebuiltGroups.isEmpty());
            }
        }
    }

    /// A probed index whose owning map was replaced must not stay reachable
    /// through the maintenance scratch after the next pass completes.
    @Test
    void replacedIndexIsNotPinnedByProbeScratch() throws Exception {
        final Root root = new Root();
        root.articles = GigaMap.New();
        ClusterStoreIndexes.registerVector(root.articles, "article-vectors",
                VectorIndexConfiguration.builder()
                        .dimension(3)
                        .similarityFunction(VectorSimilarityFunction.COSINE)
                        .build(),
                new ArticleVectorizer());
        try (EmbeddedStorageManager storage = EmbeddedStorage.start(root, this.storagePath)) {
            root.articles.add(new Article("a", new float[]{1, 0, 0}));
            storage.storeRoot();
            final StorageConnection connection = storage.createConnection();
            final ClusterIndexMaintenance maintenance = new ClusterIndexMaintenance();
            maintenance.beforeApply(connection, new ByteBuffer[0], 0,
                    ClusterIndexValidation.DEFAULT_MAX_VALIDATED_OBJECTS);
            maintenance.afterApply(connection, ClusterIndexValidation.DEFAULT_MAX_VALIDATED_OBJECTS);
            final Object retired = root.articles.index()
                    .get(VectorIndices.Category()).get("article-vectors");
            root.articles = GigaMap.New();
            storage.storeRoot();
            maintenance.beforeApply(connection, new ByteBuffer[0], 0,
                    ClusterIndexValidation.DEFAULT_MAX_VALIDATED_OBJECTS);
            maintenance.afterApply(connection, ClusterIndexValidation.DEFAULT_MAX_VALIDATED_OBJECTS);
            /* The scratch must not pin the retired map's index across the
             * replacement; probe keys are dimensions, never index instances. */
            assertTrue(scratchOf(maintenance).vectorProbes.keySet().stream()
                            .noneMatch(key -> key == retired),
                    "the retired index must not stay reachable through probe scratch");
        }
    }

    private static ClusterIndexValidation.ValidationScratch scratchOf(final ClusterIndexMaintenance maintenance)
            throws Exception {
        final Field field = ClusterIndexMaintenance.class.getDeclaredField("scratch");
        field.setAccessible(true);
        return (ClusterIndexValidation.ValidationScratch) field.get(maintenance);
    }
}
