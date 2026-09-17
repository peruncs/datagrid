package peruncs.datagrid.cluster.probe;

import org.apache.lucene.document.Document;
import org.eclipse.store.gigamap.jvector.VectorIndexConfiguration;
import org.eclipse.store.gigamap.jvector.VectorSimilarityFunction;
import org.eclipse.store.gigamap.jvector.Vectorizer;
import org.eclipse.store.gigamap.lucene.DocumentPopulator;
import org.eclipse.store.gigamap.types.GigaMap;
import peruncs.datagrid.cluster.storage.types.ClusterStoreIndexes;

/// Forked probe exercising the reflective index validator inside the named module.
///
/// Launched by [ModulePathRuntimeProbeTest] on a module path that resolves
/// `peruncs.datagrid.cluster` as a real named module. The probe class itself
/// stays on the class path, so the run proves that the exported storage
/// contracts are consumable from an unnamed module and that the reflective
/// validator — which reads upstream index internals through Store's
/// offset-based memory accessor — works under JPMS access rules, where
/// class-path suites never exercise it.
public final class ModulePathProbeMain {
    private ModulePathProbeMain() {
    }

    private static final class ProbeArticle {
        final String title;
        final float[] vector;

        ProbeArticle(final String title, final float[] vector) {
            this.title = title;
            this.vector = vector;
        }
    }

    private static final class ProbePopulator extends DocumentPopulator<ProbeArticle> {
        @Override
        public void populate(final Document document, final ProbeArticle article) {
            document.add(createTextField("title", article.title));
        }
    }

    private static final class ProbeVectorizer extends Vectorizer<ProbeArticle> {
        @Override
        public float[] vectorize(final ProbeArticle article) {
            return article.vector;
        }
    }

    public static void main(final String[] args) {
        final GigaMap<ProbeArticle> map = GigaMap.New();
        ClusterStoreIndexes.registerLucene(map, new ProbePopulator());
        ClusterStoreIndexes.registerVector(map, "probe-vectors",
                VectorIndexConfiguration.builder()
                        .dimension(3)
                        .similarityFunction(VectorSimilarityFunction.COSINE)
                        .build(),
                new ProbeVectorizer());
        ClusterStoreIndexes.validateMap(map);
        System.out.println("MODULE-PATH-PROBE-OK");
    }
}
