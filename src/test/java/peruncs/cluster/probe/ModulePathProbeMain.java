package peruncs.cluster.probe;

import org.apache.lucene.document.Document;
import org.eclipse.store.gigamap.jvector.VectorIndexConfiguration;
import org.eclipse.store.gigamap.jvector.VectorIndices;
import org.eclipse.store.gigamap.jvector.VectorSimilarityFunction;
import org.eclipse.store.gigamap.jvector.Vectorizer;
import org.eclipse.store.gigamap.lucene.DocumentPopulator;
import org.eclipse.store.gigamap.lucene.LuceneContext;
import org.eclipse.store.gigamap.types.GigaMap;
import peruncs.cluster.api.ClusterIndexes;

/// Forked probe exercising the exported index facade with the cluster module resolved by JPMS.
///
/// Launched by [ModulePathRuntimeProbeTest] on a module path that resolves
/// `peruncs.cluster` as a real named module. The probe class itself
/// stays on the class path, so the run proves that an unnamed application can
/// consume the facade while the cluster and its Lucene/JVector dependencies
/// are resolved as named modules.
public final class ModulePathProbeMain {
    private ModulePathProbeMain() {
    }

    private record ProbeArticle(String title, float[] vector) {
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

    static void main(final String[] args) {
        if (ModuleLayer.boot().findModule("jdk.incubator.vector").isEmpty()) {
            throw new AssertionError("the JDK Vector API must be enabled for JVector");
        }
        final GigaMap<ProbeArticle> map = GigaMap.New();
        final ProbePopulator populator = new ProbePopulator();
        final LuceneContext<ProbeArticle> context = ClusterIndexes.embeddedLuceneContext(populator);
        if (context.directoryCreator() != null) throw new AssertionError("Lucene context must stay embedded");
        ClusterIndexes.registerLucene(map, populator);
        ClusterIndexes.addVector(map.index().register(VectorIndices.Category()), "probe-vectors",
                VectorIndexConfiguration.builder()
                        .dimension(3)
                        .similarityFunction(VectorSimilarityFunction.COSINE)
                        .build(),
                new ProbeVectorizer());
        System.out.println("MODULE-PATH-PROBE-OK");
    }
}
