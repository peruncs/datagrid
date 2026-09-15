package peruncs.datagrid.storage.distributed.index;

import org.apache.lucene.document.Document;
import org.eclipse.store.gigamap.jvector.*;
import org.eclipse.store.gigamap.lucene.DocumentPopulator;
import org.eclipse.store.gigamap.lucene.LuceneContext;
import org.eclipse.store.gigamap.lucene.LuceneIndex;
import org.eclipse.store.gigamap.types.GigaMap;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checks the cluster index boundary and the Store lifecycle it protects.
 *
 * <p>The test deliberately exercises mutations, Store reload, text search,
 * vector search, and vectorizer reuse. A directory configuration is not a
 * supported fallback, so both index families must fail before registration
 * can create one.</p>
 */
@SuppressWarnings("unchecked")
class ClusterStoreIndexesTest
{
	@TempDir
	Path storagePath;

	private static final AtomicInteger VECTORIZE_CALLS = new AtomicInteger();

	private static final class Article
	{
		String title;
		String body;
		float[] vector;

		Article(final String title, final String body, final float[] vector)
		{
			this.title = title;
			this.body = body;
			this.vector = vector;
		}
	}

	private static final class Root
	{
		GigaMap<Article> articles;
	}

	private static final class ArticlePopulator extends DocumentPopulator<Article>
	{
		@Override
		public void populate(final Document document, final Article article)
		{
			document.add(createTextField("title", article.title));
			document.add(createTextField("body", article.body));
		}
	}

	private static final class ArticleVectorizer extends Vectorizer<Article>
	{
		@Override
		public float[] vectorize(final Article article)
		{
			VECTORIZE_CALLS.incrementAndGet();
			return article.vector;
		}
	}

	private static VectorIndexConfiguration vectorConfiguration()
	{
		return VectorIndexConfiguration.builder()
			.dimension(3)
			.similarityFunction(VectorSimilarityFunction.COSINE)
			.build();
	}

	@Test
	void externalDirectoriesAreRejected()
	{
		final LuceneContext<Article> externalLucene = LuceneContext.New(
			this.storagePath.resolve("lucene"),
			new ArticlePopulator()
		);
		assertThrows(IllegalArgumentException.class,
			() -> ClusterStoreIndexes.validateLuceneContext(externalLucene));

		final VectorIndexConfiguration externalVector = VectorIndexConfiguration.builder()
			.dimension(3)
			.similarityFunction(VectorSimilarityFunction.COSINE)
			.onDisk(true)
			.indexDirectory(this.storagePath.resolve("vectors"))
			.build();
		assertThrows(IllegalArgumentException.class,
			() -> ClusterStoreIndexes.validateVectorConfiguration(externalVector));
	}

	@Test
	void embeddedIndexesFollowAddUpdateDeleteAndReload()
	{
		final Root root = new Root();
		root.articles = GigaMap.New();
		final LuceneIndex<Article> text = ClusterStoreIndexes.registerLucene(root.articles, new ArticlePopulator());
		final VectorIndices<Article> vectors = root.articles.index().register(VectorIndices.Category());
		final VectorIndex<Article> vector = ClusterStoreIndexes.addVector(
			vectors, "article-vectors", vectorConfiguration(), new ArticleVectorizer()
		);

		final long removedId;
		final long retainedId;
		try (EmbeddedStorageManager storage = EmbeddedStorage.start(root, this.storagePath))
		{
			retainedId = root.articles.add(new Article("Eclipse", "distributed storage", new float[]{1, 0, 0}));
			removedId = root.articles.add(new Article("Obsolete", "message transport", new float[]{0, 1, 0}));
			root.articles.update(retainedId, article ->
			{
				article.title = "Aeron";
				article.body = "fast transport";
				article.vector = new float[]{0.9f, 0.1f, 0};
			});
			root.articles.removeById(removedId);
			root.articles.store();

			assertEquals(1, text.query("body:fast").size());
			assertEquals(0, text.query("body:message").size());
			final VectorSearchResult<Article> result = vector.search(new float[]{1, 0, 0}, 10);
			assertEquals(1, result.size());
			assertEquals("Aeron", result.toList().get(0).entity().title);
		}

		VECTORIZE_CALLS.set(0);
		try (EmbeddedStorageManager storage = EmbeddedStorage.start(this.storagePath))
		{
			final Root reloaded = storage.root();
			assertNotNull(reloaded);
			assertFalse(reloaded.articles.isEmpty());
			ClusterStoreIndexes.validateVectorIndexes(reloaded.articles);

			final LuceneIndex<Article> reloadedText = reloaded.articles.index().get(LuceneIndex.class);
			final VectorIndices<Article> reloadedVectors = reloaded.articles.index().get(VectorIndices.Category());
			assertEquals(1, reloadedText.query("title:Aeron").size());
			assertEquals(0, reloadedText.query("title:Obsolete").size());
			final VectorSearchResult<Article> result = reloadedVectors.get("article-vectors")
				.search(new float[]{1, 0, 0}, 10);
			assertEquals(1, result.size());
			assertEquals("Aeron", result.toList().get(0).entity().title);
			assertEquals(0, VECTORIZE_CALLS.get(), "computed vectors must be loaded from Store state");
		}
	}

	@Test
	void duplicateLuceneRegistrationFailsExplicitly()
	{
		final GigaMap<Article> map = GigaMap.New();
		ClusterStoreIndexes.registerLucene(map, new ArticlePopulator());
		assertThrows(IllegalStateException.class,
			() -> ClusterStoreIndexes.registerLucene(map, new ArticlePopulator()));
	}
}
