package peruncs.datagrid.storage.distributed.types;


import org.eclipse.serializer.concurrency.XThreads;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * This handler decides when a received storage update may touch the graph.
 *
 * <p>The built-in synchronized handler applies one update at a time. Framework
 * integrations can provide a handler that uses their own cluster lock. The
 * returned stage must complete only after the updater has finished, because
 * replication cursor persistence depends on that completion boundary.</p>
 */
	@FunctionalInterface
	public interface ObjectGraphUpdateHandler
	{
		/** Runs an update when the object graph may be changed.
		 *
		 * @param updater update to run
		 * @return stage completed after the update has finished
		 */
		CompletionStage<Void> objectGraphUpdateAvailable(ObjectGraphUpdater updater);

		/** Creates a handler that serializes updates on the Store lock.
		 *
		 * @return synchronized update handler
		 */
		static ObjectGraphUpdateHandler Synchronized()
		{
			return updater ->
			{
				final CompletableFuture<Void> result = new CompletableFuture<>();
				try
				{
					XThreads.executeSynchronized(updater::updateObjectGraph);
					result.complete(null);
				}
				catch (final Throwable failure)
				{
					result.completeExceptionally(failure);
				}
				return result;
			};
		}

}
