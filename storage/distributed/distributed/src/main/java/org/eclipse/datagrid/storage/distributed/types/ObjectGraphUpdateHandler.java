package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 * 
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */


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
