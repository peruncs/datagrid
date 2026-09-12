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

/**
 * This handler decides when a received storage update may touch the graph.
 *
 * <p>The built-in synchronized handler applies one update at a time. Framework
 * integrations can provide a handler that uses their own cluster lock, but an
 * update must not race with a local graph write.</p>
 */
@FunctionalInterface
public interface ObjectGraphUpdateHandler
{
	void objectGraphUpdateAvailable(ObjectGraphUpdater updater);

	static ObjectGraphUpdateHandler Synchronized()
	{
		return updater -> XThreads.executeSynchronized(updater::updateObjectGraph);
	}

}
