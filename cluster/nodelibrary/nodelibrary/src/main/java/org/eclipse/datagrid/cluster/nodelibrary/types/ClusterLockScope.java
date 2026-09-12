package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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


import org.eclipse.serializer.concurrency.LockedExecutor;
import org.eclipse.serializer.functional.Action;
import org.eclipse.serializer.functional.Producer;

/**
 * This scope exposes the lock used to protect one cluster's object graph.
 *
 * <p>Read actions may share the lock. Write actions exclude reads and other
 * writes. Callers should keep the action small and must not retain mutable
 * state that was read after the action returns.</p>
 */
public abstract class ClusterLockScope
{
	private final LockedExecutor executor;

	/** Creates a scope backed by the supplied lock.
	 *
	 * @param executor lock executor
	 */
	protected ClusterLockScope(final LockedExecutor executor)
	{
		this.executor = executor;
	}

	/** Runs a value-producing action under the read lock.
	 *
	 * @param <T> result type
	 * @param producer read action
	 * @return action result
	 */
	public <T> T read(final Producer<T> producer)
	{
		return this.executor.read(producer);
	}

	/** Runs an action under the read lock.
	 *
	 * @param action read action
	 */
	public void read(final Action action)
	{
		this.executor.read(action);
	}

	/** Runs a value-producing action under the write lock.
	 *
	 * @param <T> result type
	 * @param producer write action
	 * @return action result
	 */
	public <T> T write(final Producer<T> producer)
	{
		return this.executor.write(producer);
	}

	/** Runs an action under the write lock.
	 *
	 * @param action write action
	 */
	public void write(final Action action)
	{
		this.executor.write(action);
	}
}
