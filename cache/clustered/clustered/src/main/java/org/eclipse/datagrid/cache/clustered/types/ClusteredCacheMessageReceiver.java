package org.eclipse.datagrid.cache.clustered.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered
 * %%
 * Copyright (C) 2025 - 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import org.eclipse.serializer.typing.Disposable;

/**
 * This receiver listens for remote cache invalidations.
 *
 * <p>{@link #start()} begins delivery after construction. Disposal stops
 * delivery and releases the underlying transport resources. If the polling
 * callback is still blocked when the bounded stop wait expires, disposal
 * throws and leaves the transport owned by this receiver for a retry; callers
 * must not close the cache manager underneath it. A receiver is single-use:
 * after disposal it cannot be started again; create a new receiver from the
 * provider.</p>
 */
public interface ClusteredCacheMessageReceiver extends Disposable
{
	/** Starts consuming remote invalidations. */
	void start();

	/**
	 * Returns whether the receiver is currently consuming invalidations.
	 *
	 * <p>Implementations report {@code false} before {@link #start()} and after
	 * {@link #dispose()}, and also after a terminal transport failure so a node
	 * serving stale timestamps is observable. A receiver may also stop on a
	 * valid-message application failure; callers must inspect {@link #failure()}
	 * before declaring the cache healthy.</p>
	 *
	 * @return {@code true} while the receiver is running
	 */
	boolean isRunning();

	/**
	 * Returns the terminal failure that stopped this receiver, or {@code null}
	 * while it is running or was disposed cleanly.
	 *
	 * <p>Implementations report the last transport failure so a node serving
	 * stale timestamps can be diagnosed without parsing logs.</p>
	 *
	 * @return terminal failure, or {@code null}
	 */
	RuntimeException failure();
}
