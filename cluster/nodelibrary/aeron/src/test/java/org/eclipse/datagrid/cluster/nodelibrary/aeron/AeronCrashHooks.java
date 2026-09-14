package org.eclipse.datagrid.cluster.nodelibrary.aeron;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Aeron Provider
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

import org.eclipse.datagrid.storage.distributed.aeron.writer.CrashHook;

import java.util.function.BiConsumer;

/** Test-only bridge for the forked Aeron crash harness. */
public final class AeronCrashHooks
{
	private AeronCrashHooks() { }

	/** Installs the writer and provider hooks on the calling test thread.
	 *
	 * @param hook callback that receives the crash seam name and sequence
	 */
	public static void install(final BiConsumer<String, Long> hook)
	{
		CrashHook.install(hook);
		AeronClusterReplicationTransportProvider.setCrashHook(hook);
	}

	/** Clears all writer and provider hooks on the calling test thread. */
	public static void clear()
	{
		CrashHook.clear();
		AeronClusterReplicationTransportProvider.clearCrashHook();
	}

	/** Returns the checkpoint sequence associated with the current write callback.
	 *
	 * @return current checkpoint sequence, or {@code -1} when none is active
	 */
	public static long currentCheckpointSequence()
	{
		return AeronClusterReplicationTransportProvider.currentCheckpointSequence();
	}

}
