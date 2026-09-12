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

import org.eclipse.datagrid.storage.distributed.aeron.writer.AeronCrashHook;

import java.util.function.BiConsumer;

/**
 * Public, reflection-free bridge used only by the forked Aeron crash harness.
 * It deliberately exposes no runtime state or process-exit policy.
 */
public final class AeronCrashHooks
{
	private AeronCrashHooks() { }

	/** Installs the writer/provider hook on the calling thread. */
	public static void install(final BiConsumer<String, Long> hook)
	{
		AeronCrashHook.install(hook);
		AeronClusterReplicationTransportProvider.setCrashHook(hook);
	}

	/** Clears all writer/provider hooks on the calling thread. */
	public static void clear()
	{
		AeronCrashHook.clear();
		AeronClusterReplicationTransportProvider.clearCrashHook();
	}

	/** Returns the checkpoint sequence associated with the current write callback. */
	public static long currentCheckpointSequence()
	{
		return AeronClusterReplicationTransportProvider.currentCheckpointSequence();
	}
}
