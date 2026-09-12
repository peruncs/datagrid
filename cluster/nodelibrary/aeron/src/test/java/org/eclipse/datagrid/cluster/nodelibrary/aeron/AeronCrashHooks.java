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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.BiConsumer;

/** Test-only bridge for the forked Aeron crash harness. */
public final class AeronCrashHooks
{
	private static final String STORAGE_CRASH_HOOK =
		"org.eclipse.datagrid.storage.distributed.aeron.writer.CrashHook";

	private AeronCrashHooks() { }

	/** Installs the writer and provider hooks on the calling test thread.
	 *
	 * @param hook callback that receives the crash seam name and sequence
	 */
	public static void install(final BiConsumer<String, Long> hook)
	{
		invokeStorage("install", hook);
		AeronClusterReplicationTransportProvider.setCrashHook(hook);
	}

	/** Clears all writer and provider hooks on the calling test thread. */
	public static void clear()
	{
		invokeStorage("clear");
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

	private static void invokeStorage(final String methodName, final Object... arguments)
	{
		try
		{
			final Class<?> hook = Class.forName(STORAGE_CRASH_HOOK);
			final Class<?>[] parameterTypes = arguments.length == 0
				? new Class<?>[0]
				: new Class<?>[] { BiConsumer.class };
			final Method method = hook.getDeclaredMethod(methodName, parameterTypes);
			method.setAccessible(true);
			method.invoke(null, arguments);
		}
		catch (final ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure)
		{
			throw new IllegalStateException("storage Aeron crash hook is unavailable", failure);
		}
		catch (final InvocationTargetException failure)
		{
			final Throwable cause = failure.getCause();
			if (cause instanceof final RuntimeException runtime) throw runtime;
			if (cause instanceof final Error error) throw error;
			throw new IllegalStateException("storage Aeron crash hook failed", cause);
		}
	}
}
