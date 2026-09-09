package org.eclipse.datagrid.storage.distributed.aeron.writer;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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

import java.util.function.BiConsumer;

/**
 * Thread-local seam used by deterministic crash tests.
 *
 * <p>Normal writes pay only for a thread-local lookup. A test installs a hook
 * on the thread that owns the write, and must clear it when the test ends.
 * Hooks are not inherited by polling or worker threads.</p>
 *
 * <p>Throwing hooks are safe in unit tests. A blocking hook must be used only
 * by a forked child that the parent can terminate; blocking while holding a
 * publisher or coordinator monitor can otherwise deadlock the test.</p>
 */
final class CrashHook
{
	private static final ThreadLocal<BiConsumer<String, Long>> CURRENT = new ThreadLocal<>();

	private CrashHook()
	{
	}

	static void install(final BiConsumer<String, Long> hook)
	{
		if (hook == null) CURRENT.remove();
		else CURRENT.set(hook);
	}

	static void clear()
	{
		CURRENT.remove();
	}

	static void invoke(final String name, final long sequence)
	{
		final BiConsumer<String, Long> hook = CURRENT.get();
		if (hook != null) hook.accept(name, sequence);
	}
}
