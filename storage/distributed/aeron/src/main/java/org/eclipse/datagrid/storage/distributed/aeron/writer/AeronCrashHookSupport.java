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
 * Direct bridge to deterministic writer crash seams used by forked tests in a
 * separate Maven module.
 *
 * <p>The bridge is deliberately stateless and thread-confined. Callers must
 * clear an installed hook in {@code finally}; blocking hooks must be used only
 * by forked processes that are expected to be killed.</p>
 */
public interface AeronCrashHookSupport
{
	/** Installs the calling test thread's crash hook. */
	static void install(final BiConsumer<String, Long> hook)
	{
		CrashHook.install(hook);
	}

	/** Clears the calling test thread's crash hook. */
    static void clear()
	{
		CrashHook.clear();
	}
}
