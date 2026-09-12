package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
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

import java.nio.file.Path;
import java.util.function.BiConsumer;

/** Explicit, reflection-free bridge used by forked metadata crash tests. */
public final class AtomicFileStoreCrashHook
{
	private AtomicFileStoreCrashHook() { }

	/** Installs a hook on the calling thread. */
	public static void install(final BiConsumer<String, Path> hook)
	{
		AtomicFileStore.setTestHook(hook);
	}

	/** Clears the calling thread's hook. */
	public static void clear()
	{
		AtomicFileStore.clearTestHook();
	}
}
