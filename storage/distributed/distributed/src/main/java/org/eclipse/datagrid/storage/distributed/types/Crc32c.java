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

import java.util.zip.CRC32C;

/** Shared CRC32C implementation for replication wire and checkpoint data. */
public final class Crc32c
{
	private static final ThreadLocal<CRC32C> LOCAL = ThreadLocal.withInitial(CRC32C::new);
	private Crc32c()
	{
	}

	/** Returns the CRC32C of a byte range. */
	public static int compute(final byte[] bytes, final int offset, final int length)
	{
		final CRC32C crc = LOCAL.get();
		crc.reset();
		crc.update(bytes, offset, length);
		return (int)crc.getValue();
	}

	/** Returns the CRC32C of the complete byte array. */
	public static int compute(final byte[] bytes)
	{
		return compute(bytes, 0, bytes.length);
	}

}
