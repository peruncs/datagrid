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

	/** Returns a resettable CRC32C accumulator for the current thread.
	 *
	 * <p>The caller owns the returned accumulator until the next call on the same
	 * thread. Callers that share an accumulator must provide their own locking.</p>
	 *
	 * @return reset CRC32C accumulator
	 */
	public static CRC32C accumulator()
	{
		final CRC32C crc = LOCAL.get();
		crc.reset();
		return crc;
	}

	/** Returns the CRC32C of a byte range.
	 *
	 * @param bytes source bytes
	 * @param offset first byte to include
	 * @param length number of bytes to include
	 * @return CRC32C value
	 */
	public static int compute(final byte[] bytes, final int offset, final int length)
	{
		if (bytes == null || offset < 0 || length < 0 || offset > bytes.length - length)
		{
			throw new IllegalArgumentException("invalid CRC32C range");
		}
		final CRC32C crc = LOCAL.get();
		crc.reset();
		crc.update(bytes, offset, length);
		return (int)crc.getValue();
	}

	/** Returns the CRC32C of the complete byte array.
	 *
	 * @param bytes source bytes
	 * @return CRC32C value
	 */
	public static int compute(final byte[] bytes)
	{
		if (bytes == null) throw new IllegalArgumentException("bytes must not be null");
		return compute(bytes, 0, bytes.length);
	}

}
