package org.eclipse.datagrid.cluster.nodelibrary.types.crashtest;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/** Independent field-level oracle for the crash child's append-only Store fixture. */
final class StoreFixture
{
	private StoreFixture()
	{
	}

	static Evidence inspect(final Path path) throws IOException
	{
		if (!Files.exists(path)) return new Evidence(List.of(), true);
		final byte[] bytes = Files.readAllBytes(path);
		final List<byte[]> records = new ArrayList<>();
		int offset = 0;
		while (offset < bytes.length)
		{
			if (bytes.length - offset < Integer.BYTES * 2) return new Evidence(records, false);
			final int length = ByteBuffer.wrap(bytes, offset, Integer.BYTES)
				.order(ByteOrder.BIG_ENDIAN).getInt();
			final int expectedCrc = ByteBuffer.wrap(bytes, offset + Integer.BYTES, Integer.BYTES)
				.order(ByteOrder.BIG_ENDIAN).getInt();
			if (length < 0 || bytes.length - offset - Integer.BYTES * 2 < length)
			{
				return new Evidence(records, false);
			}
			final byte[] payload = new byte[length];
			System.arraycopy(bytes, offset + Integer.BYTES * 2, payload, 0, length);
			if (crc(payload) != expectedCrc) return new Evidence(records, false);
			records.add(payload);
			offset += Integer.BYTES * 2 + length;
		}
		return new Evidence(records, offset == bytes.length);
	}

	static void assertRecords(final Path path, final List<byte[]> expected) throws IOException
	{
		final Evidence evidence = inspect(path);
		if (!evidence.valid() || evidence.records().size() != expected.size())
		{
			throw new AssertionError("invalid Store fixture " + path + ": actual=" +
				evidence.records().size() + ", expected=" + expected.size() + ", valid=" + evidence.valid());
		}
		for (int i = 0; i < expected.size(); i++)
		{
			if (!java.util.Arrays.equals(expected.get(i), evidence.records().get(i)))
			{
				throw new AssertionError("Store fixture payload mismatch at record " + i);
			}
		}
	}

	static boolean contains(final Path path, final byte[] payload) throws IOException
	{
		for (final byte[] record : inspect(path).records())
		{
			if (java.util.Arrays.equals(record, payload)) return true;
		}
		return false;
	}

	private static int crc(final byte[] payload)
	{
		final CRC32C crc = new CRC32C();
		crc.update(payload, 0, payload.length);
		return (int)crc.getValue();
	}

	record Evidence(List<byte[]> records, boolean valid)
	{
	}
}
