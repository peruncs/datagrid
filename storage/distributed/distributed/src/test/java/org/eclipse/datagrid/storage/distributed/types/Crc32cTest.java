package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
 * %%
 * Copyright (C) 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies the shared allocation-free CRC32C helper. */
class Crc32cTest
{
	/** A reused thread-local accumulator must start each message from zero. */
	@Test
	void accumulatorResetsBetweenMessages()
	{
		final var first = Crc32c.accumulator();
		first.update(new byte[] {1, 2, 3});

		final var second = Crc32c.accumulator();
		second.update(new byte[] {4, 5});

		assertEquals(Crc32c.compute(new byte[] {4, 5}), (int)second.getValue());
	}

	/** Null input is rejected consistently instead of failing while reading its length. */
	@Test
	void rejectsNullInput()
	{
		assertThrows(IllegalArgumentException.class, () -> Crc32c.compute((byte[])null));
	}
}
