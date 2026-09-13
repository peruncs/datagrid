package org.eclipse.datagrid.cluster.nodelibrary.aeron;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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

/** Verifies that archive gaps and tails select the documented recovery policy. */
class AeronCrashRecoveryPolicyTest
{
	private static final AeronWriterBoundary BOUNDARY = new AeronWriterBoundary(1, 7, 100);

	/** Verifies exact archive prefix can be extended. */
	@Test
	void exactArchivePrefixCanBeExtended()
	{
		assertDoesNotThrow(() -> BOUNDARY.validateArchiveStop(100));
	}

	/** Verifies archive ahead fails closed as reseed required. */
	@Test
	void archiveAheadFailsClosedAsReseedRequired()
	{
		final IllegalStateException failure = assertThrows(IllegalStateException.class,
			() -> BOUNDARY.validateArchiveStop(101));
		assertTrue(failure.getMessage().startsWith("RESEED_REQUIRED:"));
	}

	/** Verifies archive behind fails closed as reseed required. */
	@Test
	void archiveBehindFailsClosedAsReseedRequired()
	{
		final IllegalStateException failure = assertThrows(IllegalStateException.class,
			() -> BOUNDARY.validateArchiveStop(99));
		assertTrue(failure.getMessage().startsWith("RESEED_REQUIRED:"));
	}

	/** Verifies active recording cannot be extended from checkpoint. */
	@Test
	void activeRecordingCannotBeExtendedFromCheckpoint()
	{
		final IllegalStateException failure = assertThrows(IllegalStateException.class,
			() -> BOUNDARY.validateArchiveStop(-1));
		assertTrue(failure.getMessage().startsWith("RESEED_REQUIRED:"));
	}
}
