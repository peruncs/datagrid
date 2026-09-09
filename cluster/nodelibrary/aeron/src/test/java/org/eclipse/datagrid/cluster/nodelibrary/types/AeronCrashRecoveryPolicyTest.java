package org.eclipse.datagrid.cluster.nodelibrary.types;

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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies that archive gaps and tails select the documented recovery policy. */
class AeronCrashRecoveryPolicyTest
{
	/** Verifies exact archive prefix can be extended. */
	@Test
	void exactArchivePrefixCanBeExtended()
	{
		assertDoesNotThrow(() -> AeronClusterReplicationTransportProvider.validateRecordingPositions(100, 100));
	}

	/** Verifies archive ahead fails closed as reseed required. */
	@Test
	void archiveAheadFailsClosedAsReseedRequired()
	{
		final IllegalStateException failure = assertThrows(IllegalStateException.class,
			() -> AeronClusterReplicationTransportProvider.validateRecordingPositions(101, 100));
		assertTrue(failure.getMessage().startsWith("RESEED_REQUIRED:"));
	}

	/** Verifies archive behind fails closed as reseed required. */
	@Test
	void archiveBehindFailsClosedAsReseedRequired()
	{
		final IllegalStateException failure = assertThrows(IllegalStateException.class,
			() -> AeronClusterReplicationTransportProvider.validateRecordingPositions(99, 100));
		assertTrue(failure.getMessage().startsWith("RESEED_REQUIRED:"));
	}

	/** Verifies active recording cannot be extended from checkpoint. */
	@Test
	void activeRecordingCannotBeExtendedFromCheckpoint()
	{
		final IllegalStateException failure = assertThrows(IllegalStateException.class,
			() -> AeronClusterReplicationTransportProvider.validateRecordingPositions(-1, 100));
		assertTrue(failure.getMessage().startsWith("RESEED_REQUIRED:"));
	}
}
