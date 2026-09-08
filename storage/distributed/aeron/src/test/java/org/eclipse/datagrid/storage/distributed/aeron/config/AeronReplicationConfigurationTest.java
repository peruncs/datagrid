package org.eclipse.datagrid.storage.distributed.aeron.config;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AeronReplicationConfigurationTest
{
	@Test
	void defaultsExposeAeronAndDataGridLimits()
	{
		final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.defaults();
		assertEquals(16 * 1024 * 1024, configuration.termLength());
		assertEquals(1408, configuration.mtuLength());
		assertEquals(2 * 1024 * 1024, configuration.maxMessageLength());
		assertEquals(ReplicationDurabilityMode.ARCHIVE_FIRST, configuration.durabilityMode());
	}

	@Test
	void rejectsChunkThatCannotFitOneAeronMessage()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
			.termLength(64 * 1024)
			.chunkSize(8 * 1024)
			.build());
	}

	@Test
	void acceptsTunedValuesWhenTheInvariantHolds()
	{
		final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
			.termLength(32 * 1024 * 1024)
			.mtuLength(8192)
			.chunkSize(256 * 1024)
			.maxTransactionBytes(16 * 1024 * 1024)
			.build();
		assertEquals(4 * 1024 * 1024, configuration.maxMessageLength());
	}

	@Test
	void readsAllTunableLimitsFromProperties()
	{
		final java.util.Properties properties = new java.util.Properties();
		properties.setProperty(AeronReplicationConfiguration.TERM_LENGTH_PROPERTY, "1048576");
		properties.setProperty(AeronReplicationConfiguration.MTU_LENGTH_PROPERTY, "1024");
		properties.setProperty(AeronReplicationConfiguration.CHUNK_SIZE_PROPERTY, "32768");
		properties.setProperty(AeronReplicationConfiguration.MAX_TRANSACTION_BYTES_PROPERTY, "262144");
		properties.setProperty(AeronReplicationConfiguration.OFFER_TIMEOUT_NANOS_PROPERTY, "5000");
		properties.setProperty(AeronReplicationConfiguration.DURABILITY_MODE_PROPERTY, "enqueue-then-archive");

		final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.from(properties);
		assertEquals(1024, configuration.mtuLength());
		assertEquals(32768, configuration.chunkSize());
		assertEquals(262144, configuration.maxTransactionBytes());
		assertEquals(5000, configuration.offerTimeoutNanos());
		assertEquals(ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE, configuration.durabilityMode());
	}

	@Test
	void rejectsInvalidTermMtuChunkAndTimeoutValues()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
			.termLength(1000).build());
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
			.mtuLength(511).build());
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
			.chunkSize(0).build());
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
			.offerTimeoutNanos(0).build());
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
			.maxTransactionBytes(0).build());
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
			.termLength(1 << 30).chunkSize(20 * 1024 * 1024).maxTransactionBytes(20 * 1024 * 1024).build());
	}

	@Test
	void rejectsInvalidPropertiesBeforeAeronStarts()
	{
		final java.util.Properties properties = new java.util.Properties();
		properties.setProperty(AeronReplicationConfiguration.DURABILITY_MODE_PROPERTY, "unknown");
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.from(properties));
		assertThrows(NullPointerException.class, () -> AeronReplicationConfiguration.from(null));
	}

	@Test
	void reportsInvalidNumericPropertiesWithTheirKey()
	{
		final java.util.Properties properties = new java.util.Properties();
		properties.setProperty(AeronReplicationConfiguration.MTU_LENGTH_PROPERTY, "not-a-number");
		final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
			() -> AeronReplicationConfiguration.from(properties));
		assertTrue(failure.getMessage().contains(AeronReplicationConfiguration.MTU_LENGTH_PROPERTY));
	}
}
