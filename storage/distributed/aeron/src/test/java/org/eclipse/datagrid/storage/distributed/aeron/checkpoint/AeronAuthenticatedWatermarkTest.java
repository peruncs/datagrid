package org.eclipse.datagrid.storage.distributed.aeron.checkpoint;

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

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Tests authentication, encoding, and monotonicity of retention watermarks. */
class AeronAuthenticatedWatermarkTest
{
	private static final UUID CLUSTER = UUID.randomUUID();
	private static final UUID GENERATION = UUID.randomUUID();
	private static final UUID READER_ONE = UUID.randomUUID();
	private static final UUID READER_TWO = UUID.randomUUID();
	private static final byte[] SECRET = "test-only-retention-secret".getBytes(java.nio.charset.StandardCharsets.UTF_8);

	@Test
	void signedWatermarkRoundTripsAndRejectsTampering()
	{
		final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.sign(
			READER_ONE, CLUSTER, GENERATION, 3, 17, 42, 4_096, SECRET);
		final AeronAuthenticatedWatermark decoded = AeronAuthenticatedWatermark.decode(watermark.encode());
		assertTrue(decoded.verify(SECRET));
		assertArrayEquals(watermark.authentication(), decoded.authentication());

		final byte[] encoded = watermark.encode();
		encoded[encoded.length - 1] ^= 1;
		assertFalse(AeronAuthenticatedWatermark.decode(encoded).verify(SECRET));
	}

	@Test
	void validatorRejectsRollbackAndIdentityConfusion()
	{
		final AeronAuthenticatedWatermark.Validator validator = new AeronAuthenticatedWatermark.Validator(SECRET);
		validator.accept(AeronAuthenticatedWatermark.sign(READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 100, SECRET));
		validator.accept(AeronAuthenticatedWatermark.sign(READER_ONE, CLUSTER, GENERATION, 3, 17, 2, 200, SECRET));
		assertThrows(IllegalStateException.class, () -> validator.accept(
			AeronAuthenticatedWatermark.sign(READER_ONE, CLUSTER, GENERATION, 3, 17, 3, 199, SECRET)));
		assertThrows(IllegalStateException.class, () -> validator.accept(
			AeronAuthenticatedWatermark.sign(READER_ONE, CLUSTER, GENERATION, 3, 17, 1, 99, SECRET)));
		assertThrows(IllegalStateException.class, () -> validator.accept(
			AeronAuthenticatedWatermark.sign(READER_ONE, UUID.randomUUID(), GENERATION, 3, 17, 3, 300, SECRET)));
	}

	/** Sequence values must remain incrementable by the writer and validator. */
	@Test
	void rejectsSequenceThatWouldOverflowNextReservation()
	{
		assertThrows(IllegalArgumentException.class, () -> new AeronAuthenticatedWatermark(
			READER_ONE, CLUSTER, GENERATION, 3, 17, Long.MAX_VALUE, 100, new byte[32]));
	}

	@Test
	void quorumRequiresEveryReaderAndAggregatesLeastProgress()
	{
		final AeronAuthenticatedWatermark.Quorum quorum =
			new AeronAuthenticatedWatermark.Quorum(java.util.Set.of(READER_ONE, READER_TWO), SECRET);
		quorum.accept(AeronAuthenticatedWatermark.sign(READER_ONE, CLUSTER, GENERATION, 3, 17, 8, 800, SECRET));
		assertThrows(IllegalStateException.class, quorum::aggregate);
		quorum.accept(AeronAuthenticatedWatermark.sign(READER_TWO, CLUSTER, GENERATION, 3, 17, 7, 700, SECRET));
		final AeronAuthenticatedWatermark aggregate = quorum.aggregate();
		assertEquals(7, aggregate.sequence());
		assertEquals(700, aggregate.position());
		assertThrows(SecurityException.class, () -> quorum.accept(
			AeronAuthenticatedWatermark.sign(UUID.randomUUID(), CLUSTER, GENERATION, 3, 17, 9, 900, SECRET)));
	}
}
