package org.eclipse.datagrid.storage.distributed.aeron.wire;

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

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies envelope framing, bounds, and corruption detection. */
class AeronReplicationEnvelopeTest
{
	private static final UUID CLUSTER = UUID.randomUUID();

	/** Verifies round trip preserves opaque serializer bytes. */
	@Test
	void roundTripPreservesOpaqueSerializerBytes()
	{
		final byte[] payload = new byte[] { 0, 1, 2, 127, -1 };
		final byte[] encoded = AeronReplicationEnvelope.encode(
			CLUSTER, 9, 42, AeronReplicationEnvelope.Kind.STORE_BINARY,
			100, 2, 3, 95, 0, payload
		);

		final AeronReplicationEnvelope.Envelope decoded = AeronReplicationEnvelope.decode(
			new UnsafeBuffer(encoded), 0, encoded.length
		);

		assertEquals(CLUSTER, decoded.clusterId());
		assertEquals(9, decoded.epoch());
		assertEquals(42, decoded.sequence());
		assertEquals(AeronReplicationEnvelope.Kind.STORE_BINARY, decoded.kind());
		assertEquals(100, decoded.payloadLength());
		assertEquals(2, decoded.chunkIndex());
		assertEquals(3, decoded.chunkCount());
		assertEquals(95, decoded.chunkOffset());
		assertArrayEquals(payload, decoded.payload());
	}

	/** Verifies rejection of corrupt payload before delivery. */
	@Test
	void rejectsCorruptPayloadBeforeDelivery()
	{
		final byte[] encoded = AeronReplicationEnvelope.encode(
			CLUSTER, 1, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
			1, 0, 1, 0, 0, new byte[] { 7 }
		);
		encoded[AeronReplicationEnvelope.HEADER_LENGTH] = 8;

		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.decode(
			new UnsafeBuffer(encoded), 0, encoded.length
		));
	}

	/** Verifies rejection of truncated and unknown version. */
	@Test
	void rejectsTruncatedAndUnknownVersion()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.decode(
			new UnsafeBuffer(new byte[AeronReplicationEnvelope.HEADER_LENGTH - 1]),
			0,
			AeronReplicationEnvelope.HEADER_LENGTH - 1
		));

		final byte[] encoded = AeronReplicationEnvelope.encode(
			CLUSTER, 1, 1, AeronReplicationEnvelope.Kind.COMMIT,
			0, 0, 1, 0, 0, new byte[0]
		);
		encoded[5] = 2;
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.decode(
			new UnsafeBuffer(encoded), 0, encoded.length
		));
	}

	/** Verifies rejection of invalid chunk metadata and source bounds. */
	@Test
	void rejectsInvalidChunkMetadataAndSourceBounds()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.encode(
			CLUSTER, 1, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
			1, 1, 1, 0, 0, new byte[] { 7 }
		));

		final byte[] encoded = AeronReplicationEnvelope.encode(
			CLUSTER, 1, 1, AeronReplicationEnvelope.Kind.COMMIT,
			0, 0, 1, 0, 0, new byte[0]
		);
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.decode(
			new UnsafeBuffer(encoded), 1, encoded.length
		));
		encoded[7] = 1;
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.decode(
			new UnsafeBuffer(encoded), 0, encoded.length
		));
	}

	/** Verifies rejection of nulls negative fields and marker payloads. */
	@Test
	void rejectsNullsNegativeFieldsAndMarkerPayloads()
	{
		assertThrows(NullPointerException.class, () -> AeronReplicationEnvelope.encode(
			null, 1, 1, AeronReplicationEnvelope.Kind.STORE_BINARY, 1, 0, 1, 0, 0, new byte[] {1}
		));
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.encode(
			CLUSTER, 1, 1, AeronReplicationEnvelope.Kind.STORE_BINARY, -1, 0, 1, 0, 0, new byte[0]
		));
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.encode(
			CLUSTER, 1, 1, AeronReplicationEnvelope.Kind.COMMIT, 0, 0, 1, 0, 0, new byte[] {1}
		));
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.encode(
			CLUSTER, -1, 1, AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0, 0, new byte[0]
		));
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.encode(
			CLUSTER, 1, -1, AeronReplicationEnvelope.Kind.STORE_BINARY, 0, 0, 1, 0, 0, new byte[0]
		));
	}

	/** Verifies rejection of logical payload bounds and reserved header byte. */
	@Test
	void rejectsLogicalPayloadBoundsAndReservedHeaderByte()
	{
		final byte[] encoded = AeronReplicationEnvelope.encode(
			CLUSTER, 1, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
			1, 0, 1, 0, 0, new byte[] {7}
		);
		java.nio.ByteBuffer.wrap(encoded).putInt(24, 0);
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.decode(
			new UnsafeBuffer(encoded), 0, encoded.length
		));

		final byte[] reserved = AeronReplicationEnvelope.encode(
			CLUSTER, 1, 1, AeronReplicationEnvelope.Kind.COMMIT, 0, 0, 1, 0, 0, new byte[0]
		);
		reserved[7] = 1;
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.decode(
			new UnsafeBuffer(reserved), 0, reserved.length
		));
	}

	/** Verifies rejection of truncated data payload declared by header. */
	@Test
	void rejectsTruncatedDataPayloadDeclaredByHeader()
	{
		final byte[] encoded = AeronReplicationEnvelope.encode(
			CLUSTER, 1, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
			1, 0, 1, 0, 0, new byte[] {7}
		);
		java.nio.ByteBuffer.wrap(encoded).putInt(24, 10);
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.decode(
			new UnsafeBuffer(encoded), 0, encoded.length - 1
		));
	}

	/** Verifies decodes at non zero offset without reading outside source. */
	@Test
	void decodesAtNonZeroOffsetWithoutReadingOutsideSource()
	{
		final byte[] encoded = AeronReplicationEnvelope.encode(
			CLUSTER, 3, 8, AeronReplicationEnvelope.Kind.STORE_BINARY,
			3, 0, 1, 0, 0, new byte[] {3, 4, 5}
		);
		final byte[] framed = new byte[encoded.length + 6];
		System.arraycopy(encoded, 0, framed, 3, encoded.length);
		final AeronReplicationEnvelope.Envelope result = assertDoesNotThrow(() ->
			AeronReplicationEnvelope.decode(new UnsafeBuffer(framed), 3, encoded.length));
		assertEquals(8, result.sequence());
		assertArrayEquals(new byte[] {3, 4, 5}, result.payload());
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.decode(
			new UnsafeBuffer(framed), 3, encoded.length + 1
		));
	}

	/** Verifies rejection of length larger than source without integer underflow. */
	@Test
	void rejectsLengthLargerThanSourceWithoutIntegerUnderflow()
	{
		final byte[] encoded = AeronReplicationEnvelope.encode(
			CLUSTER, 1, 1, AeronReplicationEnvelope.Kind.COMMIT,
			0, 0, 1, 0, 0, new byte[0]);
		assertThrows(IllegalArgumentException.class, () -> AeronReplicationEnvelope.decode(
			new UnsafeBuffer(encoded), 0, Integer.MAX_VALUE));
	}

	/** Verifies envelope payload accessor is defensive. */
	@Test
	void envelopePayloadAccessorIsDefensive()
	{
		final byte[] encoded = AeronReplicationEnvelope.encode(
			CLUSTER, 1, 1, AeronReplicationEnvelope.Kind.STORE_BINARY,
			1, 0, 1, 0, 0, new byte[] {7});
		final AeronReplicationEnvelope.Envelope envelope = AeronReplicationEnvelope.decode(
			new UnsafeBuffer(encoded), 0, encoded.length);
		final byte[] payload = envelope.payload();
		payload[0] = 9;
		assertArrayEquals(new byte[] {7}, envelope.payload());
	}
}
