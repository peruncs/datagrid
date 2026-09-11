package org.eclipse.datagrid.storage.distributed.aeron.wire;

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

import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Reproducible, dependency-free benchmark for the Aeron envelope staging path.
 *
 * <p>Run from the module test class path with
 * {@code java ... AeronEnvelopeBenchmark --iterations=5000 --sizes=65536,1048576}.
 * The output reports the CRC/header/copy cost only; Archive offers and forced
 * checkpoint writes must be measured by a separate environment benchmark.</p>
 */
public final class AeronEnvelopeBenchmark
{
	private AeronEnvelopeBenchmark()
	{
	}

	public static void main(final String[] arguments)
	{
		int iterations = 2_000;
		String sizes = "65536,1048576,16777216";
		for (final String argument : arguments)
		{
			if (argument.startsWith("--iterations=")) iterations = Integer.parseInt(argument.substring(13));
			else if (argument.startsWith("--sizes=")) sizes = argument.substring(8);
		}
		if (iterations <= 0) throw new IllegalArgumentException("iterations must be positive");
		final UUID clusterId = UUID.fromString("00000000-0000-0000-0000-000000000001");
		for (final String sizeText : sizes.split(","))
		{
			final int payloadLength = Integer.parseInt(sizeText.trim());
			if (payloadLength <= 0) throw new IllegalArgumentException("size must be positive");
			final UnsafeBuffer payload = new UnsafeBuffer(ByteBuffer.allocateDirect(payloadLength));
			for (int i = 0; i < payloadLength; i++) payload.putByte(i, (byte)i);
			final UnsafeBuffer target = new UnsafeBuffer(ByteBuffer.allocateDirect(
				AeronReplicationEnvelope.HEADER_LENGTH + payloadLength));
			for (int i = 0; i < 100; i++)
			{
				AeronReplicationEnvelope.encode(target, 0, clusterId, 1, i,
					AeronReplicationEnvelope.Kind.STORE_BINARY, payloadLength, 0, 1, 0, 0,
					payload, 0, payloadLength);
			}
			final long start = System.nanoTime();
			for (int i = 0; i < iterations; i++)
			{
				AeronReplicationEnvelope.encode(target, 0, clusterId, 1, i + 100,
					AeronReplicationEnvelope.Kind.STORE_BINARY, payloadLength, 0, 1, 0, 0,
					payload, 0, payloadLength);
			}
			final long elapsed = System.nanoTime() - start;
			final double seconds = elapsed / 1_000_000_000.0;
			final double bytesPerSecond = payloadLength * (double)iterations / seconds;
			System.out.printf("payload=%d iterations=%d ns/op=%.1f MiB/s=%.1f%n",
				payloadLength, iterations, elapsed / (double)iterations,
				bytesPerSecond / (1024.0 * 1024.0));
		}
	}
}
