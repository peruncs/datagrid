package org.eclipse.datagrid.storage.distributed.aeron.crashtest;

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

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import org.agrona.DirectBuffer;
import org.eclipse.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.CRC32C;

/**
 * Independent Archive recording scanner used by crash tests.
 *
 * <p>This class deliberately does not use the production reader or assembler.
 * It verifies the recording itself, so a reader that accidentally accepts a
 * malformed or duplicate terminal cannot make a crash test pass.</p>
 */
public final class RecordingInspector
{
	private static final int MAX_INSPECTED_PAYLOAD = 64 * 1024 * 1024;
	private RecordingInspector()
	{
	}

	public static RecordingEvidence inspect(
		final AeronArchive archive,
		final long recordingId,
		final String replayChannel,
		final int streamId,
		final UUID clusterId,
		final long epoch,
		final long timeoutMillis
	)
	{
		return inspect(archive, recordingId, replayChannel, streamId, clusterId, epoch, timeoutMillis,
			archive.getStopPosition(recordingId));
	}

	/**
	 * Inspects a recording prefix, including an active recording when the caller
	 * supplies a non-negative observed stop position. This is useful immediately
	 * after a writer crash, when the Archive has not yet marked the recording
	 * stopped but its committed prefix must still be checked.
	 *
	 * @param stopPosition prefix end returned by the caller, or a negative value
	 *                     to read the Archive stop position
	 * @return evidence for the inspected prefix
	 */
	public static RecordingEvidence inspect(
		final AeronArchive archive,
		final long recordingId,
		final String replayChannel,
		final int streamId,
		final UUID clusterId,
		final long epoch,
		final long timeoutMillis,
		final long stopPosition
	)
	{
		final long start = archive.getStartPosition(recordingId);
		final long effectiveStop = stopPosition >= 0 ? stopPosition : archive.getStopPosition(recordingId);
		if (start < 0 || effectiveStop < start)
		{
			throw new IllegalStateException("recording is not stopped: id=" + recordingId +
				" start=" + start + " stop=" + effectiveStop);
		}
		final long length = effectiveStop - start;
		if (length == 0)
		{
			return RecordingEvidence.empty(recordingId, start, effectiveStop);
		}
		try (Subscription subscription = archive.replay(recordingId, start, length, replayChannel, streamId))
		{
			final Map<Long, TransactionFrames> transactions = new HashMap<>();
			final Map<Long, TransactionFrames> dictionaries = new HashMap<>();
			final Map<Long, AeronReplicationEnvelope.Kind> terminals = new HashMap<>();
			final Map<Long, Integer> terminalCrc = new HashMap<>();
			final Map<Long, Integer> terminalLengths = new HashMap<>();
			final Map<Long, Integer> terminalChunks = new HashMap<>();
			final AtomicLong lastPosition = new AtomicLong(start);
			final AeronReplicationEnvelope.EnvelopeView view = new AeronReplicationEnvelope.EnvelopeView();
			final FragmentAssembler assembler = new FragmentAssembler((buffer, offset, fragmentLength, header) ->
			{
				lastPosition.set(Math.max(lastPosition.get(), header.position() + fragmentLength));
				inspectEnvelope(buffer, offset, fragmentLength, view, clusterId, epoch,
					transactions, dictionaries, terminals, terminalCrc, terminalLengths, terminalChunks);
			});
			final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
			while (lastPosition.get() < effectiveStop && System.nanoTime() < deadline)
			{
				if (subscription.poll(assembler, 20) == 0)
				{
					LockSupport.parkNanos(100_000L);
				}
			}
			if (lastPosition.get() < effectiveStop)
			{
				throw new AssertionError("Archive replay timed out at " + lastPosition.get() +
					" of " + effectiveStop + " for recording " + recordingId);
			}
			return evidence(recordingId, start, effectiveStop, transactions, dictionaries, terminals,
				terminalCrc, terminalLengths, terminalChunks);
		}
	}

	private static void inspectEnvelope(
		final DirectBuffer buffer,
		final int offset,
		final int length,
		final AeronReplicationEnvelope.EnvelopeView view,
		final UUID clusterId,
		final long epoch,
		final Map<Long, TransactionFrames> transactions,
		final Map<Long, TransactionFrames> dictionaries,
		final Map<Long, AeronReplicationEnvelope.Kind> terminals,
		final Map<Long, Integer> terminalCrc,
		final Map<Long, Integer> terminalLengths,
		final Map<Long, Integer> terminalChunks
	)
	{
		AeronReplicationEnvelope.decodeView(buffer, offset, length, view);
		if (!view.matches(clusterId) || view.epoch() != epoch)
		{
			throw new IllegalStateException("recording envelope identity mismatch at sequence " + view.sequence());
		}
		if (view.kind() == AeronReplicationEnvelope.Kind.COMMIT ||
			view.kind() == AeronReplicationEnvelope.Kind.ABORT)
		{
			final AeronReplicationEnvelope.Kind previous = terminals.putIfAbsent(view.sequence(), view.kind());
			if (previous != null)
			{
				throw new IllegalStateException("multiple terminal envelopes for sequence " + view.sequence());
			}
			terminalCrc.put(view.sequence(), view.commitCrc32c());
			terminalLengths.put(view.sequence(), view.payloadLength());
			terminalChunks.put(view.sequence(), view.chunkCount());
			if (view.kind() == AeronReplicationEnvelope.Kind.COMMIT &&
				(view.payloadLength() == 0 ? view.chunkCount() != 1 : view.chunkCount() <= 0))
			{
				throw new IllegalStateException("invalid commit metadata for sequence " + view.sequence());
			}
			if (view.kind() == AeronReplicationEnvelope.Kind.COMMIT && view.payloadLength() == 0)
			{
				transactions.putIfAbsent(view.sequence(), new TransactionFrames(0, 1));
			}
			return;
		}
		if (view.kind() == AeronReplicationEnvelope.Kind.TYPE_DICTIONARY)
		{
			if (terminals.containsKey(view.sequence()))
			{
				throw new IllegalStateException("dictionary follows terminal envelope for sequence " + view.sequence());
			}
			if (view.payloadLength() > MAX_INSPECTED_PAYLOAD)
			{
				throw new IllegalStateException("recording payload exceeds inspection bound: " +
					view.payloadLength());
			}
			final TransactionFrames frames = dictionaries.computeIfAbsent(view.sequence(), ignored ->
				new TransactionFrames(view.payloadLength(), view.chunkCount()));
			final byte[] payload = new byte[view.payloadLengthOnWire];
			buffer.getBytes(view.payloadOffset, payload);
			frames.put(view.chunkOffset(), payload);
			return;
		}
		if (view.kind() != AeronReplicationEnvelope.Kind.STORE_BINARY)
		{
			throw new IllegalStateException("unsupported envelope kind in recording: " + view.kind());
		}
		if (view.payloadLength() > MAX_INSPECTED_PAYLOAD)
		{
			throw new IllegalStateException("recording payload exceeds inspection bound: " +
				view.payloadLength());
		}
		if (terminals.containsKey(view.sequence()))
		{
			throw new IllegalStateException("data follows terminal envelope for sequence " + view.sequence());
		}
		final TransactionFrames frames = transactions.computeIfAbsent(view.sequence(), ignored ->
			new TransactionFrames(view.payloadLength(), view.chunkCount()));
		final byte[] payload = new byte[view.payloadLengthOnWire];
		buffer.getBytes(view.payloadOffset, payload);
		frames.put(view.chunkOffset(), payload);
	}

	private static RecordingEvidence evidence(
		final long recordingId,
		final long start,
		final long stop,
		final Map<Long, TransactionFrames> transactions,
		final Map<Long, TransactionFrames> dictionaries,
		final Map<Long, AeronReplicationEnvelope.Kind> terminals,
		final Map<Long, Integer> terminalCrc,
		final Map<Long, Integer> terminalLengths,
		final Map<Long, Integer> terminalChunks
	)
	{
		final Map<Long, Integer> payloadCrc = new HashMap<>();
		long orphanBytes = 0;
		for (final Map.Entry<Long, AeronReplicationEnvelope.Kind> entry : terminals.entrySet())
		{
			if (entry.getValue() == AeronReplicationEnvelope.Kind.COMMIT &&
				!transactions.containsKey(entry.getKey()))
			{
				throw new IllegalStateException("commit has no data chunks for sequence " + entry.getKey());
			}
		}
		for (final TransactionFrames dictionary : dictionaries.values())
		{
			dictionary.join();
		}
		final List<Long> sequences = new ArrayList<>(terminals.keySet());
		Collections.sort(sequences);
		for (int i = 1; i < sequences.size(); i++)
		{
			if (sequences.get(i) != sequences.get(i - 1) + 1)
			{
				throw new IllegalStateException("recording terminal sequence gap between " +
					sequences.get(i - 1) + " and " + sequences.get(i));
			}
		}
		for (final Map.Entry<Long, TransactionFrames> entry : transactions.entrySet())
		{
			final TransactionFrames frames = entry.getValue();
			if (!terminals.containsKey(entry.getKey()))
			{
				orphanBytes += frames.payloadLength;
				continue;
			}
			final byte[] payload = frames.join();
			if (terminalLengths.get(entry.getKey()) != frames.payloadLength ||
				terminalChunks.get(entry.getKey()) != frames.chunkCount)
			{
				throw new IllegalStateException("terminal metadata mismatch for sequence " + entry.getKey());
			}
			final int crc = crc(payload);
			payloadCrc.put(entry.getKey(), crc);
			final AeronReplicationEnvelope.Kind terminal = terminals.get(entry.getKey());
			if (terminal == AeronReplicationEnvelope.Kind.COMMIT && terminalCrc.get(entry.getKey()) != crc)
			{
				throw new IllegalStateException("commit CRC mismatch for sequence " + entry.getKey());
			}
		}
		return new RecordingEvidence(recordingId, start, stop, terminals, payloadCrc, orphanBytes);
	}

	private static int crc(final byte[] payload)
	{
		final CRC32C crc = new CRC32C();
		crc.update(payload, 0, payload.length);
		return (int)crc.getValue();
	}

	private static final class TransactionFrames
	{
		private final int payloadLength;
		private final int chunkCount;
		private final Map<Integer, byte[]> chunks = new HashMap<>();

		private TransactionFrames(final int payloadLength, final int chunkCount)
		{
			this.payloadLength = payloadLength;
			this.chunkCount = chunkCount;
		}

		private void put(final int offset, final byte[] payload)
		{
			if (this.chunks.putIfAbsent(offset, payload) != null)
			{
				throw new IllegalStateException("duplicate data chunk at offset " + offset);
			}
		}

		private byte[] join()
		{
			final byte[] result = new byte[this.payloadLength];
			int copied = 0;
			final List<Integer> offsets = new ArrayList<>(this.chunks.keySet());
			Collections.sort(offsets);
			for (final int offset : offsets)
			{
				final byte[] chunk = this.chunks.get(offset);
				if (offset != copied || chunk.length > result.length - offset)
				{
					throw new IllegalStateException("recording data chunks have a gap or overlap");
				}
				System.arraycopy(chunk, 0, result, offset, chunk.length);
				copied += chunk.length;
			}
			if (copied != this.payloadLength || this.chunks.size() != this.chunkCount)
			{
				throw new IllegalStateException("recording data chunk count/length mismatch");
			}
			return result;
		}
	}

	public record RecordingEvidence(
		long recordingId,
		long startPosition,
		long stopPosition,
		Map<Long, AeronReplicationEnvelope.Kind> terminalBySequence,
		Map<Long, Integer> payloadCrcBySequence,
		long orphanTailLength
	)
	{
		private static RecordingEvidence empty(final long recordingId, final long start, final long stop)
		{
			return new RecordingEvidence(recordingId, start, stop, Map.of(), Map.of(), 0);
		}

		public RecordingEvidence(
			final long recordingId,
			final long startPosition,
			final long stopPosition,
			final Map<Long, AeronReplicationEnvelope.Kind> terminalBySequence,
			final Map<Long, Integer> payloadCrcBySequence,
			final long orphanTailLength)
		{
			this.recordingId = recordingId;
			this.startPosition = startPosition;
			this.stopPosition = stopPosition;
			this.terminalBySequence = Map.copyOf(terminalBySequence);
			this.payloadCrcBySequence = Map.copyOf(payloadCrcBySequence);
			this.orphanTailLength = orphanTailLength;
		}
	}
}
