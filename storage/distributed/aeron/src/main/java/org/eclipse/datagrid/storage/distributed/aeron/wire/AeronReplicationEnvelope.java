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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.datagrid.storage.distributed.types.Crc32c;

import java.nio.ByteOrder;
import java.util.UUID;
import java.util.zip.CRC32C;

/**
 * The small envelope around one piece of a replicated Store transaction.
 *
 * <p>Eclipse Serializer still owns the Store binary format. This type adds
 * only the identity, order, chunk location, and checksum needed to move that
 * binary through Aeron. A transaction is visible only after its commit marker
 * and full-binary checksum pass validation.</p>
 *
 * <p>The checksum detects accidental corruption. It is not authentication, so
 * deployments that cross a trust boundary must protect the Aeron channels.</p>
 */
public final class AeronReplicationEnvelope
{
	/* The reusable checksum state is safe only for the polling thread that owns
	 * the current decode. Do not re-enter checksum computation from a callback. */
	private static final ThreadLocal<CRC32C> DIRECT_CRC = ThreadLocal.withInitial(CRC32C::new);
	private static final int CRC_SCRATCH_BYTES = 16 * 1024;
	private static final ThreadLocal<byte[]> CRC_SCRATCH =
		ThreadLocal.withInitial(() -> new byte[CRC_SCRATCH_BYTES]);
	public static final int MAGIC = 0x44474152; // DGAR
	/** Wire version with a checksum covering every decision-bearing header field. */
	public static final short VERSION = 2;
	/** Header bytes, including the final header CRC32C at offset 64. */
	public static final int HEADER_LENGTH = 68;
	private static final int HEADER_CRC_OFFSET = 64;

	/** Identifies the data or terminal marker carried by an envelope. */
	public enum Kind
	{
		/** A chunk of the type dictionary that precedes Store data. */
		TYPE_DICTIONARY(1),
		/** A chunk of one Store binary. */
		STORE_BINARY(2),
		/**
		 * The terminal witness for a published transaction. Its payload length
		 * and chunk count repeat the assembled binary metadata; its commit CRC
		 * validates the complete binary.
		 */
		COMMIT(3),
		/**
		 * The terminal witness for a rejected transaction. Its payload length
		 * and chunk count repeat the prepared metadata, while its CRC is zero.
		 */
		ABORT(4);

		private final int code;
		Kind(final int code) { this.code = code; }
		private static Kind fromCode(final int code)
		{
			return switch (code)
			{
			case 1 -> TYPE_DICTIONARY;
			case 2 -> STORE_BINARY;
			case 3 -> COMMIT;
			case 4 -> ABORT;
			default -> throw new IllegalArgumentException("unknown envelope kind=" + code);
			};
		}
	}

	/** Returns whether a wire kind carries transaction data rather than a terminal marker. */
	public static boolean isPayloadKindCode(final int code)
	{
		return code == Kind.TYPE_DICTIONARY.code || code == Kind.STORE_BINARY.code;
	}

	private AeronReplicationEnvelope()
	{
	}

	/**
	 * Encodes an envelope into a new byte array.
	 *
	 * <p>This allocation-friendly form is intended for tests and compatibility
	 * callers. The writer uses the direct-buffer overload to keep Store data off
	 * the heap.</p>
	 */
	public static byte[] encode(
		final UUID clusterId,
		final long epoch,
		final long sequence,
		final Kind kind,
		final int payloadLength,
		final int chunkIndex,
		final int chunkCount,
		final int chunkOffset,
		final int commitCrc32c,
		final byte[] payload
	)
	{
		if (payload == null) throw new NullPointerException("payload");
		final byte[] encoded = new byte[Math.addExact(HEADER_LENGTH, payload.length)];
		final UnsafeBuffer target = new UnsafeBuffer(encoded);
		encode(target, 0, clusterId, epoch, sequence, kind, payloadLength, chunkIndex, chunkCount,
			chunkOffset, commitCrc32c, new UnsafeBuffer(payload), 0, payload.length);
		return encoded;
	}

	/**
	 * Encodes directly into a caller-owned Agrona buffer. The writer uses this
	 * form so each chunk can be offered without first creating a heap frame.
	 *
	 * @param target destination buffer
	 * @param targetOffset destination offset
	 * @param clusterId replication cluster identity
	 * @param epoch writer epoch
	 * @param sequence transaction sequence
	 * @param kind envelope kind
	 * @param payloadLength logical, unchunked payload length
	 * @param chunkIndex zero-based chunk index
	 * @param chunkCount total chunk count
	 * @param chunkOffset offset within the logical payload
	 * @param commitCrc32c complete Store-binary CRC carried by terminal markers
	 * @param payload source payload buffer
	 * @param payloadOffset source offset
	 * @param chunkLength bytes copied into the envelope
	 * @return number of bytes written
	 * @throws IllegalArgumentException when bounds or protocol fields are invalid
	 */
	public static int encode(
		final MutableDirectBuffer target,
		final int targetOffset,
		final UUID clusterId,
		final long epoch,
		final long sequence,
		final Kind kind,
		final int payloadLength,
		final int chunkIndex,
		final int chunkCount,
		final int chunkOffset,
		final int commitCrc32c,
		final DirectBuffer payload,
		final int payloadOffset,
		final int chunkLength
	)
	{
		validate(clusterId, epoch, sequence, kind, payloadLength, chunkIndex, chunkCount,
			chunkOffset, commitCrc32c, payload, payloadOffset, chunkLength);
		final int encodedLength = Math.addExact(HEADER_LENGTH, chunkLength);
		if (target == null || targetOffset < 0 || targetOffset > target.capacity() - encodedLength)
		{
			throw new IllegalArgumentException("target buffer is too small");
		}
		target.putInt(targetOffset, MAGIC, ByteOrder.BIG_ENDIAN);
		target.putShort(targetOffset + 4, VERSION, ByteOrder.BIG_ENDIAN);
		target.putByte(targetOffset + 6, (byte)kind.code);
		target.putByte(targetOffset + 7, (byte)0);
		target.putLong(targetOffset + 8, epoch, ByteOrder.BIG_ENDIAN);
		target.putLong(targetOffset + 16, sequence, ByteOrder.BIG_ENDIAN);
		target.putInt(targetOffset + 24, payloadLength, ByteOrder.BIG_ENDIAN);
		target.putInt(targetOffset + 28, chunkIndex, ByteOrder.BIG_ENDIAN);
		target.putInt(targetOffset + 32, chunkCount, ByteOrder.BIG_ENDIAN);
		target.putInt(targetOffset + 36, chunkOffset, ByteOrder.BIG_ENDIAN);
		target.putInt(targetOffset + 40, crc32c(payload, payloadOffset, chunkLength), ByteOrder.BIG_ENDIAN);
		target.putInt(targetOffset + 44, commitCrc32c, ByteOrder.BIG_ENDIAN);
		target.putLong(targetOffset + 48, clusterId.getMostSignificantBits(), ByteOrder.BIG_ENDIAN);
		target.putLong(targetOffset + 56, clusterId.getLeastSignificantBits(), ByteOrder.BIG_ENDIAN);
		target.putInt(targetOffset + HEADER_CRC_OFFSET,
			crc32c(target, targetOffset, HEADER_CRC_OFFSET), ByteOrder.BIG_ENDIAN);
		// The publisher can stage a multi-buffer chunk directly in the destination
		// payload area. Avoid copying that already-staged range a second time.
		if (payload != target || payloadOffset != targetOffset + HEADER_LENGTH)
		{
			target.putBytes(targetOffset + HEADER_LENGTH, payload, payloadOffset, chunkLength);
		}
		return encodedLength;
	}

	private static void validate(
		final UUID clusterId,
		final long epoch,
		final long sequence,
		final Kind kind,
		final int payloadLength,
		final int chunkIndex,
		final int chunkCount,
		final int chunkOffset,
		final int commitCrc32c,
		final DirectBuffer payload,
		final int payloadOffset,
		final int chunkLength
	)
	{
		if (clusterId == null || kind == null || payload == null)
			throw new NullPointerException("clusterId, kind, and payload are required");
		if (epoch < 0 || sequence < 0 || sequence == Long.MAX_VALUE || payloadLength < 0 || chunkIndex < 0 || chunkCount <= 0 ||
			chunkIndex >= chunkCount || chunkOffset < 0 || payloadOffset < 0 || chunkLength < 0 ||
			payloadOffset > payload.capacity() - chunkLength)
		{
			throw new IllegalArgumentException("invalid envelope field");
		}
		if (chunkLength > payloadLength ||
			(kind == Kind.COMMIT || kind == Kind.ABORT) && chunkLength != 0)
		{
			throw new IllegalArgumentException("invalid payload length");
		}
		if ((kind == Kind.COMMIT || kind == Kind.ABORT) &&
			(chunkIndex != 0 || chunkOffset != 0 || kind == Kind.ABORT && commitCrc32c != 0))
		{
			throw new IllegalArgumentException("non-canonical terminal marker");
		}
		if (kind != Kind.COMMIT && kind != Kind.ABORT)
		{
			/* A zero-length data payload is represented by exactly one empty chunk.
			 * Accepting a multi-chunk empty transaction would let a sender reserve a
			 * sequence that can never reach a valid terminal state and would leave the
			 * assembler waiting forever for bytes that do not exist. */
			if (payloadLength == 0 && (chunkIndex != 0 || chunkCount != 1 || chunkOffset != 0))
			{
				throw new IllegalArgumentException("empty payload must use one canonical chunk");
			}
			if (payloadLength > 0 && chunkLength == 0)
			{
				throw new IllegalArgumentException("non-empty payload chunks must carry bytes");
			}
			final long end = (long)chunkOffset + chunkLength;
			if (end > payloadLength || chunkIndex == chunkCount - 1 && end != payloadLength)
			{
				throw new IllegalArgumentException("chunk does not match logical payload length");
			}
		}
	}

	/**
	 * Decodes one complete envelope and copies its payload.
	 *
	 * <p>Use the reusable view overload in a reader. This method is kept for
	 * tests and callers that need an owned byte array.</p>
	 */
	public static Envelope decode(final DirectBuffer source, final int offset, final int length)
	{
		final EnvelopeView view = decodeView(source, offset, length);
		final byte[] payload = new byte[view.payloadLengthOnWire];
		source.getBytes(view.payloadOffset, payload);
		return new Envelope(view.clusterId(), view.epoch, view.sequence, view.kind, view.payloadLength,
			view.chunkIndex, view.chunkCount, view.chunkOffset, view.commitCrc32c, payload);
	}

	/** Convenience form for codec tests; production readers use the reusable view. */
	static EnvelopeView decodeView(final DirectBuffer source, final int offset, final int length)
	{
		return decodeView(source, offset, length, new EnvelopeView());
	}

	/**
	 * Decodes into a caller-provided view. The view is valid until the next call
	 * that reuses it, so a reader must copy any payload it keeps.
	 */
	public static EnvelopeView decodeView(
		final DirectBuffer source,
		final int offset,
		final int length,
		final EnvelopeView view
	)
	{
		if (view == null) throw new NullPointerException("view");
		if (source == null || offset < 0 || length < HEADER_LENGTH || length > source.capacity() ||
			offset > source.capacity() - length)
		{
			throw new IllegalArgumentException("truncated envelope");
		}
		if (source.getInt(offset, ByteOrder.BIG_ENDIAN) != MAGIC ||
			source.getShort(offset + 4, ByteOrder.BIG_ENDIAN) != VERSION)
		{
			throw new IllegalArgumentException("unknown DataGrid envelope");
		}
		if (source.getInt(offset + HEADER_CRC_OFFSET, ByteOrder.BIG_ENDIAN) !=
			crc32c(source, offset, HEADER_CRC_OFFSET))
		{
			throw new IllegalArgumentException("envelope header CRC32C mismatch");
		}
		final int kindCode = source.getByte(offset + 6) & 0xff;
		if (source.getByte(offset + 7) != 0)
		{
			throw new IllegalArgumentException("unknown envelope flags");
		}
		final Kind kind = Kind.fromCode(kindCode);
		final long epoch = source.getLong(offset + 8, ByteOrder.BIG_ENDIAN);
		final long sequence = source.getLong(offset + 16, ByteOrder.BIG_ENDIAN);
		final int payloadLength = source.getInt(offset + 24, ByteOrder.BIG_ENDIAN);
		final int chunkIndex = source.getInt(offset + 28, ByteOrder.BIG_ENDIAN);
		final int chunkCount = source.getInt(offset + 32, ByteOrder.BIG_ENDIAN);
		final int chunkOffset = source.getInt(offset + 36, ByteOrder.BIG_ENDIAN);
		if (epoch < 0 || sequence < 0 || sequence == Long.MAX_VALUE || payloadLength < 0 || chunkIndex < 0 ||
			chunkCount <= 0 || chunkIndex >= chunkCount || chunkOffset < 0 ||
			(kind != Kind.COMMIT && kind != Kind.ABORT &&
				(length - HEADER_LENGTH > payloadLength ||
					(chunkIndex == chunkCount - 1 && (long)chunkOffset + length - HEADER_LENGTH != payloadLength))))
		{
			throw new IllegalArgumentException("invalid envelope bounds");
		}
		if ((kind == Kind.COMMIT || kind == Kind.ABORT) && length != HEADER_LENGTH)
		{
			throw new IllegalArgumentException("marker carries a payload");
		}
		if ((kind == Kind.COMMIT || kind == Kind.ABORT) &&
			(chunkIndex != 0 || chunkOffset != 0 || (kind == Kind.ABORT && source.getInt(offset + 44,
				ByteOrder.BIG_ENDIAN) != 0)))
		{
			throw new IllegalArgumentException("non-canonical terminal marker");
		}
		final int payloadOnWire = length - HEADER_LENGTH;
		if (kind != Kind.COMMIT && kind != Kind.ABORT &&
			((long)chunkOffset + payloadOnWire > payloadLength))
		{
			throw new IllegalArgumentException("chunk exceeds logical payload length");
		}
		if (kind != Kind.COMMIT && kind != Kind.ABORT && payloadLength == 0 &&
			(chunkIndex != 0 || chunkCount != 1 || chunkOffset != 0 || payloadOnWire != 0))
		{
			throw new IllegalArgumentException("empty payload must use one canonical chunk");
		}
		if (kind != Kind.COMMIT && kind != Kind.ABORT && payloadLength > 0 && payloadOnWire == 0)
		{
			throw new IllegalArgumentException("non-empty payload chunks must carry bytes");
		}
		if (source.getInt(offset + 40, ByteOrder.BIG_ENDIAN) != crc32c(source, offset + HEADER_LENGTH, payloadOnWire))
		{
			throw new IllegalArgumentException("payload CRC32C mismatch");
		}
		view.set(source, offset + HEADER_LENGTH, payloadOnWire,
			source.getLong(offset + 48, ByteOrder.BIG_ENDIAN),
			source.getLong(offset + 56, ByteOrder.BIG_ENDIAN), epoch, sequence, kind,
			payloadLength, chunkIndex, chunkCount, chunkOffset, source.getInt(offset + 44, ByteOrder.BIG_ENDIAN));
		return view;
	}

	/** Computes the checksum used to detect damaged chunks and commits. */
	public static int crc32c(final byte[] payload)
	{
		return Crc32c.compute(payload);
	}

	/** Computes the same checksum directly from an Agrona buffer range. */
	public static int crc32c(final DirectBuffer payload, final int offset, final int length)
	{
		if (payload == null || offset < 0 || length < 0 || offset > payload.capacity() - length)
		{
			throw new IllegalArgumentException("invalid CRC32C range");
		}
		final CRC32C crc = DIRECT_CRC.get();
		crc.reset();
		/* Agrona's bulk copy keeps this path allocation-free after the first use
		 * on a polling thread and avoids allocating a ByteBuffer duplicate for
		 * every decoded fragment. */
		final byte[] scratch = CRC_SCRATCH.get();
		for (int copied = 0; copied < length; )
		{
			final int amount = Math.min(scratch.length, length - copied);
			payload.getBytes(offset + copied, scratch, 0, amount);
			crc.update(scratch, 0, amount);
			copied += amount;
		}
		return (int)crc.getValue();
	}

	/** Reusable view over one decoded envelope; it does not own the payload. */
	public static final class EnvelopeView
	{
		private DirectBuffer source;
		private int payloadOffset;
		private int payloadLengthOnWire;
		long clusterMostSignificantBits;
		long clusterLeastSignificantBits;
		long epoch;
		long sequence;
		Kind kind;
		int payloadLength;
		int chunkIndex;
		int chunkCount;
		int chunkOffset;
		int commitCrc32c;

		public EnvelopeView()
		{
		}

		void set(final DirectBuffer source, final int payloadOffset, final int payloadLengthOnWire,
			final long clusterMostSignificantBits, final long clusterLeastSignificantBits,
			final long epoch, final long sequence, final Kind kind, final int payloadLength,
			final int chunkIndex, final int chunkCount, final int chunkOffset, final int commitCrc32c)
		{
			this.source = source;
			this.payloadOffset = payloadOffset;
			this.payloadLengthOnWire = payloadLengthOnWire;
			this.clusterMostSignificantBits = clusterMostSignificantBits;
			this.clusterLeastSignificantBits = clusterLeastSignificantBits;
			this.epoch = epoch;
			this.sequence = sequence;
			this.kind = kind;
			this.payloadLength = payloadLength;
			this.chunkIndex = chunkIndex;
			this.chunkCount = chunkCount;
			this.chunkOffset = chunkOffset;
			this.commitCrc32c = commitCrc32c;
		}

		public UUID clusterId() { return new UUID(this.clusterMostSignificantBits, this.clusterLeastSignificantBits); }
		public boolean matches(final UUID clusterId)
		{
			return clusterId.getMostSignificantBits() == this.clusterMostSignificantBits &&
				clusterId.getLeastSignificantBits() == this.clusterLeastSignificantBits;
		}
		public long epoch() { return this.epoch; }
		public long sequence() { return this.sequence; }
		public Kind kind() { return this.kind; }
		public int payloadLength() { return this.payloadLength; }
		public int chunkIndex() { return this.chunkIndex; }
		public int chunkCount() { return this.chunkCount; }
		public int chunkOffset() { return this.chunkOffset; }
		public int commitCrc32c() { return this.commitCrc32c; }
		/** Returns the borrowed source buffer; valid until the next decode into this view. */
		public DirectBuffer source() { return this.source; }
		/** Returns the payload offset in {@link #source()}. */
		public int payloadOffset() { return this.payloadOffset; }
		/** Returns the number of payload bytes present in this envelope. */
		public int payloadLengthOnWire() { return this.payloadLengthOnWire; }
	}

	/** Owned decoded envelope returned by the allocation-friendly codec path. */
	public record Envelope(
		UUID clusterId,
		long epoch,
		long sequence,
		Kind kind,
		int payloadLength,
		int chunkIndex,
		int chunkCount,
		int chunkOffset,
		int commitCrc32c,
		byte[] payload
	)
	{
		public Envelope
		{
			if (payload == null) throw new NullPointerException("payload");
		}

		/** Returns a defensive copy for codec tests and compatibility callers. */
		@Override
		public byte[] payload()
		{
			return this.payload.clone();
		}
	}

}
