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

import org.agrona.DirectBuffer;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.*;

/**
 * Authenticated, monotonic reader watermark used by the Archive-retention
 * controller.
 *
 * <p>The signed bytes include the reader, cluster, Store generation, writer
 * epoch, recording identity, sequence, and recording position. A token copied
 * from a different reader, cluster, generation, recording, or writer epoch
 * therefore cannot authorize deletion.</p>
 *
 * @param readerId		  reader that produced the watermark
 * @param clusterId		  replication cluster identity
 * @param storeGeneration Store image identity
 * @param writerEpoch	  writer epoch associated with the recording
 * @param recordingId	  Aeron Archive recording identity
 * @param sequence		  transaction sequence acknowledged by the reader
 * @param position		  Archive position acknowledged by the reader
 * @param authentication  HMAC over the identity and progress fields
 */
public record AeronAuthenticatedWatermark(
		UUID readerId,
		UUID clusterId,
		UUID storeGeneration,
		long writerEpoch,
		long recordingId,
		long sequence,
		long position,
		byte[] authentication) {

	private static final int VERSION = 1;
	private static final int UUID_BYTES = 16;
	private static final int IDENTITY_LENGTH = Integer.BYTES + UUID_BYTES * 3 + Long.BYTES * 4;
	private static final int AUTHENTICATION_LENGTH = 32;
	private static final String ALGORITHM = "HmacSHA256";
	private static final UUID UUID_ZERO = new UUID(0L, 0L);
	private static final ThreadLocal<Mac> MAC = ThreadLocal.withInitial(() -> {
		try {
			return Mac.getInstance(ALGORITHM);
		} catch (final GeneralSecurityException failure) {
			throw new IllegalStateException("HMAC-SHA256 is unavailable", failure);
		}
	});
	private static final ThreadLocal<byte[]> AUTHENTICATION_SCRATCH =
			ThreadLocal.withInitial(() -> new byte[AUTHENTICATION_LENGTH]);
	private static final ThreadLocal<ByteBuffer> CANONICAL_SCRATCH = ThreadLocal.withInitial(() ->
			ByteBuffer.allocate(IDENTITY_LENGTH).order(ByteOrder.BIG_ENDIAN));

	/**
	 * Creates a watermark and copies the authentication bytes so the token is
	 * immutable after construction.
	 */
	public AeronAuthenticatedWatermark {
		if (readerId == null || clusterId == null || storeGeneration == null || authentication == null) {
			throw new NullPointerException("watermark identity and authentication are required");
		}
		if (writerEpoch < 0 || recordingId < 0 || sequence < -1 || sequence == Long.MAX_VALUE || position < -1 ||
			authentication.length != AUTHENTICATION_LENGTH) {
			throw new IllegalArgumentException("invalid Aeron watermark");
		}
		authentication = authentication.clone();
	}

	/**
	 * Creates a signed watermark using a caller-owned secret key.
	 *
	 * @param readerId		  reader that produced the watermark
	 * @param clusterId		  replication cluster identity
	 * @param storeGeneration Store image identity
	 * @param writerEpoch	  writer epoch associated with the recording
	 * @param recordingId	  Aeron Archive recording identity
	 * @param sequence		  transaction sequence acknowledged by the reader
	 * @param position		  Archive position acknowledged by the reader
	 * @param secret		  HMAC secret
	 * @return a signed watermark
	 */
	public static AeronAuthenticatedWatermark sign(
			final UUID readerId,
			final UUID clusterId,
			final UUID storeGeneration,
			final long writerEpoch,
			final long recordingId,
			final long sequence,
			final long position,
			final byte[] secret
	) {
		final byte[] authentication = authenticate(canonical(readerId, clusterId,
						storeGeneration, writerEpoch, recordingId, sequence, position),
				secret);
		return new AeronAuthenticatedWatermark(readerId, clusterId,
				storeGeneration, writerEpoch, recordingId, sequence, position,
				authentication);
	}

	/** Signs directly into the fixed wire representation used by the watermark publication. */
	public static byte[] signEncoded(
			final UUID readerId,
			final UUID clusterId,
			final UUID storeGeneration,
			final long writerEpoch,
			final long recordingId,
			final long sequence,
			final long position,
			final byte[] secret
	) {
		final byte[] encoded = new byte[IDENTITY_LENGTH + AUTHENTICATION_LENGTH];
		putCanonical(ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN), readerId, clusterId,
				storeGeneration, writerEpoch, recordingId, sequence, position);
		authenticateInto(encoded, IDENTITY_LENGTH, secret, encoded, IDENTITY_LENGTH);
		return encoded;
	}

	/**
	 * Decodes a token; authentication is checked separately with
	 * {@link #verify(byte[])}.
	 *
	 * @param encoded serialized watermark bytes
	 * @return decoded watermark
	 */
	public static AeronAuthenticatedWatermark decode(final byte[] encoded) {
		if (encoded == null) throw new NullPointerException("encoded");
		if (encoded.length != IDENTITY_LENGTH + AUTHENTICATION_LENGTH) {
			throw new IllegalArgumentException("invalid Aeron watermark encoding length");
		}
		final ByteBuffer buffer = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
		if (buffer.getInt() != VERSION) throw new IllegalArgumentException("unsupported Aeron watermark version");
		final UUID readerId = new UUID(buffer.getLong(), buffer.getLong());
		final UUID clusterId = new UUID(buffer.getLong(), buffer.getLong());
		final UUID storeGeneration = new UUID(buffer.getLong(), buffer.getLong());
		final long epoch = buffer.getLong();
		final long recordingId = buffer.getLong();
		final long sequence = buffer.getLong();
		final long position = buffer.getLong();
		final byte[] authentication = new byte[AUTHENTICATION_LENGTH];
		buffer.get(authentication);
		return new AeronAuthenticatedWatermark(
				readerId, clusterId, storeGeneration, epoch, recordingId, sequence, position, authentication);
	}

	/** Decodes directly from an Aeron/Agrona frame without copying the identity bytes. */
	public static AeronAuthenticatedWatermark decode(final DirectBuffer encoded, final int offset, final int length) {
		if (encoded == null) throw new NullPointerException("encoded");
		if (offset < 0 || length != IDENTITY_LENGTH + AUTHENTICATION_LENGTH ||
			offset > encoded.capacity() - length) {
			throw new IllegalArgumentException("invalid Aeron watermark encoding length");
		}
		int cursor = offset;
		if (encoded.getInt(cursor, ByteOrder.BIG_ENDIAN) != VERSION)
			throw new IllegalArgumentException("unsupported Aeron watermark version");
		cursor += Integer.BYTES;
		final UUID readerId = new UUID(encoded.getLong(cursor, ByteOrder.BIG_ENDIAN),
				encoded.getLong(cursor + Long.BYTES, ByteOrder.BIG_ENDIAN));
		cursor += 16;
		final UUID clusterId = new UUID(encoded.getLong(cursor, ByteOrder.BIG_ENDIAN),
				encoded.getLong(cursor + Long.BYTES, ByteOrder.BIG_ENDIAN));
		cursor += 16;
		final UUID storeGeneration = new UUID(encoded.getLong(cursor, ByteOrder.BIG_ENDIAN),
				encoded.getLong(cursor + Long.BYTES, ByteOrder.BIG_ENDIAN));
		cursor += 16;
		final long epoch = encoded.getLong(cursor, ByteOrder.BIG_ENDIAN);
		final long recordingId = encoded.getLong(cursor += Long.BYTES, ByteOrder.BIG_ENDIAN);
		final long sequence = encoded.getLong(cursor += Long.BYTES, ByteOrder.BIG_ENDIAN);
		final long position = encoded.getLong(cursor += Long.BYTES, ByteOrder.BIG_ENDIAN);
		cursor += Long.BYTES;
		final byte[] authentication = new byte[AUTHENTICATION_LENGTH];
		encoded.getBytes(cursor, authentication);
		return new AeronAuthenticatedWatermark(
				readerId, clusterId, storeGeneration, epoch, recordingId, sequence, position, authentication);
	}

	/**
	 * Creates an authenticated aggregate at the least advanced boundary. The
	 * aggregate uses the zero UUID as its reader id and is accepted only by a
	 * caller that has already verified every configured reader token.
	 *
	 * @param watermarks authenticated reader watermarks from one writer
	 * @param secret	 HMAC secret
	 * @return a signed watermark at the least advanced boundary
	 */
	public static AeronAuthenticatedWatermark aggregate(
			final Collection<AeronAuthenticatedWatermark> watermarks, final byte[] secret) {
		if (watermarks == null || watermarks.isEmpty()) throw new IllegalArgumentException("watermarks are empty");
		AeronAuthenticatedWatermark least = null;
		for (final AeronAuthenticatedWatermark watermark : watermarks) {
			if (watermark == null || !watermark.verify(secret))
				throw new SecurityException("invalid Aeron watermark authentication");
			if (least == null) {
				least = watermark;
			} else {
				if (!sameWriterIdentity(least, watermark))
					throw new IllegalArgumentException("watermarks do not belong to one Aeron writer");
				if (compareProgress(watermark, least) < 0) least = watermark;
			}
		}
		return sign(UUID_ZERO, least.clusterId(), least.storeGeneration(), least.writerEpoch(),
				least.recordingId(), least.sequence(), least.position(), secret);
	}

	private static byte[] canonical(
			final UUID readerId, final UUID clusterId, final UUID storeGeneration, final long writerEpoch,
			final long recordingId, final long sequence, final long position) {
		if (readerId == null || clusterId == null || storeGeneration == null)
			throw new NullPointerException("watermark identity");
		final byte[] canonical = new byte[IDENTITY_LENGTH];
		putCanonical(ByteBuffer.wrap(canonical).order(ByteOrder.BIG_ENDIAN), readerId, clusterId,
				storeGeneration, writerEpoch, recordingId, sequence, position);
		return canonical;
	}

	private static void putCanonical(final ByteBuffer buffer,
									 final UUID readerId, final UUID clusterId, final UUID storeGeneration, final long writerEpoch,
									 final long recordingId, final long sequence, final long position) {
		buffer.putInt(VERSION)
				.putLong(readerId.getMostSignificantBits()).putLong(readerId.getLeastSignificantBits())
				.putLong(clusterId.getMostSignificantBits()).putLong(clusterId.getLeastSignificantBits())
				.putLong(storeGeneration.getMostSignificantBits()).putLong(storeGeneration.getLeastSignificantBits())
				.putLong(writerEpoch).putLong(recordingId).putLong(sequence).putLong(position);
	}

	private static boolean sameWriterIdentity(
			final AeronAuthenticatedWatermark left, final AeronAuthenticatedWatermark right) {
		return left.clusterId().equals(right.clusterId()) &&
			   left.storeGeneration().equals(right.storeGeneration()) &&
			   left.writerEpoch() == right.writerEpoch() && left.recordingId() == right.recordingId();
	}

	private static int compareProgress(
			final AeronAuthenticatedWatermark left, final AeronAuthenticatedWatermark right) {
		final int sequence = Long.compare(left.sequence(), right.sequence());
		return sequence == 0 ? Long.compare(left.position(), right.position()) : sequence;
	}

	private static boolean monotonicProgress(
			final AeronAuthenticatedWatermark newer, final AeronAuthenticatedWatermark previous) {
		/* Recording positions are monotonic independently of the logical sequence.
		 * Comparing sequence first alone would accept (N+1, position before N),
		 * producing a cursor that claims to have advanced while pointing backwards
		 * into the recording. */
		if (newer.sequence() == previous.sequence()) {
			/* A resolved sequence has exactly one terminal Archive position. Accept
			 * byte-for-byte progress duplicates, but reject a cursor assembled from
			 * fields observed at different transaction boundaries. */
			return newer.position() == previous.position();
		}
		return newer.sequence() > previous.sequence() && newer.position() > previous.position();
	}

	private static byte[] authenticate(final byte[] value, final byte[] secret) {
		final byte[] authentication = new byte[AUTHENTICATION_LENGTH];
		authenticateInto(value, value.length, secret, authentication, 0);
		return authentication;
	}

	private static void authenticateInto(final byte[] value, final int length,
										 final byte[] secret, final byte[] target, final int targetOffset) {
		if (secret == null || secret.length == 0) throw new IllegalArgumentException("watermark secret is empty");
		try {
			final Mac mac = MAC.get();
			mac.init(new SecretKeySpec(secret, ALGORITHM));
			mac.update(value, 0, length);
			mac.doFinal(target, targetOffset);
		} catch (final GeneralSecurityException failure) {
			throw new IllegalStateException("HMAC-SHA256 is unavailable", failure);
		}
	}

	/**
	 * Returns a defensive copy of the authentication bytes.
	 *
	 * @return copied authentication bytes
	 */
	@Override
	public byte[] authentication() {
		return this.authentication.clone();
	}

	@Override
	public boolean equals(final Object candidate) {
		if (this == candidate) return true;
		if (!(candidate instanceof AeronAuthenticatedWatermark other)) return false;
		return this.writerEpoch == other.writerEpoch && this.recordingId == other.recordingId &&
			   this.sequence == other.sequence && this.position == other.position &&
			   this.readerId.equals(other.readerId) && this.clusterId.equals(other.clusterId) &&
			   this.storeGeneration.equals(other.storeGeneration) &&
			   Arrays.equals(this.authentication, other.authentication);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(this.readerId, this.clusterId, this.storeGeneration,
				this.writerEpoch, this.recordingId, this.sequence, this.position);
		return 31 * result + Arrays.hashCode(this.authentication);
	}

	/**
	 * Verifies the HMAC without accepting a token with a different identity.
	 *
	 * @param secret HMAC secret
	 * @return {@code true} when the token was signed with the secret
	 */
	public boolean verify(final byte[] secret) {
		final ByteBuffer canonical = CANONICAL_SCRATCH.get();
		canonical.clear();
		putCanonical(canonical, this.readerId, this.clusterId, this.storeGeneration, this.writerEpoch,
				this.recordingId, this.sequence, this.position);
		final byte[] expected = AUTHENTICATION_SCRATCH.get();
		authenticateInto(canonical.array(), IDENTITY_LENGTH, secret, expected, 0);
		return MessageDigest.isEqual(this.authentication, expected);
	}

	/**
	 * Encodes the signed token for a cursor or a control message.
	 *
	 * @return serialized watermark bytes
	 */
	public byte[] encode() {
		final byte[] encoded = new byte[IDENTITY_LENGTH + AUTHENTICATION_LENGTH];
		final ByteBuffer buffer = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
		putCanonical(buffer, this.readerId, this.clusterId, this.storeGeneration,
				this.writerEpoch, this.recordingId, this.sequence, this.position);
		buffer.put(this.authentication);
		return encoded;
	}

	/**
	 * Tracks the greatest accepted watermark and rejects replay, rollback, or a
	 * conflicting position for an already acknowledged sequence.
	 */
	public static final class Validator {
		private final byte[] secret;
		private final Map<UUID, AeronAuthenticatedWatermark> latest = new HashMap<>();

		/**
		 * Creates a validator for one HMAC secret.
		 *
		 * @param secret HMAC secret
		 */
		public Validator(final byte[] secret) {
			if (secret == null || secret.length == 0) throw new IllegalArgumentException("watermark secret is empty");
			this.secret = secret.clone();
		}

		/**
		 * Accepts a verified, monotonically advancing reader watermark.
		 *
		 * @param watermark watermark to accept
		 */
		public synchronized void accept(final AeronAuthenticatedWatermark watermark) {
			if (watermark == null || !watermark.verify(this.secret))
				throw new SecurityException("invalid Aeron watermark authentication");
			final AeronAuthenticatedWatermark previous = this.latest.get(watermark.readerId());
			if (previous != null && (!sameWriterIdentity(previous, watermark) ||
									 !monotonicProgress(watermark, previous))) {
				throw new IllegalStateException("Aeron reader watermark is not monotonic");
			}
			this.latest.put(watermark.readerId(), watermark);
		}

		/**
		 * Returns the latest accepted watermark for one reader, or {@code null}.
		 *
		 * @param readerId reader identity
		 * @return latest accepted watermark, or {@code null}
		 */
		public synchronized AeronAuthenticatedWatermark latest(final UUID readerId) {
			return this.latest.get(readerId);
		}

		/**
		 * Restores one reader entry after a failed durable state write.
		 *
		 * @param readerId	reader identity
		 * @param watermark prior watermark, or {@code null} to remove the entry
		 */
		public synchronized void restore(final UUID readerId, final AeronAuthenticatedWatermark watermark) {
			if (watermark == null) this.latest.remove(readerId);
			else this.latest.put(readerId, watermark);
		}

		/**
		 * Returns a stable snapshot for aggregation or diagnostics.
		 *
		 * @return immutable reader-to-watermark snapshot
		 */
		public synchronized Map<UUID, AeronAuthenticatedWatermark> snapshot() {
			return Map.copyOf(this.latest);
		}
	}

	/** Verifies and aggregates a configured set of reader acknowledgements. */
	public static final class Quorum {
		private final Set<UUID> expectedReaders;
		private final Set<UUID> activeReaders;
		private final Set<UUID> retiredReaders = new HashSet<>();
		private final Validator validator;

		/**
		 * Creates a quorum for the configured readers.
		 *
		 * @param expectedReaders reader identities that must acknowledge
		 * @param secret		  HMAC secret
		 */
		public Quorum(final Collection<UUID> expectedReaders, final byte[] secret) {
			if (expectedReaders == null || expectedReaders.isEmpty())
				throw new IllegalArgumentException("at least one Aeron reader is required");
			final HashSet<UUID> readers = new HashSet<>(expectedReaders);
			if (readers.size() != expectedReaders.size() || readers.contains(null) || readers.contains(UUID_ZERO))
				throw new IllegalArgumentException("invalid Aeron reader identity");
			this.expectedReaders = readers;
			this.activeReaders = new HashSet<>(readers);
			this.validator = new Validator(secret);
		}

		/**
		 * Accepts one authenticated acknowledgement from a configured reader.
		 *
		 * @param watermark acknowledgement to accept
		 */
		public synchronized void accept(final AeronAuthenticatedWatermark watermark) {
			if (watermark == null || watermark.sequence() < 0 || watermark.position() < 0 ||
				!this.activeReaders.contains(watermark.readerId()))
				throw new SecurityException("Aeron watermark reader is not part of the configured quorum");
			this.validator.accept(watermark);
		}

		/**
		 * Returns the least advanced acknowledgement once every reader has reported.
		 *
		 * @return least advanced authenticated acknowledgement
		 */
		public synchronized AeronAuthenticatedWatermark aggregate() {
			final Map<UUID, AeronAuthenticatedWatermark> snapshot = this.validator.snapshot();
			if (this.activeReaders.isEmpty())
				throw new IllegalStateException("Aeron reader quorum has no active readers");
			if (!snapshot.keySet().containsAll(this.activeReaders))
				throw new IllegalStateException("Aeron reader quorum has not acknowledged the requested boundary");
			return AeronAuthenticatedWatermark.aggregate(
					this.activeReaders.stream().map(snapshot::get).toList(), this.validator.secret);
		}

		/**
		 * Returns whether every configured reader has supplied a watermark.
		 *
		 * @return {@code true} when every configured reader has reported
		 */
		public synchronized boolean isComplete() {
			return !this.activeReaders.isEmpty() &&
				   this.validator.snapshot().keySet().containsAll(this.activeReaders);
		}

		/**
		 * Returns whether a reader identity is part of this writer's retention quorum.
		 *
		 * @param readerId reader identity
		 * @return {@code true} when the reader belongs to the quorum
		 */
		public synchronized boolean acceptsReader(final UUID readerId) {
			return this.activeReaders.contains(readerId);
		}

		/** Permanently retires a configured reader from subsequent quorum decisions. */
		public synchronized boolean retire(final UUID readerId) {
			if (!this.expectedReaders.contains(readerId))
				throw new IllegalArgumentException("reader is not configured for this Aeron quorum: " + readerId);
			if (!this.retiredReaders.add(readerId)) return false;
			this.activeReaders.remove(readerId);
			this.validator.restore(readerId, null);
			return true;
		}

		/** Restores a reader when durable retirement persistence fails. */
		public synchronized void reinstate(final UUID readerId) {
			if (this.retiredReaders.remove(readerId)) this.activeReaders.add(readerId);
		}

		/** Returns the durable retirement tombstones. */
		public synchronized Set<UUID> retiredReaders() {
			return Set.copyOf(this.retiredReaders);
		}

		/**
		 * Returns the authenticated per-reader state for durable persistence.
		 *
		 * @return immutable reader-to-watermark snapshot
		 */
		public synchronized Map<UUID, AeronAuthenticatedWatermark> snapshot() {
			return this.validator.snapshot();
		}

		/**
		 * Returns one reader's current acknowledgement for transactional updates.
		 *
		 * @param readerId reader identity
		 * @return current acknowledgement, or {@code null}
		 */
		public synchronized AeronAuthenticatedWatermark latest(final UUID readerId) {
			return this.validator.latest(readerId);
		}

		/**
		 * Restores one reader's prior acknowledgement after persistence failure.
		 *
		 * @param readerId	reader identity
		 * @param watermark prior watermark, or {@code null} to remove the entry
		 */
		public synchronized void restore(final UUID readerId, final AeronAuthenticatedWatermark watermark) {
			this.validator.restore(readerId, watermark);
		}

	}
}
