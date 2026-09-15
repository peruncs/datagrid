package peruncs.datagrid.cluster.nodelibrary.aeron;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationCursor;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationLogRetention;
import peruncs.datagrid.storage.distributed.aeron.checkpoint.AeronAuthenticatedWatermark;
import peruncs.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCursor;
import peruncs.datagrid.storage.distributed.types.AtomicFileStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.util.*;
import java.util.function.*;

/**
 * Writer-owned, authenticated Archive retention controller.
 *
 * <p>This component deliberately owns no transport lifecycle. The provider
 * supplies a small writer access view, so retention cannot accidentally close
 * or replace the publication while validating a reader watermark.</p>
 */
final class AeronArchiveRetention implements ReplicationLogRetention
{
	/* Versions 1 and 2 were development-only layouts. There is no migration
	 * contract, so the complete tombstone-aware layout starts at version 3. */
	private static final int STATE_VERSION = 3;

	private final byte[] secret;
	private final Set<UUID> configuredReaders;
	/* Replaced atomically when durable state is restored. Keeping the live quorum
	 * immutable during parsing prevents a malformed state file from installing a
	 * partial retirement/acknowledgement set. All access is serialized by this
	 * controller's synchronized public methods. */
	private AeronAuthenticatedWatermark.Quorum quorum;
	private final Runnable ensureWriter;
	private final RecordingPositions recordingPositions;
	private final LongSupplier recordingId;
	private final Supplier<AeronWriterBoundary> writerBoundary;
	private final LongUnaryOperator segmentPurger;
	private final UUID clusterId;
	private final UUID storeGeneration;
	private final long writerEpoch;
	private final IntSupplier termLength;
	private final IntSupplier segmentLength;
	private final BooleanSupplier watermarkDeliveryAvailable;
	private final Path statePath;
	private boolean closed;
	private boolean stateRestored;
	private AeronAuthenticatedWatermark persistedBoundary;

	AeronArchiveRetention(
		final byte[] secret,
		final Set<UUID> readers,
		final Runnable ensureWriter,
		final RecordingPositions recordingPositions,
		final LongSupplier recordingId,
		final Supplier<AeronWriterBoundary> writerBoundary,
		final LongUnaryOperator segmentPurger,
		final UUID clusterId,
		final UUID storeGeneration,
		final long writerEpoch,
		final IntSupplier termLength,
		final IntSupplier segmentLength,
		final BooleanSupplier watermarkDeliveryAvailable,
		final Path statePath
	)
	{
		Objects.requireNonNull(secret, "secret");
		Objects.requireNonNull(readers, "readers");
		Objects.requireNonNull(ensureWriter, "ensureWriter");
		Objects.requireNonNull(recordingPositions, "recordingPositions");
		Objects.requireNonNull(recordingId, "recordingId");
		Objects.requireNonNull(writerBoundary, "writerBoundary");
		Objects.requireNonNull(segmentPurger, "segmentPurger");
		Objects.requireNonNull(clusterId, "clusterId");
		Objects.requireNonNull(storeGeneration, "storeGeneration");
		Objects.requireNonNull(termLength, "termLength");
		Objects.requireNonNull(segmentLength, "segmentLength");
		Objects.requireNonNull(watermarkDeliveryAvailable, "watermarkDeliveryAvailable");
		this.secret = secret.clone();
		this.configuredReaders = Set.copyOf(readers);
		this.quorum = new AeronAuthenticatedWatermark.Quorum(readers, this.secret);
		this.ensureWriter = ensureWriter;
		this.recordingPositions = recordingPositions;
		this.recordingId = recordingId;
		this.writerBoundary = writerBoundary;
		this.segmentPurger = segmentPurger;
		this.clusterId = clusterId;
		this.storeGeneration = storeGeneration;
		this.writerEpoch = writerEpoch;
		this.termLength = termLength;
		this.segmentLength = segmentLength;
		this.watermarkDeliveryAvailable = watermarkDeliveryAvailable;
		this.statePath = statePath;
	}

	@Override
	public synchronized boolean isSupported()
	{
		if (this.closed || !this.watermarkDeliveryAvailable.getAsBoolean()) return false;
		this.ensureStateRestored();
		return this.quorum.isComplete();
	}

	@Override
	public synchronized MaintenanceResult deleteThrough(final ReplicationCursor cursor)
	{
		if (this.closed) throw new IllegalStateException("Aeron retention is closed");
		if (!this.watermarkDeliveryAvailable.getAsBoolean())
		{
			throw new UnsupportedOperationException("Aeron retention requires a deployed reader-to-writer watermark channel");
		}
		this.ensureStateRestored();
		if (cursor == null || !"aeron".equalsIgnoreCase(cursor.transport()))
			throw new IllegalArgumentException("Aeron retention requires an Aeron cursor");
		if (cursor.logicalSequence() < 0)
			throw new IllegalArgumentException("retention cursor must name a resolved sequence");
		try
		{
			if (!this.quorum.isComplete())
			{
				throw new IllegalStateException(
					"Aeron reader quorum has not acknowledged the requested boundary; missing=" +
						this.quorum.missingReaders());
			}
			final AeronAuthenticatedWatermark quorumWatermark = this.quorum.aggregate();
			final AeronWriterBoundary requested = this.requestedBoundary(cursor);
			if (quorumWatermark.sequence() < requested.sequence() ||
				quorumWatermark.sequence() == requested.sequence() &&
				quorumWatermark.position() < requested.position())
				throw new IllegalStateException("Aeron reader quorum has not reached the requested sequence");
			final long targetPosition = Math.min(quorumWatermark.position(), requested.position());
			this.ensureWriter.run();
			final AeronWriterBoundary terminal = this.writerBoundary.get();
			if (terminal == null || terminal.sequence() < 0 || terminal.position() < 0 ||
				requested.sequence() > terminal.sequence() || targetPosition > terminal.position())
			{
				throw new IllegalStateException("reader watermark is ahead of the durable writer boundary");
			}
			final long activeRecordingId = this.recordingId.getAsLong();
			if (requested.recordingId() != activeRecordingId || quorumWatermark.recordingId() != activeRecordingId)
				throw new IllegalArgumentException("retention watermark recording does not match the active writer");
			final long start = this.recordingPositions.startPosition().applyAsLong(activeRecordingId);
			final long boundary = AeronArchive.segmentFileBasePosition(start, targetPosition,
				this.termLength.getAsInt(), this.segmentLength.getAsInt());
			if (boundary <= start)
			{
				return new MaintenanceResult(MaintenanceResult.Status.NOTHING_TO_DELETE, start,
					"reader quorum has not crossed a complete Archive segment");
			}
			final long stop = this.recordingPositions.stopPosition().applyAsLong(activeRecordingId);
			final long recorded = stop < 0
				? this.recordingPositions.recordingPosition().applyAsLong(activeRecordingId) : stop;
			if (recorded < 0) throw new IllegalStateException("Aeron recording has no durable position");
			if (boundary > recorded)
				throw new IllegalArgumentException("retention watermark does not cover a complete Archive segment");
			try
			{
				this.segmentPurger.applyAsLong(boundary);
			}
			catch (final ArchiveException failure)
			{
				/* Aeron 1.53 sends active-recording as ACTIVE_RECORDING, but its
				 * isValidDetach replay guard sends GENERIC plus this producer-owned
				 * prefix. Keep both cases explicit and regression-pinned. */
				if (failure.errorCode() == ArchiveException.ACTIVE_RECORDING ||
					failure.errorCode() == ArchiveException.GENERIC && failure.getMessage() != null &&
					failure.getMessage().contains("invalid detach: replay in progress"))
				{
					return new MaintenanceResult(MaintenanceResult.Status.DEFERRED_ACTIVE_REPLAY, boundary,
						"Archive replay still uses a segment selected for retention");
				}
				throw failure;
			}
			return new MaintenanceResult(MaintenanceResult.Status.DELETED, boundary,
				"Archive segments purged through boundary");
		}
		catch (final SecurityException | IllegalArgumentException | IllegalStateException failure)
		{
			throw failure;
		}
		catch (final RuntimeException failure)
		{
			if (failure instanceof ArchiveException archiveFailure)
			{
				throw new IllegalStateException(
					"Aeron Archive retention failed closed (errorCode=" + archiveFailure.errorCode() + ")",
					archiveFailure);
			}
			throw new IllegalStateException("Aeron Archive retention failed closed", failure);
		}
	}

	@Override
	public synchronized void retireReader(final UUID readerId)
	{
		if (this.closed) throw new IllegalStateException("Aeron retention is closed");
		if (!this.watermarkDeliveryAvailable.getAsBoolean())
		{
			throw new UnsupportedOperationException(
				"Aeron reader retirement requires a deployed reader-to-writer watermark channel");
		}
		this.ensureStateRestored();
		if (readerId == null) throw new NullPointerException("readerId");
		final AeronAuthenticatedWatermark previous = this.quorum.latest(readerId);
		if (!this.quorum.retire(readerId)) return;
		try
		{
			this.persistState();
		}
		catch (final RuntimeException failure)
		{
			this.quorum.reinstate(readerId);
			this.quorum.restore(readerId, previous);
			throw failure;
		}
	}

	@Override
	public synchronized void recordReaderWatermark(final ReplicationCursor cursor)
	{
		if (this.closed) throw new IllegalStateException("Aeron retention is closed");
		if (!this.watermarkDeliveryAvailable.getAsBoolean())
		{
			throw new UnsupportedOperationException(
				"Aeron retention watermark delivery is not configured");
		}
		this.ensureStateRestored();
		if (cursor == null || !"aeron".equalsIgnoreCase(cursor.transport()))
			throw new IllegalArgumentException("Aeron retention requires an Aeron cursor");
		if (cursor.logicalSequence() < 0)
			throw new IllegalArgumentException("reader watermark must name a resolved sequence");
		final AeronAuthenticatedWatermark watermark;
		try
		{
			watermark = AeronAuthenticatedWatermark.decode(cursor.providerPosition());
		}
		catch (final RuntimeException failure)
		{
			throw new IllegalArgumentException("reader cursor has no authenticated Aeron watermark", failure);
		}
		if (watermark.sequence() != cursor.logicalSequence() ||
			!this.storeGeneration.equals(cursor.storeGeneration()))
		{
			throw new SecurityException("reader cursor and authenticated watermark do not name one boundary");
		}
		this.recordReaderWatermark(watermark);
	}

	/** Accepts a watermark already decoded by the Aeron control subscription. */
	synchronized void recordReaderWatermark(final AeronAuthenticatedWatermark watermark)
	{
		if (this.closed) throw new IllegalStateException("Aeron retention is closed");
		if (!this.watermarkDeliveryAvailable.getAsBoolean())
		{
			throw new UnsupportedOperationException(
				"Aeron retention watermark delivery is not configured");
		}
		this.ensureStateRestored();
		if (watermark == null || !this.quorum.acceptsReader(watermark.readerId()) || !watermark.verify(this.secret) ||
			watermark.sequence() < 0 || watermark.position() < 0 ||
			!this.matchesWriter(watermark) ||
			(this.recordingId.getAsLong() >= 0 && watermark.recordingId() != this.recordingId.getAsLong()))
		{
			throw new SecurityException("reader watermark identity or authentication is invalid");
		}
		/* Resolve a lazily created recording only after authenticating the token.
		 * This prevents an unauthenticated caller from forcing writer startup while
		 * still ensuring that a valid token is checked against the actual recording. */
		this.ensureWriter.run();
		if (watermark.recordingId() != this.recordingId.getAsLong())
		{
			throw new SecurityException("reader watermark recording does not match the active writer");
		}
		final AeronWriterBoundary terminal = this.writerBoundary.get();
		if (terminal == null || terminal.sequence() < watermark.sequence() ||
			terminal.sequence() == watermark.sequence() && terminal.position() < watermark.position())
		{
			throw new IllegalStateException("reader watermark is ahead of the durable writer boundary");
		}
		final AeronAuthenticatedWatermark previous = this.quorum.latest(watermark.readerId());
		this.quorum.accept(watermark);
		final AeronAuthenticatedWatermark completeBoundary = this.completeBoundary(this.quorum);
		/* A watermark that does not advance the complete quorum cannot authorize a
		 * new deletion. Leaving the older file in place is deliberately conservative
		 * after a crash: the writer may retain extra Archive segments, but it cannot
		 * delete a segment on the strength of an unpersisted boundary. */
		if (!this.advancesPersistedBoundary(completeBoundary)) return;
		try
		{
			this.persistState();
		}
		catch (final RuntimeException failure)
		{
			/* The in-memory quorum must never advance beyond the durable quorum file.
			 * Otherwise a failed write could authorize deletion that disappears on
			 * restart. Restore the prior acknowledgement before propagating failure. */
			this.quorum.restore(watermark.readerId(), previous);
			throw failure;
		}
	}

	@Override
	public synchronized void close()
	{
		if (this.closed) return;
		this.closed = true;
		/* Keep the key only for the lifetime of the controller. The quorum owns a
		 * defensive copy for validation, so both copies must be erased explicitly. */
		this.quorum.clearSecret();
		Arrays.fill(this.secret, (byte)0);
	}

	private AeronWriterBoundary requestedBoundary(final ReplicationCursor cursor)
	{
		if (!this.storeGeneration.equals(cursor.storeGeneration()))
		{
			throw new IllegalArgumentException("retention cursor belongs to another Store generation");
		}
		try
		{
			final AeronReplicationCursor requested = AeronReplicationCursor.decode(cursor.providerPosition());
			if (!requested.clusterId().equals(this.clusterId) ||
				!requested.storeGeneration().equals(this.storeGeneration) ||
				requested.epoch() != this.writerEpoch || requested.sequence() != cursor.logicalSequence() ||
				requested.recordingPosition() < 0)
			{
				throw new IllegalArgumentException("retention cursor identity does not match the active writer");
			}
			return new AeronWriterBoundary(
				requested.sequence(), requested.recordingId(), requested.recordingPosition());
		}
		catch (final RuntimeException failure)
		{
			throw new IllegalArgumentException("retention cursor must carry a valid Aeron replication cursor", failure);
		}
	}

	private void restoreState()
	{
		if (this.statePath == null) return;
		try
		{
			final byte[] encoded;
			/* Open with NOFOLLOW_LINKS and keep the handle for the complete read. A
			 * separate isSymbolicLink/size/readAllBytes sequence is TOCTOU-prone: an
			 * attacker could replace the state path with a symlink between checks. */
			try (SeekableByteChannel channel = Files.newByteChannel(
				this.statePath, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))
			{
				final long size = channel.size();
				if (size > 1_048_576L || size < 0L)
					throw new IOException("retention state is too large");
				encoded = new byte[(int)size];
				final ByteBuffer source = ByteBuffer.wrap(encoded);
				while (source.hasRemaining())
				{
					final int read = channel.read(source);
					if (read <= 0) throw new IOException("retention state read made no progress");
				}
			}
			final ByteBuffer buffer = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
			if (buffer.remaining() < Integer.BYTES * 2)
				throw new IOException("truncated retention state");
			final int version = buffer.getInt();
			if (version != STATE_VERSION)
				throw new IOException("unsupported retention state version");
			final int count = buffer.getInt();
			if (count < 0 || count > 1024)
				throw new IOException("invalid retention state count");
			final ArrayList<AeronAuthenticatedWatermark> watermarks = new ArrayList<>(count);
			final java.util.HashSet<UUID> watermarkReaders = new java.util.HashSet<>();
			for (int i = 0; i < count; i++)
			{
				if (buffer.remaining() < Integer.BYTES)
					throw new IOException("truncated retention state");
				final int length = buffer.getInt();
				if (length <= 0 || length > buffer.remaining())
					throw new IOException("invalid retention token length");
				final byte[] token = new byte[length];
				buffer.get(token);
				final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.decode(token);
				if (!watermark.verify(this.secret) || !this.matchesWriter(watermark))
				{
					throw new SecurityException("retention state belongs to another Aeron writer");
				}
				if (watermark.sequence() < 0 || watermark.position() < 0)
					throw new IOException("retention state contains an unresolved watermark");
				if (!watermarkReaders.add(watermark.readerId()))
				{
					throw new IOException("retention state contains duplicate reader watermark");
				}
				if (!this.quorum.acceptsReader(watermark.readerId()))
				{
					throw new IOException(
						"retention state contains a watermark for an unconfigured or retired reader: " +
							watermark.readerId());
				}
				watermarks.add(watermark);
			}
			if (buffer.remaining() < Integer.BYTES) throw new IOException("truncated retirement state");
			final int retiredCount = buffer.getInt();
			if (retiredCount < 0 || retiredCount > 1024 || buffer.remaining() != retiredCount * 16)
				throw new IOException("invalid retirement state count");
			final ArrayList<UUID> retiredReaders = new ArrayList<>(retiredCount);
			final java.util.HashSet<UUID> retiredReaderSet = new java.util.HashSet<>();
			for (int i = 0; i < retiredCount; i++)
			{
				final UUID readerId = new UUID(buffer.getLong(), buffer.getLong());
				if (!this.configuredReaders.contains(readerId))
					throw new IOException("retention state contains an unconfigured retired reader " + readerId);
				if (!retiredReaderSet.add(readerId))
					throw new IOException("retention state contains duplicate retired reader " + readerId);
				if (watermarkReaders.contains(readerId))
					throw new IOException("retention state contains both a watermark and retirement for " + readerId);
				retiredReaders.add(readerId);
			}
			if (buffer.hasRemaining()) throw new IOException("trailing retention state bytes");
			/* Build a replacement quorum only after the complete file has been parsed.
			 * Mutating the live quorum here used to make a later runtime validation
			 * failure observable as partially restored state. The replacement is
			 * published in one assignment, so retries always start from a clean view. */
			final AeronAuthenticatedWatermark.Quorum restored =
				new AeronAuthenticatedWatermark.Quorum(this.configuredReaders, this.secret);
			try
			{
				for (final UUID readerId : retiredReaders)
				{
					if (!restored.retire(readerId))
						throw new IOException("retention state contains duplicate retired reader " + readerId);
				}
				for (final AeronAuthenticatedWatermark watermark : watermarks) restored.accept(watermark);
			}
			catch (final RuntimeException | IOException failure)
			{
				restored.clearSecret();
				throw failure;
			}
			final AeronAuthenticatedWatermark.Quorum previous = this.quorum;
			this.quorum = restored;
			this.persistedBoundary = this.completeBoundary(restored);
			previous.clearSecret();
		}
		catch (final NoSuchFileException ignored)
		{
			/* The state file is optional on first startup. */
		}
		catch (final IOException | RuntimeException failure)
		{
			throw new IllegalStateException("cannot load authenticated Aeron retention state " + this.statePath, failure);
		}
	}

	/** Restores durable quorum state only once the watermark channel is usable. */
	private void ensureStateRestored()
	{
		if (!this.stateRestored && this.watermarkDeliveryAvailable.getAsBoolean())
		{
			this.restoreState();
			this.stateRestored = true;
		}
	}

	private boolean matchesWriter(final AeronAuthenticatedWatermark watermark)
	{
		return watermark.clusterId().equals(this.clusterId) &&
			watermark.storeGeneration().equals(this.storeGeneration) &&
			watermark.writerEpoch() == this.writerEpoch;
	}

	private void persistState()
	{
		if (this.statePath == null) return;
		try
		{
			final var snapshot = this.quorum
					.snapshot()
					.entrySet()
					.stream()
					.sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).toList();
			final var retired = this.quorum.retiredReaders().stream().sorted().toList();
			/* Encode each token once. Retention updates are infrequent, but encoding
			 * twice used to perform two HMAC passes and produced two short-lived arrays
			 * for every reader on every durable state replacement. */
			final byte[][] encodedTokens = new byte[snapshot.size()][];
			int length = Integer.BYTES * 3 + retired.size() * 16;
			for (int index = 0; index < snapshot.size(); index++)
			{
				encodedTokens[index] = snapshot.get(index).encode();
				length += Integer.BYTES + encodedTokens[index].length;
			}
			final ByteBuffer buffer = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN)
				.putInt(STATE_VERSION).putInt(snapshot.size());
			for (final byte[] token : encodedTokens)
			{
				buffer.putInt(token.length).put(token);
			}
			buffer.putInt(retired.size());
			for (final UUID readerId : retired)
			{
				buffer.putLong(readerId.getMostSignificantBits()).putLong(readerId.getLeastSignificantBits());
			}
			buffer.flip();
				AtomicFileStore.write(this.statePath, channel ->
			{
				final ByteBuffer source = buffer.duplicate();
				while (source.hasRemaining())
				{
					if (channel.write(source) == 0) throw new IOException("Archive retention state write made no progress");
				}
				});
				this.persistedBoundary = this.completeBoundary(this.quorum);
			}
		catch (final IOException failure)
		{
			throw new IllegalStateException("cannot persist authenticated Aeron retention state " + this.statePath, failure);
		}
		}

		private AeronAuthenticatedWatermark completeBoundary(
			final AeronAuthenticatedWatermark.Quorum value)
		{
			return value.isComplete() ? value.aggregate() : null;
		}

		private boolean advancesPersistedBoundary(final AeronAuthenticatedWatermark current)
		{
			if (current == null || this.persistedBoundary == null) return current != null;
			return current.writerEpoch() != this.persistedBoundary.writerEpoch() ||
				current.recordingId() != this.persistedBoundary.recordingId() ||
				current.sequence() > this.persistedBoundary.sequence() ||
				current.sequence() == this.persistedBoundary.sequence() &&
					current.position() > this.persistedBoundary.position();
		}

	/** Minimal Archive position view required by retention decisions. */
	record RecordingPositions(LongUnaryOperator startPosition, LongUnaryOperator stopPosition,
		LongUnaryOperator recordingPosition)
	{
		RecordingPositions
		{
			if (startPosition == null || stopPosition == null || recordingPosition == null)
				throw new NullPointerException("recording position suppliers");
		}
	}
}
