package org.eclipse.datagrid.cluster.nodelibrary.aeron;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Aeron Provider
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

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.exceptions.AeronException;
import org.eclipse.datagrid.cluster.nodelibrary.types.*;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronAuthenticatedWatermark;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpoint;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpointStore;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCursor;
import org.eclipse.datagrid.storage.distributed.aeron.reader.CursorSnapshot;
import org.eclipse.datagrid.storage.distributed.aeron.reader.ReaderDeliveryListener;
import org.eclipse.datagrid.storage.distributed.aeron.reader.StorageBinaryDataClientAeronArchive;
import org.eclipse.datagrid.storage.distributed.aeron.writer.AeronArchiveReplicationPublisher;
import org.eclipse.datagrid.storage.distributed.aeron.writer.AeronReplicationWriteCoordinator;
import org.eclipse.datagrid.storage.distributed.aeron.writer.AeronStorageBinaryTargetDistributing;
import org.eclipse.datagrid.storage.distributed.types.AtomicFileStore;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataClient;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

import static java.lang.System.Logger.Level.WARNING;

/**
 * Creates the Aeron implementation of the cluster replication transport.
 *
 * <p>The returned transport creates its driver and Archive lazily. The
 * transport owns those shared resources and releases them from
 * {@link ClusterReplicationTransport#close()}; individual readers and
 * distributors do not close the runtime.</p>
 */
public final class AeronClusterReplicationTransportProvider
	implements ClusterReplicationTransportProvider
{
	/** Creates a provider that reads Aeron settings when a transport is created. */
	public AeronClusterReplicationTransportProvider()
	{
	}

	/* Test-only, thread-confined seam used by the forked crash harness. */
	private static final System.Logger LOGGER =
		System.getLogger(AeronClusterReplicationTransportProvider.class.getName());
	private static final ThreadLocal<BiConsumer<String, Long>> CRASH_HOOK = new ThreadLocal<>();
	private static final ThreadLocal<Long> CHECKPOINT_SEQUENCE = new ThreadLocal<>();
	static void setCrashHook(final BiConsumer<String, Long> hook)
	{
		if (hook == null) CRASH_HOOK.remove();
		else CRASH_HOOK.set(hook);
	}

	static void clearCrashHook()
	{
		CRASH_HOOK.remove();
	}

	private static void crashPoint(final String name, final long sequence)
	{
		final BiConsumer<String, Long> hook = CRASH_HOOK.get();
		if (hook != null) hook.accept(name, sequence);
	}

	static long currentCheckpointSequence()
	{
		final Long sequence = CHECKPOINT_SEQUENCE.get();
		return sequence == null ? -1L : sequence;
	}

	/* Package-private forked-test seam. Keeping the private Transport type hidden
	 * avoids adding a production lifecycle interface solely for fault injection. */
	static void stopDriverForTest(final ClusterReplicationTransport transport)
	{
		if (!(transport instanceof Transport aeronTransport))
			throw new IllegalArgumentException("transport was not created by the Aeron provider");
		aeronTransport.stopDriver();
	}
	/** Returns the provider id used by configuration. */
	@Override
	public String id()
	{
		return "aeron";
	}

	/** Creates a transport with settings read from the node properties. */
	@Override
	public ClusterReplicationTransport create(final NodelibraryPropertiesProvider properties)
	{
		final Transport transport = new Transport(AeronSettings.fromEnvironment(properties));
		/* Probe metadata durability before any synchronized runtime startup path. */
		transport.verifyMetadataStorage();
		return transport;
	}

	/** Owns the Aeron resources for one node and one transport lifecycle. */
	private static final class Transport implements ClusterReplicationTransport
	{
		private final AeronSettings settings;
		private final AeronArchiveCapacity archiveCapacity;
		/** One transport-owned copy; avoids cloning the configured HMAC key for every cursor. */
		private final byte[] retentionSecret;
		private final AtomicLong nextSequence = new AtomicLong();
		private volatile AeronRuntime runtime;
		private volatile AeronArchiveReplicationPublisher writer;
		private volatile AeronReplicationWriteCoordinator coordinator;
		private volatile StorageBinaryDataClientAeronArchive reader;
		private volatile long readerRecordingId = Aeron.NULL_VALUE;
		/** Recording identity selected at writer startup; retained when RecordingPos is briefly unavailable. */
		private volatile long writerRecordingId;
		/* Published together after the terminal checkpoint is durable.  Readers of
		 * positionProvider() must never combine fields from two transactions. */
		private volatile AeronWriterBoundary writerBoundary = new AeronWriterBoundary(-1, -1, -1);
		private volatile boolean writerRecoveryInProgress;
		private volatile ReplicationHealth.State writerRecoveryState;
		/** First asynchronous MediaDriver failure; health must not hide it. */
		private volatile RuntimeException driverFailure;
		private ClusterStorageBinaryDataDistributor distributor;
		private String distributorStream;
		private ReplicationPositionProvider positionProvider;
		private ReplicationLogRetention retention;
		private AeronWatermarkChannel watermarkChannel;
		private AeronHealth health;
		private volatile boolean closed;
		private volatile boolean closing;
		private final AtomicLong rejectedWatermarks = new AtomicLong();
		private final ThreadLocal<Boolean> deliveryCallback = new ThreadLocal<>();

		private Transport(final AeronSettings settings)
		{
			this.settings = settings;
			this.archiveCapacity = new AeronArchiveCapacity(settings);
			this.retentionSecret = settings.retentionSecret();
			this.writerRecordingId = settings.recordingId();
		}

		private void verifyMetadataStorage()
		{
			final Path parent = this.settings.checkpointPath().toAbsolutePath().getParent();
			if (parent == null) throw new IllegalArgumentException("Aeron checkpoint path must have a parent directory");
			if (Files.isSymbolicLink(this.settings.checkpointPath()))
			{
				throw new IllegalArgumentException(
					"Aeron checkpoint path must not be a symbolic link: " + this.settings.checkpointPath());
			}
			AeronRuntime.ensurePrivateDirectory(parent);
			try
			{
				AtomicFileStore.verify(this.settings.checkpointPath());
			}
			catch (final IOException failure)
			{
				throw new IllegalStateException(
					"Aeron replication metadata storage does not support atomic replacement", failure);
			}
		}

		/** Claims the provider's single configured replication stream. */
		private void claimStream(final String streamName)
		{
			if (streamName == null || streamName.isBlank())
			{
				throw new IllegalArgumentException("Aeron replication stream name must not be blank");
			}
			if (this.distributorStream == null)
			{
				this.distributorStream = streamName;
			}
			else if (!this.distributorStream.equals(streamName))
			{
				throw new IllegalArgumentException(
					"Aeron transport is configured for stream " + this.distributorStream + ", not " + streamName);
			}
		}

		@Override
		public String id()
		{
			return "aeron";
		}

		@Override
		public synchronized ClusterStorageBinaryDataDistributor distributor(
			final String streamName,
			final boolean asynchronous
		)
		{
			/* Aeron data publication is intentionally available only through the
			 * persistence target below, where local acceptance and checkpoint fencing
			 * are one serialized operation. The distributor remains the dictionary and
			 * lifecycle control object required by the neutral Store integration. */
			this.ensureOpen();
			this.claimStream(streamName);
			if (asynchronous)
			{
				throw new IllegalArgumentException(
					"Aeron replication does not support asynchronous distribution; use the bounded Store write path");
			}
			if (this.distributor != null && java.util.Objects.equals(this.distributorStream, streamName))
			{
				return this.distributor;
			}
			if (this.distributor != null)
			{
				throw new IllegalStateException("Aeron transport supports one replication stream per provider");
			}
			this.distributor = new AeronDistributor(
				() -> "writer".equals(this.settings.role()),
				value ->
				{
					this.nextSequence.accumulateAndGet(value, Math::max);
					if (this.writer != null) this.writer.synchronizeNextSequence(value);
					});
			return this.distributor;
		}

		@Override
		public synchronized UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
			final String streamName, final org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor distributor)
		{
			this.ensureOpen();
			this.claimStream(streamName);
			if (!"writer".equals(this.settings.role()))
			{
				return UnaryOperator.identity();
			}
			final AeronReplicationWriteCoordinator coordinator = this.ensureCoordinator();
			return delegate -> new AeronStorageBinaryTargetDistributing(
				delegate,
				coordinator,
				distributor,
				sequence ->
				{
					if (distributor instanceof ClusterStorageBinaryDataDistributor cluster)
					{
						cluster.messageIndex(sequence);
					}
				},
				() -> !(distributor instanceof ClusterStorageBinaryDataDistributor cluster) || !cluster.ignoreDistribution()
			);
		}

		@Override
		public synchronized ClusterStorageBinaryDataClient client(
			final ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
			final String streamName,
			final AfterDataMessageConsumedListener cursorListener,
			final ReplicationCursor startingCursor,
			final boolean commitPosition
		)
		{
			this.ensureOpen();
			this.claimStream(streamName);
		/* A writer owns publication only. Even with an external Archive it must not
		 * create a second subscription against its own recording; that would violate
		 * the one-writer/N-reader topology and expose a partially initialized reader
		 * through the writer's health path. Configure a separate reader/backup-reader
		 * node when replay is required. */
		if ("writer".equals(this.settings.role()))
			{
				return ClusterStorageBinaryDataClient.NoOp(startingCursor, cursorListener);
			}
			if (packetAcceptor == null) throw new NullPointerException("packetAcceptor");
			if (this.reader != null)
			{
				final StorageBinaryDataClientAeronArchive previous = this.reader;
				previous.dispose();
				this.reader = null;
			}
			this.ensureRuntime();
			final long recordingId = this.settings.recordingId() >= 0
				? this.settings.recordingId() : this.discoverReaderRecordingId();
			this.readerRecordingId = recordingId;
			this.rejectUncertainReaderImport(recordingId);
			final ReplicationCursor cursor = startingCursor == null
				? new ReplicationCursor("aeron", null, -1, new byte[0]) : startingCursor;
			final boolean aeronCursor = "aeron".equalsIgnoreCase(cursor.transport());
			if (!aeronCursor && !"none".equalsIgnoreCase(cursor.transport()))
			{
				throw new IllegalArgumentException("cursor belongs to transport " + cursor.transport());
			}
			if (!aeronCursor && cursor.providerPosition().length != 0)
			{
				throw new IllegalArgumentException("an uninitialized cursor cannot carry Aeron provider state");
			}
			if (aeronCursor && cursor.storeGeneration() != null &&
				!this.settings.storeGeneration().equals(cursor.storeGeneration()))
			{
				throw new IllegalArgumentException("cursor store generation does not match Aeron configuration");
			}
			if (aeronCursor && cursor.logicalSequence() >= 0 && cursor.storeGeneration() == null)
			{
				throw new IllegalArgumentException("resolved Aeron cursor must identify its Store generation");
			}
			final AtomicReference<StorageBinaryDataClientAeronArchive> readerRef = new AtomicReference<>();
			final long cursorPosition = this.cursorPosition(cursor, recordingId);
			if (aeronCursor && cursor.logicalSequence() >= 0 && cursorPosition < 0)
			{
				throw new IllegalArgumentException("Aeron cursor has a sequence but no recording position");
			}
			final StorageBinaryDataClientAeronArchive replacement;
			try
			{
				replacement = StorageBinaryDataClientAeronArchive.New(
				this.aeron(),
				archiveContext(),
					recordingId,
				aeronCursor && cursorPosition >= 0
					? cursorPosition
					: io.aeron.archive.client.PersistentSubscription.FROM_START,
				this.settings.liveChannel(),
				this.settings.streamId(),
				this.settings.replayChannel(),
				this.settings.streamId() + 1,
				this.settings.replication(),
				this.settings.clusterId(),
				this.settings.epoch(),
				aeronCursor ? cursor.logicalSequence() : -1,
					new ReceiverAdapter(this, packetAcceptor),
				() ->
				{
					final StorageBinaryDataClientAeronArchive current = readerRef.get();
					if (current != null && current == this.reader)
					{
						final CursorSnapshot snapshot = current.cursorSnapshot();
						this.nextSequence.accumulateAndGet(snapshot.sequence() + 1, Math::max);
						/* Aeron has no broker offset to commit. Its cursor is the
						 * durability boundary for every reader, including ordinary readers;
						 * the neutral commitPosition flag only controls broker transports. */
						if (cursorListener != null)
						{
							/* Persist the local recovery cursor before advertising the same
							 * boundary to the writer's retention controller. */
							final byte[] position = new AeronReplicationCursor(
								this.settings.clusterId(), this.settings.nodeId(), this.settings.storeGeneration(),
								this.settings.epoch(), recordingId, snapshot.position(), snapshot.sequence()).encode();
							this.enterDeliveryCallback();
							try
							{
								cursorListener.onChange(MessageInfo.New(snapshot.sequence(), "aeron",
									this.settings.storeGeneration(), position));
							}
							finally
							{
								this.exitDeliveryCallback();
							}
							this.publishReaderWatermark(snapshot, recordingId);
						}
					}
				},
				this.readerDeliveryListener(),
					aeronCursor ? cursorPosition : -1
				);
			}
			catch (final RuntimeException | Error failure)
			{
				this.reader = null;
				throw failure;
			}
			this.reader = replacement;
			readerRef.set(replacement);
			/* Re-advertise the durable cursor when a reader restarts even if no new
			 * transaction arrives. Otherwise a restarted writer cannot rebuild its
			 * retention quorum until unrelated Store traffic happens. */
			if (aeronCursor && cursor.logicalSequence() >= 0)
			{
				this.publishReaderWatermark(
					new CursorSnapshot(cursor.logicalSequence(), cursorPosition), recordingId);
			}
			return new ClientAdapter(replacement, recordingId, this.settings.clusterId(),
				this.settings.nodeId(), this.settings.storeGeneration(), this.settings.epoch());
		}

		/** Decodes and validates the sole supported self-describing Aeron cursor. */
		private long cursorPosition(final ReplicationCursor cursor, final long expectedRecordingId)
		{
			final byte[] positionBytes = cursor.providerPosition();
			if (positionBytes.length == 0) return -1L;
			try
			{
				final AeronReplicationCursor aeron = AeronReplicationCursor.decode(positionBytes);
				if (!aeron.clusterId().equals(this.settings.clusterId()) ||
					!aeron.storeGeneration().equals(this.settings.storeGeneration()) ||
					aeron.epoch() != this.settings.epoch() || aeron.recordingId() != expectedRecordingId ||
					aeron.sequence() != cursor.logicalSequence())
				{
					throw new ReseedRequiredException(
						"Aeron cursor identity does not match the configured reader or writer");
				}
				return aeron.recordingPosition();
			}
			catch (final IllegalArgumentException failure)
			{
				throw new ReseedRequiredException("invalid Aeron cursor", failure);
			}
		}

		private long discoverReaderRecordingId()
		{
			final io.aeron.ChannelUri live = io.aeron.ChannelUri.parse(this.settings.liveChannel());
			final String alias = live.get(io.aeron.CommonContext.ALIAS_PARAM_NAME);
			if (alias == null || alias.isBlank())
			{
				throw new IllegalStateException(
					"Aeron recording discovery requires alias= on ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL");
			}
			final AtomicLong discovered = new AtomicLong(Aeron.NULL_VALUE);
			final int count = this.archive().listRecordingsForUri(0L, 2, "alias=" + alias,
				this.settings.streamId(), (controlSessionId, correlationId, recordingId, startTimestamp,
					stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength,
					termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel,
					sourceIdentity) -> discovered.set(recordingId));
			if (count != 1 || discovered.get() < 0)
			{
				throw new IllegalStateException(
					"Aeron recording discovery requires exactly one alias=" + alias +
						" recording for stream " + this.settings.streamId() + "; found " + count);
			}
			return discovered.get();
		}

		private Path readerUncertaintyPath()
		{
			return this.settings.checkpointPath().resolveSibling(
				this.settings.checkpointPath().getFileName() + ".reader-inflight");
		}

		private void rejectUncertainReaderImport(final long recordingId)
		{
			final Path path = this.readerUncertaintyPath();
			if (!Files.exists(path)) return;
			try
			{
				final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(path);
				if (checkpoint.recordType() != AeronReplicationCheckpoint.RecordType.READER_CURSOR ||
					checkpoint.state() != AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN ||
					!checkpoint.clusterId().equals(this.settings.clusterId()) ||
					!checkpoint.nodeId().equals(this.settings.nodeId()) ||
					!checkpoint.storeGeneration().equals(this.settings.storeGeneration()) ||
					checkpoint.recordingId() != recordingId ||
					checkpoint.writerEpoch() != this.settings.epoch())
				{
					throw reseedRequired(
						"reader import marker identity does not match Aeron configuration: " + path,
						null);
				}
				throw reseedRequired("reader Store import is uncertain at sequence " +
					checkpoint.transactionSequence() + "; manual reseed is required: " + path, null);
			}
			catch (final IOException failure)
			{
				throw reseedRequired("cannot read uncertain reader import marker; manual reseed is required: " + path,
					failure);
			}
		}

		private ReaderDeliveryListener readerDeliveryListener()
		{
			final Path path = this.readerUncertaintyPath();
			return new ReaderDeliveryListener()
			{
				@Override
				public void beforeStoreImport(final long sequence, final long position, final int dataLength,
					final int dataChunkCount, final int crc32c)
				{
					enterDeliveryCallback();
					try
					{
						final AeronReplicationCheckpoint checkpoint = new AeronReplicationCheckpoint(
							AeronReplicationCheckpoint.RecordType.READER_CURSOR,
							AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST,
							AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN,
							settings.clusterId(), settings.nodeId(), settings.storeGeneration(), readerRecordingId,
							settings.epoch(), sequence, position, dataLength, dataChunkCount, crc32c);
						AeronReplicationCheckpointStore.write(path, checkpoint);
					}
					catch (final IOException failure)
					{
						throw reseedRequired("cannot persist uncertain reader import marker: " + path, failure);
					}
					finally
					{
						exitDeliveryCallback();
					}
				}

				@Override
				public void afterStoreImport()
				{
					enterDeliveryCallback();
					try
					{
						AtomicFileStore.delete(path);
					}
					catch (final IOException failure)
					{
						throw reseedRequired("cannot clear uncertain reader import marker: " + path, failure);
					}
					finally
					{
						exitDeliveryCallback();
					}
				}
			};
		}

		@Override
		public synchronized ReplicationPositionProvider positionProvider(final String streamName)
		{
			this.ensureOpen();
			this.claimStream(streamName);
			if (this.positionProvider == null)
			{
				this.positionProvider = new org.eclipse.datagrid.cluster.nodelibrary.aeron.AeronPositionProvider(
					() -> "writer".equals(this.settings.role()),
					this::ensureWriter,
					() -> this.writerBoundary,
					this.settings::clusterId,
					this.settings::nodeId,
					this.settings::storeGeneration,
					this.settings::epoch);
			}
			return this.positionProvider;
		}

		@Override
		public synchronized ReplicationLogRetention retention()
		{
			this.ensureOpen();
			if (this.retention != null) return this.retention;
			final byte[] retentionSecret = this.retentionSecret;
			if (!this.retentionSupported())
			{
				/* Retention without a shared authentication key and an embedded Archive
				 * cannot prove that every reader has crossed the requested boundary. */
				this.retention = new ReplicationLogRetention()
				{
					@Override public boolean isSupported() { return false; }
					@Override public MaintenanceResult deleteThrough(final ReplicationCursor cursor)
					{
						throw new UnsupportedOperationException(
							"Aeron Archive retention requires an embedded writer and an authenticated watermark secret");
					}
					@Override public void close() { }
				};
				return this.retention;
			}
			this.retention = this.newRetentionController(retentionSecret);
			this.ensureRuntime();
			return this.retention;
		}

		private AeronArchiveRetention newRetentionController(final byte[] retentionSecret)
		{
			return new AeronArchiveRetention(retentionSecret, this.settings.retentionReaders(),
				() ->
				{
					if (!this.writerReady())
						throw new IllegalStateException("Aeron writer must be running before retention maintenance");
				}, new AeronArchiveRetention.RecordingPositions(
					recordingId -> this.archive().getStartPosition(recordingId),
					recordingId -> this.archive().getStopPosition(recordingId),
					recordingId -> this.archive().getRecordingPosition(recordingId)),
				() -> this.writerRecordingId,
				() -> this.writerBoundary, boundary ->
				{
					final AeronReplicationWriteCoordinator currentCoordinator = this.coordinator;
					final AeronArchiveReplicationPublisher currentWriter = this.writer;
					if (currentCoordinator == null || currentWriter == null)
					{
						throw new IllegalStateException(
							"Aeron retention requires the coordinator-backed Store writer");
					}
					return currentCoordinator.withWritesPaused(
						() -> currentWriter.purgeSegmentsWhileWritesPaused(boundary));
				}, this.settings.clusterId(), this.settings.storeGeneration(),
				this.settings.epoch(), this.settings.replication()::termLength,
				this.settings::archiveSegmentFileLength,
				() -> this.watermarkChannel != null && this.watermarkChannel.available(),
				this.settings.checkpointPath().resolveSibling(
					this.settings.checkpointPath().getFileName() + ".retention"));
		}

		private void publishReaderWatermark(final CursorSnapshot snapshot, final long recordingId)
		{
			final AeronWatermarkChannel channel = this.watermarkChannel;
			final byte[] secret = this.retentionSecret;
			if (channel == null || secret == null) return;
			channel.publish(AeronAuthenticatedWatermark.signEncoded(
				this.settings.nodeId(), this.settings.clusterId(), this.settings.storeGeneration(),
				this.settings.epoch(), recordingId, snapshot.sequence(), snapshot.position(), secret));
		}

		private void ensureWatermarkChannel()
		{
			if (this.watermarkChannel != null || this.retentionSecret == null) return;
			if ("writer".equals(this.settings.role()))
			{
				if (!this.retentionSupported()) return;
				if (this.retention == null)
				{
					this.retention = this.newRetentionController(this.retentionSecret);
				}
				final AeronArchiveRetention controller = (AeronArchiveRetention)this.retention;
				this.watermarkChannel = AeronWatermarkChannel.writer(this.aeron(),
					this.settings.watermarkChannel(), this.settings.watermarkStreamId(), (encoded, offset, length) ->
					{
						try
						{
							final AeronAuthenticatedWatermark watermark =
								AeronAuthenticatedWatermark.decode(encoded, offset, length);
							controller.recordReaderWatermark(watermark);
						}
						catch (final RuntimeException rejected)
						{
							/* Reject one malformed, unauthenticated, stale, or future
							 * watermark without killing delivery of later valid progress. */
							final long count = this.rejectedWatermarks.incrementAndGet();
							if ((count & (count - 1)) == 0)
							{
								LOGGER.log(WARNING,
									"Rejected Aeron reader watermark count=" + count, rejected);
							}
						}
					});
			}
			else
			{
				this.watermarkChannel = AeronWatermarkChannel.reader(this.aeron(),
					this.settings.watermarkChannel(), this.settings.watermarkStreamId());
			}
		}

		private boolean retentionSupported()
		{
			return this.retentionSecret != null && !this.settings.retentionReaders().isEmpty() &&
				"writer".equals(this.settings.role()) && !this.settings.externalArchive();
		}

		@Override
		public synchronized ReplicationHealth health(
			final StorageControllerAdapter storage,
			final ClusterStorageBinaryDataClient client
		)
		{
			this.ensureOpen();
			if (this.health == null || !this.health.matches(storage, client))
			{
				this.health = new org.eclipse.datagrid.cluster.nodelibrary.aeron.AeronHealth(
					storage,
					client,
					() -> this.closed,
					() -> this.driverFailure != null,
					this.archiveCapacity::available,
					this::writerReady,
					() -> "writer".equals(this.settings.role()),
					this::writerCheckpointState,
					this.archiveCapacity::usableSpaceBytes,
					() -> this.writerBoundary.position(),
					() -> this.writerBoundary.sequence(),
					() -> this.reader == null ? -1L : this.reader.lastAppliedSequence());
			}
			return this.health;
		}

		private boolean writerReady()
		{
			return "writer".equals(this.settings.role()) && !this.writerRecoveryInProgress &&
				this.writerRecoveryState == null && this.writer != null && !this.writer.isFailed();
		}

		/**
		 * Reports current writer readiness without starting the runtime. Lifecycle
		 * startup belongs to an explicit client or write operation, not a health
		 * probe.
		 */

		private synchronized ReplicationHealth.State writerCheckpointState()
		{
			if (this.driverFailure != null)
			{
				return ReplicationHealth.State.FAILED;
			}
			if (!"writer".equals(this.settings.role()))
			{
				return null;
			}
			/* Preserve a failed startup result even though the writer object was never
			 * installed. Returning STARTING for a null writer would hide a permanent
			 * RESEED_REQUIRED/FAILED state from health probes. */
			if (this.writerRecoveryState != null) return this.writerRecoveryState;
			if (this.writerRecoveryInProgress || this.writer == null)
			{
				return ReplicationHealth.State.STARTING;
			}
			if (this.writer != null)
			{
				/* A live writer can fail closed after an offer, await, or checkpoint
				 * error without being discarded immediately.  Do not advertise LIVE
				 * while that publisher can no longer accept a durable transaction. */
				return this.writer.isFailed() ? ReplicationHealth.State.FAILED : null;
			}
			/* A writer with no checkpoint state is healthy once its publisher exists. */
			return null;
		}

		private synchronized AeronArchiveReplicationPublisher ensureWriter()
		{
			if (this.writer != null) return this.writer;
			this.writerRecoveryInProgress = true;
			try
			{
				this.ensureRuntime();
				final AeronReplicationCheckpoint checkpoint = this.loadWriterCheckpoint();
				crashPoint("AFTER_RECOVERY_CHECKPOINT_READ",
					checkpoint == null ? -1L : checkpoint.transactionSequence());
				final long initialSequence = checkpoint == null ? this.nextSequence.get() :
					checkpoint.transactionSequence() + 1;
				this.nextSequence.set(initialSequence);
				final long recordingId = checkpoint != null ? checkpoint.recordingId() : this.settings.recordingId();
				if (recordingId >= 0)
				{
					this.validateRecordingBoundary(recordingId, checkpoint);
					try
					{
						this.writer = (this.settings.externalArchive()
							? AeronArchiveReplicationPublisher.ExtendRemote(this.archive(), recordingId,
								this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
								this.settings.epoch(), initialSequence)
							: AeronArchiveReplicationPublisher.Extend(this.archive(), recordingId,
							this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
							this.settings.epoch(), initialSequence));
					}
					catch (final RuntimeException failure)
					{
						throw reseedRequired("cannot extend configured recording " + recordingId +
							" after restart; the Archive recording is not safely reusable", failure);
					}
				}
				else
				{
					if (checkpoint != null && checkpoint.transactionSequence() >= 0)
					{
						throw reseedRequired("writer checkpoint has no recording identity", null);
					}
					this.writer = (this.settings.externalArchive()
						? AeronArchiveReplicationPublisher.NewRemote(this.archive(), this.settings.liveChannel(),
							this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
							this.settings.epoch(), initialSequence)
						: AeronArchiveReplicationPublisher.New(this.archive(), this.settings.liveChannel(),
						this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
						this.settings.epoch(), initialSequence));
				}
				final long discoveredRecordingId = this.writer.recordingId();
				this.writerRecordingId = discoveredRecordingId >= 0 ? discoveredRecordingId : recordingId;
				if (checkpoint == null && this.writerRecordingId >= 0)
				{
					long startPosition = -1L;
					try
					{
						startPosition = this.runtime == null ? -1L :
							this.archive().getStartPosition(this.writerRecordingId);
					}
					catch (final RuntimeException ignored)
					{
						/* A fresh recording may not expose its catalog position until the
						 * first image is connected. Keep the boundary sequence explicit and
						 * leave the position unknown rather than inventing a byte offset. */
					}
					this.writerBoundary = new AeronWriterBoundary(initialSequence - 1,
						this.writerRecordingId, startPosition);
				}
				this.writerRecoveryState = null;
				return this.writer;
			}
			catch (final RuntimeException failure)
			{
				this.writerRecoveryState = failure instanceof ReseedRequiredException
					? ReplicationHealth.State.RESEED_REQUIRED : ReplicationHealth.State.FAILED;
				throw failure;
			}
			catch (final Error failure)
			{
				this.writerRecoveryState = ReplicationHealth.State.FAILED;
				throw failure;
			}
			finally
			{
				this.writerRecoveryInProgress = false;
			}
		}

		private synchronized AeronReplicationWriteCoordinator ensureCoordinator()
		{
			if (this.coordinator == null)
			{
				final AeronArchiveReplicationPublisher.CheckpointWriter checkpointWriter =
					new AeronArchiveReplicationPublisher.CheckpointWriter()
					{
						@Override
						public void onState(final AeronReplicationCheckpoint.State state, final long sequence,
							final int dataLength, final int dataChunkCount, final int dataCrc32c, final long position)
						{
							persistWriterCheckpoint(state, sequence, dataLength, dataChunkCount, dataCrc32c, position);
						}

						@Override
						public void clearEnqueueFence()
						{
							try
							{
								AtomicFileStore.delete(inFlightCheckpointPath());
							}
							catch (final IOException failure)
							{
								throw new IllegalStateException("cannot clear local enqueue fence", failure);
							}
						}
					};
				this.coordinator = this.ensureWriter().newWriteCoordinator(
					this.settings.replication().durabilityMode(), checkpointWriter,
					this.archiveCapacity::available);
			}
			return this.coordinator;
		}

		private AeronReplicationCheckpoint loadWriterCheckpoint()
		{
			final AeronReplicationCheckpoint checkpoint = this.readWriterCheckpoint();
			if (checkpoint != null)
			{
				if (checkpoint.recordingId() >= 0) this.writerRecordingId = checkpoint.recordingId();
				this.writerBoundary = new AeronWriterBoundary(
					checkpoint.transactionSequence(), checkpoint.recordingId(), checkpoint.recordingPosition());
			}
			return checkpoint;
		}

		/** Reads and validates checkpoint state without changing live transport state. */
		private AeronReplicationCheckpoint readWriterCheckpoint()
		{
			final Path inFlightPath = this.inFlightCheckpointPath();
			if (Files.exists(inFlightPath))
			{
				if (!Files.exists(this.settings.checkpointPath()))
				{
					throw reseedRequired("in-flight writer transaction has no terminal checkpoint: " + inFlightPath, null);
				}
				try
				{
					final AeronReplicationCheckpoint inFlight = AeronReplicationCheckpointStore.read(inFlightPath);
					final AeronReplicationCheckpoint terminal = AeronReplicationCheckpointStore.read(this.settings.checkpointPath());
					this.validateWriterCheckpointIdentity(inFlight);
					this.validateWriterCheckpointIdentity(terminal);
						if ((terminal.state() == AeronReplicationCheckpoint.State.COMMITTED ||
							terminal.state() == AeronReplicationCheckpoint.State.REJECTED) &&
							terminal.transactionSequence() >= inFlight.transactionSequence())
						{
							if (terminal.transactionSequence() == inFlight.transactionSequence() &&
								(terminal.dataLength() != inFlight.dataLength() ||
									terminal.dataChunkCount() != inFlight.dataChunkCount() ||
									terminal.resolutionCrc32c() != inFlight.resolutionCrc32c() ||
									terminal.recordingId() != inFlight.recordingId()))
							{
								throw reseedRequired("terminal checkpoint does not match the in-flight fence", null);
							}
							AtomicFileStore.delete(inFlightPath);
					}
					else
					{
						throw reseedRequired("in-flight writer transaction is not covered by a terminal checkpoint " +
							inFlightPath, null);
					}
				}
				catch (final IOException failure)
				{
					throw reseedRequired("cannot validate in-flight writer transaction " + inFlightPath, failure);
				}
			}
			if (!Files.exists(this.settings.checkpointPath())) return null;
			try
			{
				final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(this.settings.checkpointPath());
				this.validateWriterCheckpointIdentity(checkpoint);
				if (checkpoint.state() != AeronReplicationCheckpoint.State.COMMITTED &&
					checkpoint.state() != AeronReplicationCheckpoint.State.REJECTED)
				{
					throw new ReseedRequiredException(
						"writer checkpoint is not restartable (state=" + checkpoint.state() +
						", sequence=" + checkpoint.transactionSequence() + ", path=" +
							this.settings.checkpointPath() + ")");
				}
				return checkpoint;
			}
			catch (final IOException failure)
			{
				throw reseedRequired("cannot read writer checkpoint", failure);
			}
		}

		private void validateWriterCheckpointIdentity(final AeronReplicationCheckpoint checkpoint)
		{
			final AeronReplicationCheckpoint.DurabilityMode expectedMode = switch (
				this.settings.replication().durabilityMode())
			{
				case ARCHIVE_FIRST -> AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST;
				case ENQUEUE_THEN_ARCHIVE -> AeronReplicationCheckpoint.DurabilityMode.ENQUEUE_THEN_ARCHIVE;
				case LOCAL_DURABLE_FIRST -> throw new IllegalStateException(
					"LOCAL_DURABLE_FIRST is not supported by the Aeron provider");
			};
			if (checkpoint.recordType() != AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT ||
				checkpoint.durabilityMode() != expectedMode ||
				!checkpoint.clusterId().equals(this.settings.clusterId()) ||
				!checkpoint.nodeId().equals(this.settings.nodeId()) ||
				!checkpoint.storeGeneration().equals(this.settings.storeGeneration()) ||
				checkpoint.writerEpoch() != this.settings.epoch())
			{
				throw reseedRequired("writer checkpoint identity does not match Aeron configuration", null);
			}
			if (this.settings.recordingId() >= 0 && checkpoint.recordingId() >= 0 &&
				this.settings.recordingId() != checkpoint.recordingId())
			{
				throw reseedRequired("configured recording does not match writer checkpoint", null);
			}
		}

		/**
		 * Verifies the Archive stop-position boundary against the durable writer
		 * checkpoint. Extending beyond the last persisted terminal checkpoint can
		 * reuse a sequence already present in the Archive, so this scalar fence
		 * fails closed until explicit tail replay/truncation is implemented. It
		 * does not scan envelope frames or prove that an orphan tail is replayable.
		 */
		private void validateRecordingBoundary(final long recordingId,
			final AeronReplicationCheckpoint checkpoint)
		{
			if (checkpoint == null)
			{
				this.validateEmptyRecordingBoundary(recordingId);
				return;
			}
			if (checkpoint.recordingPosition() < 0)
			{
				this.validateEmptyRecordingBoundary(recordingId);
				return;
			}
			final long stopPosition;
			try
			{
				stopPosition = this.archive().getStopPosition(recordingId);
			}
			catch (final RuntimeException failure)
			{
				throw reseedRequired("cannot inspect recording tail " + recordingId, failure);
			}
            new AeronWriterBoundary(checkpoint.transactionSequence(), recordingId,
                checkpoint.recordingPosition()).validateArchiveStop(stopPosition);
        }

		private void validateEmptyRecordingBoundary(final long recordingId)
		{
			/* A recording without a terminal checkpoint is safe only when it is
			 * genuinely empty; otherwise the next writer could reuse an orphaned
			 * sequence. */
			try
			{
				final long start = this.archive().getStartPosition(recordingId);
				final long stop = this.archive().getStopPosition(recordingId);
				if (stop < 0 || start < 0 || stop > start)
				{
					throw reseedRequired("recording has data but no terminal writer checkpoint", null);
				}
			}
			catch (final ReseedRequiredException failure)
			{
				throw failure;
			}
			catch (final RuntimeException failure)
			{
				throw reseedRequired("cannot inspect empty recording boundary " + recordingId, failure);
			}
		}

		private static ReseedRequiredException reseedRequired(final String message,
			final Throwable cause)
		{
			return cause == null ? new ReseedRequiredException(message) :
				new ReseedRequiredException(message, cause);
		}

		private void persistWriterCheckpoint(final AeronReplicationCheckpoint.State state,
			final long sequence, final int dataLength, final int dataChunkCount,
			final int dataCrc32c, final long position)
		{
			if (state == AeronReplicationCheckpoint.State.PREPARING ||
				state == AeronReplicationCheckpoint.State.ENQUEUED ||
				state == AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN)
			{
				this.writeCheckpoint(this.inFlightCheckpointPath(), state, sequence, dataLength,
					dataChunkCount, dataCrc32c, position);
				return;
			}
			if (state != AeronReplicationCheckpoint.State.COMMITTED &&
				state != AeronReplicationCheckpoint.State.REJECTED)
			{
				return;
			}
			this.writeCheckpoint(this.settings.checkpointPath(), state, sequence, dataLength,
				dataChunkCount, dataCrc32c, position);
			crashPoint("AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE", sequence);
            /* The terminal checkpoint is the durable boundary. Publish it to
             * readers before best-effort cleanup of the diagnostic fence so a
             * cleanup failure cannot make a durable commit look unavailable. */
            this.writerBoundary = new AeronWriterBoundary(sequence, this.writerRecordingId, position);
            try
			{
				AtomicFileStore.delete(this.inFlightCheckpointPath());
			}
			catch (final IOException failure)
			{
				throw new IllegalStateException("cannot clear in-flight writer checkpoint", failure);
			}
		}

		private Path inFlightCheckpointPath()
		{
			final Path path = this.settings.checkpointPath();
			return path.resolveSibling(path.getFileName() + ".inflight");
		}

		private void writeCheckpoint(final Path path, final AeronReplicationCheckpoint.State state,
			final long sequence, final int dataLength, final int dataChunkCount,
			final int dataCrc32c, final long position)
		{
			this.writeCheckpoint(path, this.newWriterCheckpoint(
				state, sequence, dataLength, dataChunkCount, dataCrc32c, position));
		}

		private void writeCheckpoint(final Path path, final AeronReplicationCheckpoint checkpoint)
		{
			CHECKPOINT_SEQUENCE.set(checkpoint.transactionSequence());
			try
			{
				AeronReplicationCheckpointStore.write(path, checkpoint);
			}
			catch (final IOException failure)
			{
				throw new IllegalStateException("cannot persist writer checkpoint", failure);
			}
			finally
			{
				CHECKPOINT_SEQUENCE.remove();
			}
		}

		private AeronReplicationCheckpoint newWriterCheckpoint(
			final AeronReplicationCheckpoint.State state, final long sequence,
			final int dataLength, final int dataChunkCount, final int dataCrc32c,
			final long position)
		{
			final long discoveredRecordingId = this.writer == null ? Aeron.NULL_VALUE : this.writer.recordingId();
			if (discoveredRecordingId >= 0) this.writerRecordingId = discoveredRecordingId;
			final long recordingId = this.writerRecordingId >= 0 ? this.writerRecordingId : this.settings.recordingId();
			final AeronReplicationCheckpoint.DurabilityMode checkpointMode = switch (
				this.settings.replication().durabilityMode())
			{
				case ARCHIVE_FIRST -> AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST;
				case ENQUEUE_THEN_ARCHIVE -> AeronReplicationCheckpoint.DurabilityMode.ENQUEUE_THEN_ARCHIVE;
				case LOCAL_DURABLE_FIRST -> throw new IllegalStateException(
					"LOCAL_DURABLE_FIRST is not supported by the Aeron provider");
			};
			return new AeronReplicationCheckpoint(
				AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
				checkpointMode,
				state, this.settings.clusterId(), this.settings.nodeId(), this.settings.storeGeneration(),
				recordingId, this.settings.epoch(), sequence, position, dataLength, dataChunkCount, dataCrc32c);
		}

		private synchronized void ensureRuntime()
		{
			this.ensureOpen();
			if (this.driverFailure != null)
			{
				throw new IllegalStateException("Aeron runtime has failed; create a new transport", this.driverFailure);
			}
			if (this.runtime != null) return;
			if ("writer".equals(this.settings.role())) this.archiveCapacity.invalidate();
			this.runtime = AeronRuntime.start(this.settings, this::recordDriverFailure,
				() -> crashPoint("BEFORE_PUBLICATION_CONNECTED", -1L));
			try
			{
				this.ensureWatermarkChannel();
			}
			catch (final RuntimeException | Error failure)
			{
				final RuntimeException cleanupFailure = this.closeRuntimeQuietly();
				if (cleanupFailure != null) failure.addSuppressed(cleanupFailure);
				throw failure;
			}
		}

		private void recordDriverFailure(final Throwable failure)
		{
			if (failure instanceof AeronException aeronFailure &&
				aeronFailure.category() == AeronException.Category.WARN)
			{
				/* Aeron uses the configured handler for non-terminal operational events
				 * too (for example, a control response disconnecting during orderly
				 * reader shutdown). Such events are diagnostic, not a failed runtime. */
				LOGGER.log(WARNING, "Aeron transport warning", aeronFailure);
				return;
			}
			final RuntimeException normalized = failure instanceof RuntimeException runtimeException
				? runtimeException
				: new IllegalStateException("Aeron MediaDriver failed", failure);
			if (this.driverFailure == null)
			{
				this.driverFailure = normalized;
			}
			else if (this.driverFailure != normalized)
			{
				/* Health keeps the first terminal cause, but later callbacks still carry
				 * useful diagnostics (and must not disappear silently). */
				LOGGER.log(WARNING, "Additional Aeron transport failure", normalized);
			}
			final StorageBinaryDataClientAeronArchive current = this.reader;
			if (current != null)
			{
				current.fail(normalized);
			}
		}

		private synchronized void stopDriver()
		{
			this.ensureOpen();
			if (this.runtime == null) throw new IllegalStateException("Aeron driver is not running");
			this.runtime.stopDriver();
		}

		private RuntimeException closeRuntimeQuietly()
		{
			RuntimeException failure = null;
			if (this.watermarkChannel != null)
			{
				try
				{
					this.watermarkChannel.close();
					this.watermarkChannel = null;
				}
				catch (final RuntimeException closeFailure)
				{
					failure = closeFailure;
				}
			}
			if (this.retention != null)
			{
				try
				{
					this.retention.close();
					this.retention = null;
				}
				catch (final RuntimeException retentionFailure)
				{
					failure = appendFailure(failure, retentionFailure);
				}
			}
			if (this.watermarkChannel == null && this.retention == null && this.runtime != null)
			{
				try
				{
					this.runtime.close();
					this.runtime = null;
				}
				catch (final RuntimeException closeFailure)
				{
					failure = appendFailure(failure, closeFailure);
				}
			}
			return failure;
		}

		private static RuntimeException appendFailure(final RuntimeException current,
			final RuntimeException additional)
		{
			if (current == null) return additional;
			if (current != additional) current.addSuppressed(additional);
			return current;
		}

		private Aeron aeron()
		{
			if (this.runtime == null) throw new IllegalStateException("Aeron runtime is not initialized");
			return this.runtime.aeron();
		}

		private AeronArchive archive()
		{
			if (this.runtime == null) throw new IllegalStateException("Aeron runtime is not initialized");
			return this.runtime.archive();
		}

		private AeronArchive.Context archiveContext()
		{
			if (this.runtime == null) throw new IllegalStateException("Aeron runtime is not initialized");
			return this.runtime.archiveContext();
		}

		private void ensureOpen()
		{
			this.ensureNotDeliveryCallback();
			if (this.closed || this.closing)
			{
				throw new IllegalStateException(this.closed ? "Aeron transport is closed" : "Aeron transport is closing");
			}
		}

		private void ensureNotDeliveryCallback()
		{
			if (Boolean.TRUE.equals(this.deliveryCallback.get()))
			{
				throw new IllegalStateException(
					"Aeron transport cannot be re-entered from a reader delivery callback");
			}
		}

		private void enterDeliveryCallback()
		{
			if (Boolean.TRUE.equals(this.deliveryCallback.get()))
			{
				throw new IllegalStateException("nested Aeron reader delivery callback");
			}
			this.deliveryCallback.set(Boolean.TRUE);
		}

		private void exitDeliveryCallback()
		{
			this.deliveryCallback.remove();
		}

		@Override
		public void close()
		{
			this.ensureNotDeliveryCallback();
			synchronized (this)
			{
				if (this.closed || this.closing) return;
				if (this.reader == null && this.writer == null && this.coordinator == null &&
					this.runtime == null && this.retention == null && this.watermarkChannel == null)
				{
					this.closed = true;
					return;
				}
				this.closing = true;
			}

			RuntimeException failure = null;
			if (this.reader != null)
			{
				try
				{
					this.reader.dispose();
					this.reader = null;
				}
				catch (final RuntimeException readerFailure)
				{
					/* Keep the shared Aeron resources open while the polling thread is
					 * still alive; a retry of close() can finish the shutdown safely. */
					failure = readerFailure;
				}
			}
			/* The coordinator owns every pending transaction and must resolve or preserve
			 * its fence before the Archive wrapper stops the recording. Closing the raw
			 * writer first can publish an ABORT beneath a Store write that still owns the
			 * coordinator. */
			if (this.reader == null && this.coordinator != null)
			{
				try
				{
					this.coordinator.dispose();
					this.coordinator = null;
				}
				catch (final RuntimeException coordinatorFailure)
				{
					if (failure == null) failure = coordinatorFailure;
					else failure.addSuppressed(coordinatorFailure);
				}
			}
			if (this.reader == null && this.coordinator == null && this.writer != null)
			{
				try
				{
					this.writer.close();
					this.writer = null;
				}
				catch (final RuntimeException writerFailure)
				{
					if (this.writer.isClosed()) this.writer = null;
					if (failure == null) failure = writerFailure;
					else failure.addSuppressed(writerFailure);
				}
			}
			/* A reader can publish its final durable cursor from the polling thread.
			 * Stop that thread before flushing and closing the watermark publication.
			 * Likewise, keep the writer-side receiver alive until publication shutdown
			 * has completed. Closing the auxiliary channel first creates a spurious
			 * reader failure during otherwise healthy transport shutdown. */
			if (this.reader == null && this.writer == null && this.coordinator == null &&
				this.watermarkChannel != null)
			{
				try
				{
					this.watermarkChannel.close();
					this.watermarkChannel = null;
				}
				catch (final RuntimeException channelFailure)
				{
					failure = appendFailure(failure, channelFailure);
				}
			}
			if (this.reader == null && this.writer == null && this.coordinator == null &&
				this.watermarkChannel == null && this.retention != null)
			{
				try
				{
					this.retention.close();
					this.retention = null;
				}
				catch (final RuntimeException retentionFailure)
				{
					failure = appendFailure(failure, retentionFailure);
				}
			}
			/* Do not close the shared runtime while a writer or coordinator still owns
			 * the publication. A failed abort/stop is retryable; closing Aeron here would
			 * turn that retry into a use-after-close and leak the unresolved sequence. */
			if (this.reader == null && this.writer == null && this.coordinator == null && this.retention == null &&
				this.watermarkChannel == null && this.runtime != null)
			{
				try
				{
					this.runtime.close();
					this.runtime = null;
				}
				catch (final RuntimeException runtimeFailure)
				{
					failure = appendFailure(failure, runtimeFailure);
				}
			}
			if (failure != null)
			{
				this.closing = false;
				throw new IllegalStateException("failed to close Aeron transport", failure);
			}
			synchronized (this)
			{
				this.distributor = null;
				this.distributorStream = null;
				this.closed = true;
				this.closing = false;
			}
		}
	}

	/** Adapts complete Aeron data to the neutral packet acceptor. */
	private record ReceiverAdapter(Transport owner, ClusterStorageBinaryDataPacketAcceptor packetAcceptor)
		implements org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataReceiver
	{
		public void receiveTypeDictionary(final String value)
		{
			this.owner().enterDeliveryCallback();
			try
			{
				this.packetAcceptor().acceptTypeDictionary(value);
			}
			finally
			{
				this.owner().exitDeliveryCallback();
			}
		}
		public void receiveData(final org.eclipse.serializer.persistence.binary.types.Binary value)
		{
			this.owner().enterDeliveryCallback();
			try
			{
				this.packetAcceptor().acceptData(value);
				this.packetAcceptor().awaitApplied();
			}
			finally
			{
				this.owner().exitDeliveryCallback();
			}
		}
		@Override
		public boolean receiveDataOwned(final org.eclipse.serializer.persistence.binary.types.Binary value)
		{
			this.owner().enterDeliveryCallback();
			try
			{
				return this.packetAcceptor().acceptDataOwned(value);
			}
			finally
			{
				this.owner().exitDeliveryCallback();
			}
		}
		@Override
		public boolean canReceiveDataOwned()
		{
			return this.packetAcceptor().canAcceptDataOwned();
		}
		@Override
		public void awaitApplied()
		{
			this.owner().enterDeliveryCallback();
			try
			{
				this.packetAcceptor().awaitApplied();
			}
			finally
			{
				this.owner().exitDeliveryCallback();
			}
		}
	}

	/** Adds the neutral message-position view to the Aeron client. */
	private record ClientAdapter(
		StorageBinaryDataClientAeronArchive delegate,
		long recordingId,
		UUID clusterId,
		UUID nodeId,
		UUID storeGeneration,
		long epoch) implements ClusterStorageBinaryDataClient
	{
		public void start() { this.delegate().start(); }
		public void stopAtLatestMessage() { this.delegate().stopAtLatestMessage(); }
		public MessageInfo messageInfo()
		{
			final CursorSnapshot snapshot = this.delegate().cursorSnapshot();
			return MessageInfo.New(snapshot.sequence(), "aeron", this.storeGeneration(),
				new AeronReplicationCursor(this.clusterId(), this.nodeId(), this.storeGeneration(),
					this.epoch(), this.recordingId(), snapshot.position(), snapshot.sequence()).encode());
		}
		public boolean isRunning() { return this.delegate().isRunning(); }
		public boolean isLive() { return this.delegate().isLive(); }
		public RuntimeException failure() { return this.delegate().failure(); }
		public StorageBinaryDataClient.StopOutcome stopOutcome() { return this.delegate().stopOutcome(); }
		public StorageBinaryDataClient.StopResult stopResult() { return this.delegate().stopResult(); }
		public void resume() { this.delegate().resume(); }
		public void dispose() { this.delegate().dispose(); }
	}
}
