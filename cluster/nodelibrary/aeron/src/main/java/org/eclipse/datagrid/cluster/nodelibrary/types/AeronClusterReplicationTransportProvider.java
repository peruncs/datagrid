package org.eclipse.datagrid.cluster.nodelibrary.types;

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
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.exceptions.ActiveDriverException;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpoint;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpointStore;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCursor;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.aeron.reader.ReaderDeliveryListener;
import org.eclipse.datagrid.storage.distributed.aeron.reader.StorageBinaryDataClientAeronArchive;
import org.eclipse.datagrid.storage.distributed.aeron.writer.AeronArchiveReplicationPublisher;
import org.eclipse.datagrid.storage.distributed.aeron.writer.AeronReplicationWriteCoordinator;
import org.eclipse.datagrid.storage.distributed.aeron.writer.AeronStorageBinaryTargetDistributing;
import org.eclipse.datagrid.storage.distributed.types.AtomicFileStore;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacket;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

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
	/* Test-only, thread-confined seam used by the forked crash harness. */
	private static final ThreadLocal<BiConsumer<String, Long>> CRASH_HOOK = new ThreadLocal<>();
	private static final ThreadLocal<Long> CHECKPOINT_SEQUENCE = new ThreadLocal<>();
	private static final long STALE_DRIVER_RETRY_DELAY_MILLIS = 100L;

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
		return new Transport(AeronSettings.fromEnvironment(properties));
	}

	/**
	 * Checks that a stopped recording ends at the last terminal checkpoint.
	 * Any archive tail or missing prefix needs an explicit reseed because this
	 * provider does not truncate or replay an ambiguous writer tail.
	 */
	static void validateRecordingPositions(final long stopPosition, final long checkpointPosition)
	{
		if (stopPosition < 0)
		{
			throw new IllegalStateException("RESEED_REQUIRED: recording is still active");
		}
		if (stopPosition < checkpointPosition)
		{
			throw new IllegalStateException("RESEED_REQUIRED: archive stop position precedes checkpoint");
		}
		if (stopPosition > checkpointPosition)
		{
			throw new IllegalStateException("RESEED_REQUIRED: archive contains an uncheckpointed tail");
		}
	}

	private record AeronSettings(
		AeronReplicationConfiguration replication,
		UUID clusterId,
		long epoch,
		int streamId,
		long recordingId,
		String liveChannel,
		String replayChannel,
		String controlChannel,
		String controlResponseChannel,
		Path aeronDirectory,
		Path archiveDirectory,
		Path checkpointPath,
		UUID nodeId,
		UUID storeGeneration,
		int archiveFileSyncLevel,
		boolean externalArchive,
		String role
	)
	{
		private static AeronSettings fromEnvironment(final NodelibraryPropertiesProvider properties)
		{
			if (properties == null) throw new NullPointerException("properties");
			if (!properties.replicationRoleConfigured())
			{
				throw new IllegalArgumentException(
					"ECLIPSE_DATAGRID_REPLICATION_ROLE must be explicitly configured for Aeron");
			}
			final String configuredRole = properties.replicationRole();
			final String role = configuredRole == null
				? "writer" : configuredRole.trim().toLowerCase(java.util.Locale.ROOT);
			if (!role.equals("writer") && !role.equals("reader") && !role.equals("backup-reader"))
			{
				throw new IllegalArgumentException("ECLIPSE_DATAGRID_REPLICATION_ROLE must be writer, reader, or backup-reader");
			}
			final Properties values = new Properties();
			put(values, properties, AeronReplicationConfiguration.TERM_LENGTH_PROPERTY, "ECLIPSE_DATAGRID_AERON_TERM_LENGTH");
			put(values, properties, AeronReplicationConfiguration.MTU_LENGTH_PROPERTY, "ECLIPSE_DATAGRID_AERON_MTU_LENGTH");
			put(values, properties, AeronReplicationConfiguration.CHUNK_SIZE_PROPERTY, "ECLIPSE_DATAGRID_AERON_CHUNK_SIZE");
			put(values, properties, AeronReplicationConfiguration.MAX_TRANSACTION_BYTES_PROPERTY,
				"ECLIPSE_DATAGRID_AERON_MAX_TRANSACTION_BYTES");
			put(values, properties, AeronReplicationConfiguration.DURABILITY_MODE_PROPERTY,
				"ECLIPSE_DATAGRID_AERON_REPLICATION_DURABILITY_MODE");
			put(values, properties, AeronReplicationConfiguration.DURABILITY_MODE_PROPERTY,
				"ECLIPSE_DATAGRID_AERON_DURABILITY_MODE");
			put(values, properties, AeronReplicationConfiguration.OFFER_TIMEOUT_NANOS_PROPERTY,
				"ECLIPSE_DATAGRID_AERON_OFFER_TIMEOUT_NANOS");
			final AeronReplicationConfiguration replication = AeronReplicationConfiguration.from(values);
			if (replication.durabilityMode() == org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode.LOCAL_DURABLE_FIRST)
			{
				throw new IllegalArgumentException(
					"LOCAL_DURABLE_FIRST is not supported by the Aeron provider; use ARCHIVE_FIRST or ENQUEUE_THEN_ARCHIVE");
			}
			final String cluster = value(properties, "ECLIPSE_DATAGRID_AERON_CLUSTER_ID", null);
			if (cluster == null) throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_CLUSTER_ID is required");
			final long epoch = parseLong(properties, "ECLIPSE_DATAGRID_AERON_EPOCH", "1");
			final int streamId = parseInt(properties, "ECLIPSE_DATAGRID_AERON_STREAM_ID", "1001");
			final long recordingId = parseLong(properties, "ECLIPSE_DATAGRID_AERON_RECORDING_ID", "-1");
			if (epoch < 0 || streamId < 0 || recordingId < -1)
			{
				throw new IllegalArgumentException("Aeron epoch/stream/recording settings are out of range");
			}
			final Path aeronDirectory = Paths.get(value(properties, "ECLIPSE_DATAGRID_AERON_DIRECTORY", "/tmp/eclipse-datagrid-aeron"));
			final Path archiveDirectory = Paths.get(value(properties, "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY",
				aeronDirectory.resolveSibling(aeronDirectory.getFileName() + ".archive").toString()));
			if (properties.isProdMode() && (aeronDirectory.toString().startsWith("/tmp") || archiveDirectory.toString().startsWith("/tmp")))
			{
				throw new IllegalArgumentException("Aeron directories must not use /tmp in production mode");
			}
			final String configuredNodeId = value(properties, "ECLIPSE_DATAGRID_AERON_NODE_ID", null);
			final String configuredStoreGeneration = value(
				properties, "ECLIPSE_DATAGRID_AERON_STORE_GENERATION", null);
			if (configuredNodeId == null || configuredStoreGeneration == null)
			{
				throw new IllegalArgumentException(
					"ECLIPSE_DATAGRID_AERON_NODE_ID and ECLIPSE_DATAGRID_AERON_STORE_GENERATION are required");
			}
			final UUID nodeId = parseUuid(configuredNodeId, "ECLIPSE_DATAGRID_AERON_NODE_ID");
			final UUID storeGeneration = parseUuid(
				configuredStoreGeneration, "ECLIPSE_DATAGRID_AERON_STORE_GENERATION");
			// MediaDriver recreates its directory on startup, so checkpoints must live outside it.
			final Path checkpointPath = Paths.get(value(properties, "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH",
				aeronDirectory.resolveSibling(aeronDirectory.getFileName() + ".writer.checkpoint").toString()));
			final Path normalizedAeronDirectory = aeronDirectory.toAbsolutePath().normalize();
			if (archiveDirectory.toAbsolutePath().normalize().startsWith(normalizedAeronDirectory) ||
				checkpointPath.toAbsolutePath().normalize().startsWith(normalizedAeronDirectory))
			{
				throw new IllegalArgumentException("Aeron archive and checkpoint paths must be outside the MediaDriver directory");
			}
			if (properties.isProdMode() && (!aeronDirectory.isAbsolute() || !archiveDirectory.isAbsolute() || !checkpointPath.isAbsolute()))
			{
				throw new IllegalArgumentException("Aeron directories and checkpoint path must be absolute in production mode");
			}
			final int archiveFileSyncLevel = parseInt(properties, "ECLIPSE_DATAGRID_AERON_FILE_SYNC_LEVEL", "1");
			if (archiveFileSyncLevel < 0 || archiveFileSyncLevel > 2)
			{
				throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_FILE_SYNC_LEVEL must be 0, 1, or 2");
			}
			final String externalArchiveValue = value(
				properties, "ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE", "false");
			if (!"true".equalsIgnoreCase(externalArchiveValue) &&
				!"false".equalsIgnoreCase(externalArchiveValue))
			{
				throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE must be true or false");
			}
			final boolean externalArchive = Boolean.parseBoolean(externalArchiveValue);
			final String liveChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
				"aeron:udp?control=localhost:40123|control-mode=dynamic|fc=max");
			final String replayChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL", "aeron:udp?endpoint=localhost:0");
			final String controlChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_CONTROL_CHANNEL", "aeron:udp?endpoint=localhost:40124");
			final String controlResponseChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_CONTROL_RESPONSE_CHANNEL", "aeron:udp?endpoint=localhost:0");
			if (properties.isProdMode() && (wildcardEndpoint(liveChannel) || wildcardEndpoint(replayChannel) ||
				wildcardEndpoint(controlChannel) || wildcardEndpoint(controlResponseChannel)))
			{
				throw new IllegalArgumentException("wildcard Aeron endpoints are not allowed in production mode");
			}
			return new AeronSettings(
				replication,
				parseUuid(cluster, "ECLIPSE_DATAGRID_AERON_CLUSTER_ID"),
				epoch,
				streamId,
				recordingId,
				liveChannel,
				replayChannel,
				controlChannel,
				controlResponseChannel,
				aeronDirectory,
				archiveDirectory,
				checkpointPath,
				nodeId,
				storeGeneration,
				archiveFileSyncLevel,
				externalArchive,
				role
			);
		}

		private static void put(final Properties values, final NodelibraryPropertiesProvider properties,
			final String property, final String environment)
		{
			final String value = value(properties, environment, null);
			if (value != null)
			{
				values.setProperty(property, value);
			}
		}

		private static String value(final NodelibraryPropertiesProvider properties, final String name, final String fallback)
		{
			final String configured = properties.replicationProperty(name);
			final String value = configured == null ? System.getenv(name) : configured;
			return value == null || value.isBlank() ? fallback : value;
		}

		private static String channel(final NodelibraryPropertiesProvider properties, final String name, final String fallback)
		{
			final String channel = value(properties, name, fallback).trim();
			if (channel.isEmpty() || channel.chars().anyMatch(Character::isWhitespace) ||
				channel.equals("aeron:udp") ||
				!(channel.startsWith("aeron:udp?") ||
					channel.equals("aeron:ipc") || channel.startsWith("aeron:ipc?")))
			{
				throw new IllegalArgumentException("Invalid Aeron channel for " + name + ": " + channel);
			}
			if (channel.startsWith("aeron:udp?"))
			{
				boolean endpoint = false;
				boolean control = false;
				for (final String option : channel.substring(channel.indexOf('?') + 1).split("\\|"))
				{
					if (option.startsWith("control="))
					{
						control = true;
						validateUdpAddress(option.substring("control=".length()), name);
					}
					else if (option.startsWith("endpoint="))
					{
						endpoint = true;
						validateUdpAddress(option.substring("endpoint=".length()), name);
					}
				}
				if (!endpoint && !control)
				{
					throw new IllegalArgumentException("Aeron UDP channel must specify endpoint= or control= for " + name);
				}
			}
			return channel;
		}

		private static void validateUdpAddress(final String address, final String name)
		{
			final String portText;
			if (address.startsWith("["))
			{
				final int closingBracket = address.indexOf(']');
				if (closingBracket <= 1 || closingBracket + 1 >= address.length() ||
					address.charAt(closingBracket + 1) != ':')
				{
					throw new IllegalArgumentException("Aeron UDP address must include host and port for " + name);
				}
				portText = address.substring(closingBracket + 2);
			}
			else
			{
				final int colon = address.lastIndexOf(':');
				if (colon <= 0 || colon == address.length() - 1)
				{
					throw new IllegalArgumentException("Aeron UDP address must include host and port for " + name);
				}
				portText = address.substring(colon + 1);
			}
			if (portText.isEmpty())
			{
				throw new IllegalArgumentException("Aeron UDP address must include host and port for " + name);
			}
			try
			{
				final int port = Integer.parseInt(portText);
				if (port < 0 || port > 65535) throw new NumberFormatException();
			}
			catch (final NumberFormatException failure)
			{
				throw new IllegalArgumentException("Invalid Aeron UDP address for " + name + ": " + address, failure);
			}
		}

		private static boolean wildcardEndpoint(final String channel)
		{
			return channel.contains("=0.0.0.0:") || channel.contains("=*:");
		}

		private static int parseInt(final NodelibraryPropertiesProvider properties, final String name, final String fallback)
		{
			try { return Integer.parseInt(value(properties, name, fallback).trim()); }
			catch (final NumberFormatException failure) { throw new IllegalArgumentException("Invalid " + name, failure); }
		}

		private static long parseLong(final NodelibraryPropertiesProvider properties, final String name, final String fallback)
		{
			try { return Long.parseLong(value(properties, name, fallback).trim()); }
			catch (final NumberFormatException failure) { throw new IllegalArgumentException("Invalid " + name, failure); }
		}

		private static UUID parseUuid(final String value, final String name)
		{
			try
			{
				final UUID parsed = UUID.fromString(value);
				if (parsed.equals(new UUID(0L, 0L)))
				{
					throw new IllegalArgumentException(name + " must not be the zero UUID");
				}
				return parsed;
			}
			catch (final IllegalArgumentException failure) { throw new IllegalArgumentException("Invalid " + name, failure); }
		}
	}

	private static final class Transport implements ClusterReplicationTransport
	{
		private final AeronSettings settings;
		private final AtomicLong nextSequence = new AtomicLong();
		private volatile AutoCloseable driver;
		private volatile Aeron aeron;
		private volatile AeronArchive archive;
		private volatile AeronArchiveReplicationPublisher writer;
		private volatile AeronReplicationWriteCoordinator coordinator;
		private volatile StorageBinaryDataClientAeronArchive reader;
		/** Recording identity selected at writer startup; retained when RecordingPos is briefly unavailable. */
		private long writerRecordingId;
		/* Published together after the terminal checkpoint is durable.  Readers of
		 * positionProvider() must never combine fields from two transactions. */
		private volatile WriterBoundary writerBoundary = new WriterBoundary(-1, -1, -1);
		private volatile boolean writerRecoveryInProgress;
		private volatile boolean writerRecoveryValidated;
		private volatile ReplicationHealth.State writerRecoveryState;
		private long writerRecoveryFingerprint = Long.MIN_VALUE;
		/** First asynchronous MediaDriver failure; health must not hide it. */
		private volatile RuntimeException driverFailure;
		private ClusterStorageBinaryDataDistributor distributor;
		private String distributorStream;
		private volatile boolean closed;

		private record WriterBoundary(long sequence, long recordingId, long position)
		{
		}

		private Transport(final AeronSettings settings)
		{
			this.settings = settings;
			this.writerRecordingId = settings.recordingId();
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
			this.distributorStream = streamName;
			this.distributor = new ClusterStorageBinaryDataDistributor()
			{
				private volatile long index = -1;
				private volatile boolean ignored;
				private String dictionary;

				@Override public void messageIndex(final long value)
				{
					if (value < -1 || value == Long.MAX_VALUE)
					{
						throw new IllegalArgumentException("message index must be in [-1, Long.MAX_VALUE)");
					}
					this.index = value;
					nextSequence.accumulateAndGet(value + 1, Math::max);
					if (writer != null) writer.synchronizeNextSequence(value + 1);
				}
				@Override public long messageIndex() { return this.index; }
				@Override public void ignoreDistribution(final boolean value) { this.ignored = value; }
				@Override public boolean ignoreDistribution() { return this.ignored; }
				@Override public synchronized void distributeTypeDictionary(final String value)
				{
					this.dictionary = value;
				}
				@Override public synchronized String consumeTypeDictionary()
				{
					final String value = this.dictionary;
					this.dictionary = null;
					return value;
				}

				@Override
				public synchronized void distributeData(final org.eclipse.serializer.persistence.binary.types.Binary data)
				{
					if (this.ignored) return;
					if (!"writer".equals(settings.role()))
					{
						throw new IllegalStateException("Aeron replication distributor is writer-only");
					}
					throw new UnsupportedOperationException(
						"Aeron data distribution must use persistenceTargetFactory so local Store acceptance " +
						"and the durable replication fence share one transaction boundary");
				}

				/** The provider owns the shared Aeron/archive runtime. */
				@Override public void dispose() { }
			};
			return this.distributor;
		}

		@Override
		public synchronized UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
			final String streamName, final org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor distributor)
		{
			this.ensureOpen();
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
			if ("writer".equals(this.settings.role()) && !this.settings.externalArchive())
			{
				return ClusterStorageBinaryDataClient.NoOp(startingCursor, cursorListener);
			}
			this.rejectUncertainReaderImport();
			if (this.settings.recordingId() < 0)
			{
				throw new IllegalStateException("ECLIPSE_DATAGRID_AERON_RECORDING_ID is required for a reader");
			}
			if (this.reader != null)
			{
				final StorageBinaryDataClientAeronArchive previous = this.reader;
				this.reader = null;
				previous.dispose();
			}
			this.ensureRuntime();
			final ReplicationCursor cursor = startingCursor == null
				? new ReplicationCursor("aeron", null, -1, new byte[0]) : startingCursor;
			final boolean aeronCursor = "aeron".equalsIgnoreCase(cursor.transport());
			if (!aeronCursor && !"none".equalsIgnoreCase(cursor.transport()))
			{
				throw new IllegalArgumentException("cursor belongs to transport " + cursor.transport());
			}
			if (aeronCursor && cursor.storeGeneration() != null &&
				!this.settings.storeGeneration().equals(cursor.storeGeneration()))
			{
				throw new IllegalArgumentException("cursor store generation does not match Aeron configuration");
			}
			final AtomicReference<StorageBinaryDataClientAeronArchive> readerRef = new AtomicReference<>();
			final byte[] positionBytes = cursor.providerPosition();
			final long cursorPosition;
			if (positionBytes.length == Long.BYTES)
			{
				cursorPosition = ByteBuffer.wrap(positionBytes).getLong();
			}
			else if (positionBytes.length == Long.BYTES * 2)
			{
				final ByteBuffer encoded = ByteBuffer.wrap(positionBytes);
				final long recordingId = encoded.getLong();
				if (recordingId != this.settings.recordingId())
				{
					throw new IllegalArgumentException("cursor recording does not match configured recording");
				}
				cursorPosition = encoded.getLong();
			}
			else if (positionBytes.length == 0)
			{
				cursorPosition = -1;
			}
			else
			{
				throw new IllegalArgumentException("invalid Aeron cursor position encoding");
			}
			if (aeronCursor && cursor.logicalSequence() >= 0 && cursorPosition < 0)
			{
				throw new IllegalArgumentException("Aeron cursor has a sequence but no recording position");
			}
			this.reader = StorageBinaryDataClientAeronArchive.New(
				this.aeron,
				archiveContext(),
				this.settings.recordingId(),
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
				new ReceiverAdapter(packetAcceptor),
				() ->
				{
					final StorageBinaryDataClientAeronArchive current = readerRef.get();
					if (current != null && current == this.reader)
					{
						final AeronReplicationCursor snapshot = current.cursor(
							this.settings.nodeId(), this.settings.storeGeneration(), this.settings.recordingId());
						this.nextSequence.accumulateAndGet(snapshot.sequence() + 1, Math::max);
						if (commitPosition && cursorListener != null)
						{
							cursorListener.onChange(MessageInfo.New(snapshot.sequence(), "aeron",
								this.settings.storeGeneration(), encodePosition(snapshot.recordingId(), snapshot.recordingPosition())));
						}
					}
				},
				this.readerDeliveryListener()
			);
			readerRef.set(this.reader);
			return new ClientAdapter(this.reader, this.settings.recordingId(), this.settings.storeGeneration());
		}

		private Path readerUncertaintyPath()
		{
			return this.settings.checkpointPath().resolveSibling(
				this.settings.checkpointPath().getFileName() + ".reader-inflight");
		}

		private void rejectUncertainReaderImport()
		{
			final Path path = this.readerUncertaintyPath();
			if (!Files.exists(path)) return;
			try
			{
				final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(path);
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
					final AeronReplicationCheckpoint checkpoint = new AeronReplicationCheckpoint(
						AeronReplicationCheckpoint.RecordType.READER_CURSOR,
						AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST,
						AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN,
						settings.clusterId(), settings.nodeId(), settings.storeGeneration(), settings.recordingId(),
						settings.epoch(), sequence, position, dataLength, dataChunkCount, crc32c);
					try
					{
						AeronReplicationCheckpointStore.write(path, checkpoint);
					}
					catch (final IOException failure)
					{
						throw reseedRequired("cannot persist uncertain reader import marker: " + path, failure);
					}
				}

				@Override
				public void afterStoreImport()
				{
					try
					{
						AtomicFileStore.delete(path);
					}
					catch (final IOException failure)
					{
						throw reseedRequired("cannot clear uncertain reader import marker: " + path, failure);
					}
				}
			};
		}

		@Override
		public ReplicationPositionProvider positionProvider(final String streamName)
		{
			synchronized (this)
			{
				this.ensureOpen();
			}
			return new ReplicationPositionProvider()
			{
				public void init() { }
				public synchronized ReplicationCursor latest()
				{
					synchronized (Transport.this)
					{
						if (!"writer".equals(Transport.this.settings.role()))
						{
							throw new UnsupportedOperationException(
								"Aeron reader cannot provide the writer's latest committed position");
						}
						final WriterBoundary boundary = Transport.this.writerBoundary;
						// Keep the position self-describing even before a recording or
						// reader has produced a concrete position (-1). This preserves
						// the recording identity and lets consumers distinguish an
						// uninitialized cursor from a legacy/foreign cursor.
						return new ReplicationCursor("aeron", null, boundary.sequence(),
							encodePosition(boundary.recordingId(), boundary.position()));
					}
				}
				public void close() { }
			};
		}

		@Override
		public ReplicationLogRetention retention()
		{
			synchronized (this)
			{
				this.ensureOpen();
			}
			/* Retention is deliberately unsupported until fixed-reader identity and
			 * durable ACK wiring are configured.  A no-op silently leaks Archive disk
			 * and makes callers believe history was reclaimed; fail explicitly instead. */
			return new ReplicationLogRetention()
			{
				@Override
				public boolean isSupported() { return false; }

				public void deleteThrough(final ReplicationCursor cursor)
				{
					throw new UnsupportedOperationException(
						"Aeron Archive retention is unsupported until authenticated reader watermarks are configured");
				}
				public void close() { }
			};
		}

		@Override
		public ReplicationHealth health(
			final StorageControllerAdapter storage,
			final ClusterStorageBinaryDataClient client
		)
		{
			synchronized (this)
			{
				this.ensureOpen();
			}
			return new ReplicationHealth()
			{
				private volatile boolean active = true;
				public void init() { }
				public boolean isReady() { return active && !Transport.this.closed && storage.isReady() &&
					Transport.this.driverFailure == null && writerCheckpointState() == null && ("writer".equals(settings.role()) ||
						client != null && client.isLive()); }
				public boolean isHealthy() { return active && !Transport.this.closed && storage.isReady() &&
					Transport.this.driverFailure == null && writerCheckpointState() == null && ("writer".equals(settings.role()) ||
						client != null && client.isRunning()); }
				public ReplicationHealth.State state()
				{
					final ReplicationHealth.State checkpointState = writerCheckpointState();
					if (checkpointState != null) return checkpointState;
					if (!active || Transport.this.closed || Transport.this.driverFailure != null ||
						client == null || client.failure() != null)
						return ReplicationHealth.State.FAILED;
					return "writer".equals(settings.role()) || client.isLive()
						? ReplicationHealth.State.LIVE : ReplicationHealth.State.REPLAYING;
				}
				public void close() { active = false; }
			};
		}

		private synchronized ReplicationHealth.State writerCheckpointState()
		{
			if (this.driverFailure != null)
			{
				return ReplicationHealth.State.FAILED;
			}
			if (!"writer".equals(this.settings.role()) || this.writerRecoveryInProgress)
			{
				return null;
			}
			if (this.writer != null)
			{
				/* A live writer can fail closed after an offer, await, or checkpoint
				 * error without being discarded immediately.  Do not advertise LIVE
				 * while that publisher can no longer accept a durable transaction. */
				return this.writer.isFailed() ? ReplicationHealth.State.FAILED : null;
			}
			/* Runtime health must not reinterpret the normal PREPARING/ENQUEUED
			 * fence that exists during a healthy write.  Restart validation is done
			 * once by ensureWriter() and its result is retained here. */
			final long recoveryFingerprint = this.writerRecoveryFingerprint();
			if (this.writerRecoveryValidated && recoveryFingerprint == this.writerRecoveryFingerprint)
			{
				return this.writerRecoveryState;
			}
			if (!Files.exists(this.settings.checkpointPath()) && !Files.exists(this.inFlightCheckpointPath()))
			{
				this.writerRecoveryFingerprint = recoveryFingerprint;
				this.writerRecoveryValidated = true;
				this.writerRecoveryState = null;
				return null;
			}
			try
			{
				this.readWriterCheckpoint(false);
				this.writerRecoveryFingerprint = this.writerRecoveryFingerprint();
				this.writerRecoveryValidated = true;
				this.writerRecoveryState = null;
				return null;
			}
			catch (final RuntimeException failure)
			{
				this.writerRecoveryValidated = true;
				this.writerRecoveryFingerprint = this.writerRecoveryFingerprint();
				this.writerRecoveryState = failure.getMessage() != null && failure.getMessage().startsWith("RESEED_REQUIRED:")
					? ReplicationHealth.State.RESEED_REQUIRED : ReplicationHealth.State.FAILED;
				return this.writerRecoveryState;
			}
		}

		private long writerRecoveryFingerprint()
		{
			long fingerprint = 1L;
			for (final Path path : List.of(this.settings.checkpointPath(), this.inFlightCheckpointPath()))
			{
				try
				{
					if (Files.exists(path))
					{
						fingerprint = 31L * fingerprint + Files.getLastModifiedTime(path).toMillis();
						fingerprint = 31L * fingerprint + Files.size(path);
					}
				}
				catch (final IOException failure)
				{
					return Long.MIN_VALUE;
				}
			}
			return fingerprint;
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
							? AeronArchiveReplicationPublisher.ExtendRemote(this.archive, recordingId,
								this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
								this.settings.epoch(), initialSequence)
							: AeronArchiveReplicationPublisher.Extend(this.archive, recordingId,
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
						? AeronArchiveReplicationPublisher.NewRemote(this.archive, this.settings.liveChannel(),
							this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
							this.settings.epoch(), initialSequence)
						: AeronArchiveReplicationPublisher.New(this.archive, this.settings.liveChannel(),
						this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
						this.settings.epoch(), initialSequence));
				}
				this.writerRecordingId = recordingId;
				this.writerRecoveryState = null;
				this.writerRecoveryValidated = true;
				return this.writer;
			}
			catch (final RuntimeException failure)
			{
				this.writerRecoveryState = failure.getMessage() != null && failure.getMessage().startsWith("RESEED_REQUIRED:")
					? ReplicationHealth.State.RESEED_REQUIRED : ReplicationHealth.State.FAILED;
				this.writerRecoveryValidated = true;
				throw failure;
			}
			catch (final Error failure)
			{
				this.writerRecoveryState = ReplicationHealth.State.FAILED;
				this.writerRecoveryValidated = true;
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
				this.coordinator = this.ensureWriter().newWriteCoordinator(
					this.settings.replication().durabilityMode(), this::persistWriterCheckpoint);
			}
			return this.coordinator;
		}

		private AeronReplicationCheckpoint loadWriterCheckpoint()
		{
			final AeronReplicationCheckpoint checkpoint = this.readWriterCheckpoint(true);
			if (checkpoint != null)
			{
				if (checkpoint.recordingId() >= 0) this.writerRecordingId = checkpoint.recordingId();
				this.writerBoundary = new WriterBoundary(
					checkpoint.transactionSequence(), checkpoint.recordingId(), checkpoint.recordingPosition());
			}
			return checkpoint;
		}

		/** Reads and validates checkpoint state without changing live transport state. */
		private AeronReplicationCheckpoint readWriterCheckpoint(final boolean cleanupCoveredInFlight)
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
						if (cleanupCoveredInFlight)
						{
							AtomicFileStore.delete(inFlightPath);
						}
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
					throw new IllegalStateException(
						"RESEED_REQUIRED: writer checkpoint is not restartable (state=" + checkpoint.state() +
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
			if (checkpoint.recordType() != AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT ||
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
				stopPosition = this.archive.getStopPosition(recordingId);
			}
			catch (final RuntimeException failure)
			{
				throw reseedRequired("cannot inspect recording tail " + recordingId, failure);
			}
			try
			{
				AeronClusterReplicationTransportProvider.validateRecordingPositions(
					stopPosition, checkpoint.recordingPosition());
			}
			catch (final IllegalStateException failure)
			{
				final String message = failure.getMessage();
				if (message != null && message.startsWith("RESEED_REQUIRED:"))
				{
					throw failure;
				}
				throw reseedRequired("archive boundary validation failed", failure);
			}
		}

		private void validateEmptyRecordingBoundary(final long recordingId)
		{
			/* A recording without a terminal checkpoint is safe only when it is
			 * genuinely empty; otherwise the next writer could reuse an orphaned
			 * sequence. */
			try
			{
				final long start = this.archive.getStartPosition(recordingId);
				final long stop = this.archive.getStopPosition(recordingId);
				if (stop < 0 || start < 0 || stop > start)
				{
					throw reseedRequired("recording has data but no terminal writer checkpoint", null);
				}
			}
			catch (final IllegalStateException failure)
			{
				final String message = failure.getMessage();
				if (message != null && message.startsWith("RESEED_REQUIRED:")) throw failure;
				throw reseedRequired("cannot inspect empty recording boundary " + recordingId, failure);
			}
			catch (final RuntimeException failure)
			{
				throw reseedRequired("cannot inspect empty recording boundary " + recordingId, failure);
			}
		}

		private static IllegalStateException reseedRequired(final String message,
			final Throwable cause)
		{
			return cause == null ? new IllegalStateException("RESEED_REQUIRED: " + message) :
				new IllegalStateException("RESEED_REQUIRED: " + message, cause);
		}

		private void persistWriterCheckpoint(final AeronReplicationCheckpoint.State state,
			final long sequence, final int dataLength, final int dataChunkCount,
			final int dataCrc32c, final long position)
		{
			if (state == AeronReplicationCheckpoint.State.REJECTED && position < 0 &&
				(sequence < 0 || (this.writerRecordingId < 0 && this.settings.recordingId() < 0)))
			{
				try
				{
					/* A local Store rejection left no Aeron transaction. Remove only
					 * the fence; never advance the terminal writer checkpoint with a
					 * synthetic sequence. */
					AtomicFileStore.delete(this.inFlightCheckpointPath());
				}
				catch (final IOException failure)
				{
					throw new IllegalStateException("cannot clear local enqueue fence", failure);
				}
				return;
			}
			if (state == AeronReplicationCheckpoint.State.PREPARING ||
				state == AeronReplicationCheckpoint.State.ENQUEUED)
			{
				this.writeCheckpoint(this.inFlightCheckpointPath(), state, sequence, dataLength,
					dataChunkCount, dataCrc32c, position);
				return;
			}
			if (state != AeronReplicationCheckpoint.State.COMMITTED &&
				state != AeronReplicationCheckpoint.State.REJECTED &&
				state != AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN)
			{
				return;
			}
			this.writeCheckpoint(this.settings.checkpointPath(), state, sequence, dataLength,
				dataChunkCount, dataCrc32c, position);
			crashPoint("AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE", sequence);
			try
			{
				AtomicFileStore.delete(this.inFlightCheckpointPath());
			}
			catch (final IOException failure)
			{
				throw new IllegalStateException("cannot clear in-flight writer checkpoint", failure);
			}
			if (state == AeronReplicationCheckpoint.State.COMMITTED ||
				state == AeronReplicationCheckpoint.State.REJECTED)
			{
				this.writerBoundary = new WriterBoundary(sequence, this.writerRecordingId, position);
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
			return new AeronReplicationCheckpoint(
				AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
				AeronReplicationCheckpoint.DurabilityMode.valueOf(this.settings.replication().durabilityMode().name()),
				state, this.settings.clusterId(), this.settings.nodeId(), this.settings.storeGeneration(),
				recordingId, this.settings.epoch(), sequence, position, dataLength, dataChunkCount, dataCrc32c);
		}

		private synchronized void ensureRuntime()
		{
			this.ensureOpen();
			if (this.aeron != null)
			{
				return;
			}
			ensurePrivateDirectory(this.settings.aeronDirectory());
			final Path checkpointParent = this.settings.checkpointPath().toAbsolutePath().getParent();
			if (checkpointParent == null)
			{
				throw new IllegalArgumentException("Aeron checkpoint path must have a parent directory");
			}
			ensurePrivateDirectory(checkpointParent);
			try
			{
				/* Verify the metadata boundary before launching any driver. Both writer
				 * checkpoints and reader uncertainty markers use this directory, so a
				 * failed probe must be reported before either role can import or publish. */
				AtomicFileStore.verify(this.settings.checkpointPath());
			}
			catch (final IOException failure)
			{
				throw new IllegalStateException(
					"Aeron replication metadata storage does not support atomic replacement", failure
				);
			}
			if ("writer".equals(this.settings.role()))
			{
				ensurePrivateDirectory(this.settings.archiveDirectory());
			}
			final MediaDriver.Context media = new MediaDriver.Context()
				.aeronDirectoryName(this.settings.aeronDirectory().toString())
				.threadingMode(ThreadingMode.SHARED)
				.mtuLength(this.settings.replication().mtuLength())
				.publicationTermBufferLength(this.settings.replication().termLength())
				/* A local Archive spy is the writer's durable publication when no remote
				 * reader is connected.  Make that connection explicit; relying on the
				 * channel's ssc option alone leaves the MediaDriver default (false) in
				 * control and turns a zero-reader writer into NOT_CONNECTED. */
				.spiesSimulateConnection("writer".equals(this.settings.role()) && !this.settings.externalArchive())
				.errorHandler(this::recordDriverFailure)
				.dirDeleteOnStart(false)
				.dirDeleteOnShutdown(false);
			if (!"writer".equals(this.settings.role()) || this.settings.externalArchive())
			{
				try
				{
					crashPoint("BEFORE_PUBLICATION_CONNECTED", -1L);
					this.driver = launchDriver(media, () -> MediaDriver.launch(media.clone()));
					this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(this.settings.aeronDirectory().toString()));
					this.archive = AeronArchive.connect(this.archiveContext());
				}
				catch (final RuntimeException | Error failure)
				{
					final RuntimeException cleanupFailure = this.closeRuntimeQuietly();
					if (cleanupFailure != null) failure.addSuppressed(cleanupFailure);
					throw failure;
				}
				return;
			}
			final Archive.Context archiveContext = new Archive.Context()
				.aeronDirectoryName(this.settings.aeronDirectory().toString())
				.archiveDir(this.settings.archiveDirectory().toFile())
				.deleteArchiveOnStart(false)
				.threadingMode(io.aeron.archive.ArchiveThreadingMode.SHARED)
				.controlChannel(this.settings.controlChannel())
				.localControlChannel("aeron:ipc")
				.replicationChannel(this.settings.replayChannel())
				.fileSyncLevel(this.settings.archiveFileSyncLevel())
				.catalogFileSyncLevel(this.settings.archiveFileSyncLevel());
			try
			{
				crashPoint("BEFORE_PUBLICATION_CONNECTED", -1L);
				this.driver = launchDriver(media,
					() -> ArchivingMediaDriver.launch(media.clone(), archiveContext.clone()));
				this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(this.settings.aeronDirectory().toString()));
				this.archive = AeronArchive.connect(this.archiveContext());
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
			final RuntimeException normalized = failure instanceof RuntimeException runtime
				? runtime
				: new IllegalStateException("Aeron MediaDriver failed", failure);
			if (this.driverFailure == null)
			{
				this.driverFailure = normalized;
			}
		}

		/**
		 * A hard-killed embedded driver can leave its directory marker behind until
		 * the operating system releases the file lock. Retry only the known stale-
		 * driver failure; all other startup failures remain immediate.
		 */
		private static <T extends AutoCloseable> T launchDriver(final MediaDriver.Context context,
			final Supplier<T> launcher)
		{
			RuntimeException lastFailure = null;
			final long timeoutMillis = Math.max(3_000L, context.driverTimeoutMs() + 1_000L);
			final long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
			while (System.nanoTime() < deadline)
			{
				try
				{
					return launcher.get();
				}
				catch (final ActiveDriverException failure)
				{
					lastFailure = failure;
					waitForStaleDriverCleanup();
				}
				catch (final IllegalStateException failure)
				{
					/* ArchivingMediaDriver reports a stale Archive mark through
					 * Agrona's IllegalStateException rather than ActiveDriverException.
					 * Match only that exact protocol message; unrelated startup errors
					 * must remain fail-fast. */
					final String message = failure.getMessage();
					if (message == null || !message.startsWith("active mark file detected: "))
					{
						throw failure;
					}
					lastFailure = failure;
					waitForStaleDriverCleanup();
				}
			}
			throw new IllegalStateException(
				"Aeron driver directory remained active after " + timeoutMillis + " milliseconds", lastFailure);
		}

		private static void waitForStaleDriverCleanup()
		{
			try
			{
				Thread.sleep(STALE_DRIVER_RETRY_DELAY_MILLIS);
			}
			catch (final InterruptedException interrupted)
			{
				Thread.currentThread().interrupt();
				throw new IllegalStateException("interrupted while waiting for stale Aeron driver cleanup", interrupted);
			}
		}

		private static void ensurePrivateDirectory(final Path path)
		{
			try
			{
				Files.createDirectories(path);
				if (!Files.isDirectory(path))
				{
					throw new IOException("path is not a directory");
				}
				try
				{
					Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"));
				}
				catch (final UnsupportedOperationException ignored)
				{
					// DOS/Windows filesystems do not expose POSIX permissions.
				}
			}
			catch (final IOException failure)
			{
				throw new IllegalStateException("cannot create or protect Aeron directory " + path, failure);
			}
		}

		private RuntimeException closeRuntimeQuietly()
		{
			RuntimeException failure = null;
			if (this.archive != null)
			{
				try { this.archive.close(); }
				catch (final RuntimeException closeFailure) { failure = closeFailure; }
			}
			if (this.aeron != null)
			{
				try { this.aeron.close(); }
				catch (final RuntimeException closeFailure)
				{
					if (failure == null) failure = closeFailure;
					else failure.addSuppressed(closeFailure);
				}
			}
			if (this.driver != null)
			{
				try { this.driver.close(); }
				catch (final Exception closeFailure)
				{
					final RuntimeException wrapped = new IllegalStateException(
						"failed to close Aeron driver during startup cleanup", closeFailure);
					if (failure == null) failure = wrapped;
					else failure.addSuppressed(wrapped);
				}
			}
			this.archive = null;
			this.aeron = null;
			this.driver = null;
			return failure;
		}

		private AeronArchive.Context archiveContext()
		{
			return new AeronArchive.Context()
				.aeron(this.aeron)
				.aeronDirectoryName(this.settings.aeronDirectory().toString())
				.controlRequestChannel(this.settings.controlChannel())
				.controlResponseChannel(this.settings.controlResponseChannel())
				.messageTimeoutNs(this.settings.replication().offerTimeoutNanos());
		}

		private void ensureOpen()
		{
			if (this.closed)
			{
				throw new IllegalStateException("Aeron transport is closed");
			}
		}

		private static byte[] encodePosition(final long recordingId, final long position)
		{
			return ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.BIG_ENDIAN)
				.putLong(recordingId).putLong(position).array();
		}

		@Override
		public synchronized void close()
		{
			if (this.reader == null && this.writer == null && this.coordinator == null &&
				this.archive == null && this.aeron == null && this.driver == null)
			{
				this.closed = true;
				return;
			}
			this.closed = true;
			this.distributor = null;
			this.distributorStream = null;

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
			if (this.reader == null && this.writer != null)
			{
				try
				{
					this.writer.close();
					this.writer = null;
				}
				catch (final RuntimeException writerFailure)
				{
					if (failure == null) failure = writerFailure;
					else failure.addSuppressed(writerFailure);
				}
			}
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
			if (this.reader == null && this.archive != null)
			{
				try
				{
					this.archive.close();
					this.archive = null;
				}
				catch (final RuntimeException archiveFailure)
				{
					if (failure == null) failure = archiveFailure;
					else failure.addSuppressed(archiveFailure);
				}
			}
			if (this.reader == null && this.aeron != null)
			{
				try
				{
					this.aeron.close();
					this.aeron = null;
				}
				catch (final RuntimeException aeronFailure)
				{
					if (failure == null) failure = aeronFailure;
					else failure.addSuppressed(aeronFailure);
				}
			}
			if (this.reader == null && this.driver != null)
			{
				try
				{
					this.driver.close();
					this.driver = null;
				}
				catch (final Exception driverFailure)
				{
					final RuntimeException wrapped = new IllegalStateException("failed to close Aeron driver", driverFailure);
					if (failure == null) failure = wrapped;
					else failure.addSuppressed(wrapped);
				}
			}
			if (failure != null) throw new IllegalStateException("failed to close Aeron transport", failure);
		}
	}

	private record ReceiverAdapter(ClusterStorageBinaryDataPacketAcceptor packetAcceptor)
		implements org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataReceiver
	{
		public void receiveTypeDictionary(final String value)
		{
			final byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
			this.packetAcceptor().accept(java.util.List.of(StorageBinaryDataPacket.New(
				MessageType.TYPE_DICTIONARY, bytes.length, 0, 1, ByteBuffer.wrap(bytes))));
		}
		public void receiveData(final org.eclipse.serializer.persistence.binary.types.Binary value)
		{
			final ByteBuffer[] buffers = value.buffers();
			int totalLength = 0;
			for (final ByteBuffer buffer : buffers) totalLength = Math.addExact(totalLength, buffer.remaining());
			if (totalLength == 0) return;
			final List<StorageBinaryDataPacket> packets = new ArrayList<>(buffers.length);
			for (int i = 0; i < buffers.length; i++)
			{
				packets.add(StorageBinaryDataPacket.New(
					MessageType.DATA, totalLength, i, buffers.length, buffers[i].duplicate()));
			}
			this.packetAcceptor().accept(packets);
			this.packetAcceptor().awaitApplied();
		}
	}

	private record ClientAdapter(
		StorageBinaryDataClientAeronArchive delegate,
		long recordingId,
		UUID storeGeneration
	) implements ClusterStorageBinaryDataClient
	{
		public void start() { this.delegate().start(); }
		public void stopAtLatestMessage() { this.delegate().stopAtLatestMessage(); }
		public MessageInfo messageInfo()
		{
			final StorageBinaryDataClientAeronArchive.CursorSnapshot snapshot = this.delegate().cursorSnapshot();
			return MessageInfo.New(snapshot.sequence(), "aeron", this.storeGeneration(),
				Transport.encodePosition(this.recordingId(), snapshot.position()));
		}
		public boolean isRunning() { return this.delegate().isRunning(); }
		public boolean isLive() { return this.delegate().isLive(); }
		public RuntimeException failure() { return this.delegate().failure(); }
		public void resume() { this.delegate().resume(); }
		public void dispose() { this.delegate().dispose(); }
	}
}
