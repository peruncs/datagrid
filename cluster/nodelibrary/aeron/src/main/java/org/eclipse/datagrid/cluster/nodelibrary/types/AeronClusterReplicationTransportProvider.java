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
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpoint;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpointStore;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCursor;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import org.eclipse.datagrid.storage.distributed.aeron.reader.StorageBinaryDataClientAeronArchive;
import org.eclipse.datagrid.storage.distributed.aeron.writer.AeronArchiveReplicationPublisher;
import org.eclipse.datagrid.storage.distributed.aeron.writer.AeronReplicationWriteCoordinator;
import org.eclipse.datagrid.storage.distributed.aeron.writer.AeronStorageBinaryTargetDistributing;
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
import java.util.function.UnaryOperator;

/**
 * Selectable Aeron provider. Runtime resources are created on first use of the
 * selected reader or writer operation and are released by the transport's
 * {@link ClusterReplicationTransport#close()} method.
 */
public final class AeronClusterReplicationTransportProvider
	implements ClusterReplicationTransportProvider
{
	@Override
	public String id()
	{
		return "aeron";
	}

	@Override
	public ClusterReplicationTransport create(final NodelibraryPropertiesProvider properties)
	{
		return new Transport(AeronSettings.fromEnvironment(properties));
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
			final int colon = address.lastIndexOf(':');
			if (colon <= 0 || colon == address.length() - 1)
			{
				throw new IllegalArgumentException("Aeron UDP address must include host and port for " + name);
			}
			try
			{
				final int port = Integer.parseInt(address.substring(colon + 1));
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
		/** Last transaction whose terminal marker was durably checkpointed. */
		private long writerCommittedSequence = -1;
		private ClusterStorageBinaryDataDistributor distributor;
		private String distributorStream;
		private boolean closed;

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

				@Override
				public synchronized void distributeData(final org.eclipse.serializer.persistence.binary.types.Binary data)
				{
					if (this.ignored) return;
					if (!"writer".equals(settings.role()))
					{
						throw new IllegalStateException("Aeron replication distributor is writer-only");
					}
					final AeronArchiveReplicationPublisher publisher = ensureWriter();
					publisher.synchronizeNextSequence(nextSequence.get());
					try
					{
						publisher.publishTransaction(
							this.dictionary == null ? null : this.dictionary.getBytes(java.nio.charset.StandardCharsets.UTF_8),
							data.buffers()
						);
					}
					finally
					{
						this.dictionary = null;
					}
					this.index++;
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
			if ("writer".equals(this.settings.role()))
			{
				return ClusterStorageBinaryDataClient.NoOp(startingCursor, cursorListener);
			}
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
				}
			);
			readerRef.set(this.reader);
			return new ClientAdapter(this.reader, this.settings.recordingId(), this.settings.storeGeneration());
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
						final long sequence = "writer".equals(Transport.this.settings.role())
							? Transport.this.writerCommittedSequence
							: Transport.this.reader == null ? -1 : Transport.this.reader.lastResolvedSequence();
						final long recordingId;
						final long position;
						if (Transport.this.writer != null)
						{
							final long discovered = Transport.this.writer.recordingId();
							recordingId = discovered >= 0 ? discovered : Transport.this.writerRecordingId;
                            position = Transport.this.writer.recordedPosition();
						}
						else
						{
							recordingId = Transport.this.settings.recordingId();
							position = Transport.this.reader == null ? -1 : Transport.this.reader.lastResolvedPosition();
						}
						// Keep the position self-describing even before a recording or
						// reader has produced a concrete position (-1). This preserves
						// the recording identity and lets consumers distinguish an
						// uninitialized cursor from a legacy/foreign cursor.
						final byte[] encodedPosition = encodePosition(recordingId, position);
						return new ReplicationCursor("aeron", null, sequence, encodedPosition);
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
			// Retention is deliberately disabled until fixed-reader identity and
			// durable ACK wiring are configured; deleting without that watermark can
			// strand a reader and cause irreversible data loss.
			return new ReplicationLogRetention()
			{
				public void deleteThrough(final ReplicationCursor cursor) { }
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
				public boolean isReady() { return active && storage.isReady() &&
					writerCheckpointState() == null && ("writer".equals(settings.role()) || client.isRunning()); }
				public boolean isHealthy() { return active && storage.isReady() &&
					writerCheckpointState() == null && ("writer".equals(settings.role()) || client.isRunning()); }
				public ReplicationHealth.State state()
				{
					final ReplicationHealth.State checkpointState = writerCheckpointState();
					if (checkpointState != null) return checkpointState;
					if (!active || client.failure() != null) return ReplicationHealth.State.FAILED;
					return "writer".equals(settings.role()) || client.isLive()
						? ReplicationHealth.State.LIVE : ReplicationHealth.State.REPLAYING;
				}
				public void close() { active = false; }
			};
		}

		private synchronized ReplicationHealth.State writerCheckpointState()
		{
			if (!"writer".equals(this.settings.role()) || !Files.exists(this.settings.checkpointPath()))
			{
				return null;
			}
			try
			{
				this.loadWriterCheckpoint();
				return null;
			}
			catch (final RuntimeException failure)
			{
				return failure.getMessage() != null && failure.getMessage().startsWith("RESEED_REQUIRED")
					? ReplicationHealth.State.RESEED_REQUIRED : ReplicationHealth.State.FAILED;
			}
		}

		private synchronized AeronArchiveReplicationPublisher ensureWriter()
		{
			this.ensureRuntime();
			if (this.writer == null)
			{
				final AeronReplicationCheckpoint checkpoint = this.loadWriterCheckpoint();
				final long initialSequence = checkpoint == null ? this.nextSequence.get() :
					checkpoint.transactionSequence() + 1;
				this.nextSequence.set(initialSequence);
				final long recordingId = checkpoint != null ? checkpoint.recordingId() : this.settings.recordingId();
				if (recordingId >= 0)
				{
					if (checkpoint != null && checkpoint.recordingPosition() >= 0)
					{
						final long stopPosition = this.archive.getStopPosition(recordingId);
						if (stopPosition >= 0 && stopPosition < checkpoint.recordingPosition())
						{
							throw new IllegalStateException(
								"Aeron archive stop position precedes the committed writer checkpoint; reseed is required");
						}
					}
					this.writer = AeronArchiveReplicationPublisher.Extend(this.archive, recordingId,
						this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
						this.settings.epoch(), initialSequence);
				}
				else
				{
					if (checkpoint != null && checkpoint.transactionSequence() >= 0)
					{
						throw new IllegalStateException("writer checkpoint has no recording identity; reseed is required");
					}
					this.writer = AeronArchiveReplicationPublisher.New(this.archive, this.settings.liveChannel(),
						this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
						this.settings.epoch(), initialSequence);
				}
				this.writerRecordingId = recordingId;
			}
			return this.writer;
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
			if (!Files.exists(this.settings.checkpointPath())) return null;
			try
			{
				final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(this.settings.checkpointPath());
				if (checkpoint.recordType() != AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT ||
					!checkpoint.clusterId().equals(this.settings.clusterId()) ||
					!checkpoint.nodeId().equals(this.settings.nodeId()) ||
					!checkpoint.storeGeneration().equals(this.settings.storeGeneration()) ||
					checkpoint.writerEpoch() != this.settings.epoch())
				{
					throw new IllegalStateException("writer checkpoint identity does not match Aeron configuration");
				}
				if (checkpoint.state() != AeronReplicationCheckpoint.State.COMMITTED &&
					checkpoint.state() != AeronReplicationCheckpoint.State.REJECTED)
				{
					throw new IllegalStateException(
						"RESEED_REQUIRED: writer checkpoint is not restartable (state=" + checkpoint.state() +
						", sequence=" + checkpoint.transactionSequence() + ", path=" +
							this.settings.checkpointPath() + ")");
				}
				if (this.settings.recordingId() >= 0 && checkpoint.recordingId() >= 0 &&
					this.settings.recordingId() != checkpoint.recordingId())
				{
					throw new IllegalStateException("configured recording does not match writer checkpoint");
				}
				this.writerCommittedSequence = checkpoint.transactionSequence();
				return checkpoint;
			}
			catch (final IOException failure)
			{
				throw new IllegalStateException("cannot read writer checkpoint; reseed is required", failure);
			}
		}

		private void persistWriterCheckpoint(final AeronReplicationCheckpoint.State state,
			final long sequence, final int dataLength, final int dataChunkCount,
			final int dataCrc32c, final long position)
		{
			if (state != AeronReplicationCheckpoint.State.COMMITTED &&
				state != AeronReplicationCheckpoint.State.REJECTED &&
				state != AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN)
			{
				return;
			}
			final long discoveredRecordingId = this.writer == null ? Aeron.NULL_VALUE : this.writer.recordingId();
			if (discoveredRecordingId >= 0)
			{
				this.writerRecordingId = discoveredRecordingId;
			}
			final long recordingId = this.writerRecordingId >= 0 ? this.writerRecordingId : this.settings.recordingId();
			final AeronReplicationCheckpoint checkpoint = new AeronReplicationCheckpoint(
				AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
				AeronReplicationCheckpoint.DurabilityMode.valueOf(this.settings.replication().durabilityMode().name()),
				state, this.settings.clusterId(), this.settings.nodeId(), this.settings.storeGeneration(),
				recordingId, this.settings.epoch(), sequence, position, dataCrc32c
			);
			try
			{
				AeronReplicationCheckpointStore.write(this.settings.checkpointPath(), checkpoint);
				if (state == AeronReplicationCheckpoint.State.COMMITTED ||
					state == AeronReplicationCheckpoint.State.REJECTED)
				{
					this.writerCommittedSequence = sequence;
				}
			}
			catch (final IOException failure)
			{
				throw new IllegalStateException("cannot persist writer checkpoint", failure);
			}
		}

		private synchronized void ensureRuntime()
		{
			this.ensureOpen();
			if (this.aeron != null)
			{
				return;
			}
			ensurePrivateDirectory(this.settings.aeronDirectory());
			if ("writer".equals(this.settings.role()))
			{
				ensurePrivateDirectory(this.settings.archiveDirectory());
				final Path checkpointParent = this.settings.checkpointPath().toAbsolutePath().getParent();
				if (checkpointParent == null)
				{
					throw new IllegalArgumentException("Aeron checkpoint path must have a parent directory");
				}
				ensurePrivateDirectory(checkpointParent);
			}
			final MediaDriver.Context media = new MediaDriver.Context()
				.aeronDirectoryName(this.settings.aeronDirectory().toString())
				.threadingMode(ThreadingMode.SHARED)
				.mtuLength(this.settings.replication().mtuLength())
				.publicationTermBufferLength(this.settings.replication().termLength())
				.dirDeleteOnStart(false)
				.dirDeleteOnShutdown(false);
			if (!"writer".equals(this.settings.role()))
			{
				try
				{
					this.driver = MediaDriver.launch(media);
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
				this.driver = ArchivingMediaDriver.launch(media, archiveContext);
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

		private static void ensurePrivateDirectory(final Path path)
		{
			try
			{
				final boolean existed = Files.exists(path);
				Files.createDirectories(path);
				if (!Files.isDirectory(path))
				{
					throw new IOException("path is not a directory");
				}
				// Do not chmod an operator-owned parent directory merely because a
				// checkpoint lives below it. Newly-created runtime directories are
				// always protected; existing paths retain their administrator policy.
				if (existed) return;
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
			if (this.closed)
			{
				return;
			}
			this.closed = true;
			RuntimeException failure = null;
			try { if (this.reader != null) this.reader.dispose(); }
			catch (final RuntimeException e) { failure = e; }
			try { if (this.writer != null) this.writer.close(); }
			catch (final RuntimeException e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
			try { if (this.coordinator != null) this.coordinator.dispose(); }
			catch (final RuntimeException e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
			try { if (this.archive != null) this.archive.close(); }
			catch (final RuntimeException e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
			try { if (this.aeron != null) this.aeron.close(); }
			catch (final RuntimeException e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
			try
			{
				if (this.driver != null) this.driver.close();
			}
			catch (final Exception e)
			{
				if (failure == null) failure = new IllegalStateException("failed to close Aeron driver", e);
				else failure.addSuppressed(e);
			}
			this.reader = null;
			this.writer = null;
			this.coordinator = null;
			this.archive = null;
			this.aeron = null;
			this.driver = null;
			this.distributor = null;
			this.distributorStream = null;
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
		public MessageInfo messageInfo() { return MessageInfo.New(this.delegate().lastResolvedSequence(), "aeron",
			this.storeGeneration(), Transport.encodePosition(this.recordingId(), this.delegate().lastResolvedPosition())); }
		public boolean isRunning() { return this.delegate().isRunning(); }
		public boolean isLive() { return this.delegate().isLive(); }
		public RuntimeException failure() { return this.delegate().failure(); }
		public void resume() { this.delegate().resume(); }
		public void dispose() { this.delegate().dispose(); }
	}
}
