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
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.exceptions.ActiveDriverException;
import org.agrona.SystemUtil;
import org.eclipse.datagrid.cluster.nodelibrary.types.*;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronAuthenticatedWatermark;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpoint;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCheckpointStore;
import org.eclipse.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
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
	/** Creates a provider that reads Aeron settings when a transport is created. */
	public AeronClusterReplicationTransportProvider()
	{
	}

	/* Test-only, thread-confined seam used by the forked crash harness. */
	private static final System.Logger LOGGER =
		System.getLogger(AeronClusterReplicationTransportProvider.class.getName());
	private static final ThreadLocal<BiConsumer<String, Long>> CRASH_HOOK = new ThreadLocal<>();
	private static final ThreadLocal<Long> CHECKPOINT_SEQUENCE = new ThreadLocal<>();
	private static final long STALE_DRIVER_RETRY_DELAY_MILLIS = 100L;
	/* Filesystem free-space queries can block on network-backed volumes.  Do not
	 * put one in every Store write or health probe; a short cache still fails
	 * closed quickly enough when the Archive approaches its admission threshold. */
	private static final long ARCHIVE_CAPACITY_CACHE_NANOS = 250_000_000L;

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
		final Transport transport = new Transport(AeronSettings.fromEnvironment(properties));
		/* Probe metadata durability before any synchronized runtime startup path. */
		transport.verifyMetadataStorage();
		return transport;
	}

	/**
	 * Checks that a stopped recording ends at the last terminal checkpoint.
	 * Any archive tail or missing prefix needs an explicit reseed because this
	 * provider does not truncate or replay an ambiguous writer tail.
	 *
	 * @param stopPosition Archive stop position
	 * @param checkpointPosition position of the last terminal checkpoint
	 */
	public static void validateRecordingPositions(final long stopPosition, final long checkpointPosition)
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

	/** Immutable settings captured for one Aeron transport instance. */
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
		long minimumArchiveFreeBytes,
		boolean externalArchive,
		String role,
		ThreadingMode threadingMode,
		ArchiveThreadingMode archiveThreadingMode,
		int archiveSegmentFileLength,
		long archiveLowStorageSpaceThreshold,
		int maxConcurrentReplays,
		String archiveReplicationChannel,
		byte[] retentionSecret,
		Set<UUID> retentionReaders
	)
	{
		private AeronSettings
		{
			retentionSecret = retentionSecret == null ? null : retentionSecret.clone();
			retentionReaders = retentionReaders == null ? Set.of() : Set.copyOf(retentionReaders);
		}

		@Override
		public byte[] retentionSecret()
		{
			return this.retentionSecret == null ? null : this.retentionSecret.clone();
		}

		@Override
		public Set<UUID> retentionReaders()
		{
			return this.retentionReaders;
		}

		private static AeronSettings fromEnvironment(final NodelibraryPropertiesProvider properties)
		{
			if (properties == null) throw new NullPointerException("properties");
			if (!properties.replicationRoleConfigured())
			{
				throw new IllegalArgumentException(
					"ECLIPSE_DATAGRID_REPLICATION_ROLE must be explicitly configured for Aeron");
			}
			final String configuredRole = properties.replicationRole();
			if (configuredRole == null || configuredRole.isBlank())
			{
				throw new IllegalArgumentException(
					"ECLIPSE_DATAGRID_REPLICATION_ROLE must be writer, reader, or backup-reader");
			}
			final String role = configuredRole.trim().toLowerCase(java.util.Locale.ROOT);
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
			put(values, properties, AeronReplicationConfiguration.READER_STOP_TIMEOUT_NANOS_PROPERTY,
				"ECLIPSE_DATAGRID_AERON_READER_STOP_TIMEOUT_NANOS");
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
			final Path normalizedArchiveDirectory = archiveDirectory.toAbsolutePath().normalize();
			final Path normalizedCheckpointPath = checkpointPath.toAbsolutePath().normalize();
			if (overlaps(normalizedAeronDirectory, normalizedArchiveDirectory) ||
				overlaps(normalizedAeronDirectory, normalizedCheckpointPath) ||
				overlaps(normalizedArchiveDirectory, normalizedCheckpointPath))
			{
				throw new IllegalArgumentException(
					"Aeron driver, archive, and checkpoint paths must not overlap: driver=" +
						aeronDirectory + ", archive=" + archiveDirectory + ", checkpoint=" + checkpointPath);
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
			if (properties.isProdMode() && archiveFileSyncLevel == 0)
			{
				throw new IllegalArgumentException(
					"ECLIPSE_DATAGRID_AERON_FILE_SYNC_LEVEL=0 is only allowed outside production mode");
			}
			final long minimumArchiveFreeBytes = parseLong(
				properties, "ECLIPSE_DATAGRID_AERON_MIN_ARCHIVE_FREE_BYTES", "0");
			if (minimumArchiveFreeBytes < 0)
			{
				throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_MIN_ARCHIVE_FREE_BYTES must be >= 0");
			}
			final String externalArchiveValue = value(
				properties, "ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE", "false");
			if (!"true".equalsIgnoreCase(externalArchiveValue) &&
				!"false".equalsIgnoreCase(externalArchiveValue))
			{
				throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE must be true or false");
			}
			final boolean externalArchive = Boolean.parseBoolean(externalArchiveValue);
			if (externalArchive && minimumArchiveFreeBytes > 0)
			{
				throw new IllegalArgumentException(
					"ECLIPSE_DATAGRID_AERON_MIN_ARCHIVE_FREE_BYTES is only supported for embedded Archive writers");
			}
			/* Keep development defaults self-contained, but make them match the
			 * production topology: explicit framing, dynamic MDC, and a stable
			 * channel alias. Production deployments must override localhost endpoints
			 * and are rejected below when they do not. */
			final String channelAlias = "datagrid-" + cluster;
			final String liveChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
				"aeron:udp?control=localhost:40123|control-mode=dynamic|fc=max|term-length=16m|alias=" + channelAlias);
			final String replayChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL",
				"aeron:udp?endpoint=localhost:0|control=localhost:40123|control-mode=dynamic");
			final String archiveReplicationChannel = channel(properties,
				"ECLIPSE_DATAGRID_AERON_ARCHIVE_REPLICATION_CHANNEL", "aeron:udp?endpoint=localhost:0");
			final byte[] retentionSecret = retentionSecret(properties);
			final Set<UUID> retentionReaders = retentionReaders(properties);
			final String controlChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_CONTROL_CHANNEL", "aeron:udp?endpoint=localhost:40124");
			final String controlResponseChannel = channel(properties, "ECLIPSE_DATAGRID_AERON_CONTROL_RESPONSE_CHANNEL", "aeron:udp?endpoint=localhost:0");
			final ThreadingMode threadingMode = threadingMode(properties);
			final ArchiveThreadingMode archiveThreadingMode = threadingMode == ThreadingMode.DEDICATED
				? ArchiveThreadingMode.DEDICATED : ArchiveThreadingMode.SHARED;
			final int archiveSegmentFileLength = parseInt(properties, "ECLIPSE_DATAGRID_AERON_ARCHIVE_SEGMENT_FILE_LENGTH",
				Integer.toString(Archive.Configuration.segmentFileLength()));
			final long archiveLowStorageSpaceThreshold = parseLong(properties,
				"ECLIPSE_DATAGRID_AERON_ARCHIVE_LOW_STORAGE_SPACE_THRESHOLD",
				Long.toString(Archive.Configuration.lowStorageSpaceThreshold()));
			final int maxConcurrentReplays = parseInt(properties, "ECLIPSE_DATAGRID_AERON_MAX_CONCURRENT_REPLAYS",
				Integer.toString(Archive.Configuration.maxConcurrentReplays()));
			if (archiveSegmentFileLength <= 0 || Integer.bitCount(archiveSegmentFileLength) != 1 ||
				archiveSegmentFileLength < replication.termLength() ||
				archiveLowStorageSpaceThreshold < 0 || maxConcurrentReplays <= 0)
			{
				throw new IllegalArgumentException(
					"Archive segment length must be a positive power of two; low-storage threshold must not be negative; " +
						"max concurrent replays must be positive");
			}
			validateFraming(liveChannel, "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL", replication);
			validateFraming(replayChannel, "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL", replication);
			validateFraming(archiveReplicationChannel,
				"ECLIPSE_DATAGRID_AERON_ARCHIVE_REPLICATION_CHANNEL", replication);
			if ("writer".equals(role) && !externalArchive)
			{
				validateWriterTopology(liveChannel);
			}
			if (properties.isProdMode() && (wildcardEndpoint(liveChannel) || wildcardEndpoint(replayChannel) ||
				wildcardEndpoint(controlChannel) || wildcardEndpoint(controlResponseChannel) ||
				wildcardEndpoint(archiveReplicationChannel)))
			{
				throw new IllegalArgumentException("wildcard Aeron endpoints are not allowed in production mode");
			}
			if (properties.isProdMode() && (loopbackEndpoint(liveChannel) || loopbackEndpoint(replayChannel) ||
				loopbackEndpoint(controlChannel) || loopbackEndpoint(controlResponseChannel) ||
				loopbackEndpoint(archiveReplicationChannel)))
			{
				throw new IllegalArgumentException(
					"loopback Aeron endpoints are not allowed in production mode; configure routable node addresses");
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
				minimumArchiveFreeBytes,
				externalArchive,
				role,
				threadingMode,
				archiveThreadingMode,
				archiveSegmentFileLength,
				archiveLowStorageSpaceThreshold,
				maxConcurrentReplays,
				archiveReplicationChannel,
				retentionSecret,
				retentionReaders
			);
		}

		private static Set<UUID> retentionReaders(final NodelibraryPropertiesProvider properties)
		{
			final String configured = value(properties, "ECLIPSE_DATAGRID_AERON_RETENTION_READERS", null);
			if (configured == null || configured.isBlank()) return Set.of();
			final HashSet<UUID> readers = new HashSet<>();
			for (final String value : configured.split(","))
			{
				final String text = value.trim();
				if (text.isEmpty()) throw new IllegalArgumentException(
					"ECLIPSE_DATAGRID_AERON_RETENTION_READERS contains an empty reader id");
				if (!readers.add(parseUuid(text, "ECLIPSE_DATAGRID_AERON_RETENTION_READERS")))
					throw new IllegalArgumentException("ECLIPSE_DATAGRID_AERON_RETENTION_READERS contains a duplicate reader id");
			}
			return Set.copyOf(readers);
		}

		private static byte[] retentionSecret(final NodelibraryPropertiesProvider properties)
		{
			final String configured = value(properties, "ECLIPSE_DATAGRID_AERON_RETENTION_SECRET", null);
			if (configured == null || configured.isBlank()) return null;
			try
			{
				final byte[] secret = Base64.getDecoder().decode(configured.trim());
				if (secret.length < 16) throw new IllegalArgumentException(
					"ECLIPSE_DATAGRID_AERON_RETENTION_SECRET must decode to at least 16 bytes");
				return secret;
			}
			catch (final IllegalArgumentException failure)
			{
				throw new IllegalArgumentException(
					"ECLIPSE_DATAGRID_AERON_RETENTION_SECRET must be base64", failure);
			}
		}

		private static ThreadingMode threadingMode(final NodelibraryPropertiesProvider properties)
		{
			final String configured = value(properties, "ECLIPSE_DATAGRID_AERON_THREADING_MODE",
				properties.isProdMode() ? "DEDICATED" : "SHARED");
			return switch (configured.trim().toUpperCase(java.util.Locale.ROOT))
			{
				case "SHARED" -> ThreadingMode.SHARED;
				case "DEDICATED" -> ThreadingMode.DEDICATED;
				default -> throw new IllegalArgumentException(
					"ECLIPSE_DATAGRID_AERON_THREADING_MODE must be SHARED or DEDICATED");
			};
		}

		private static void put(final Properties values, final NodelibraryPropertiesProvider properties,
			final String property, final String environment)
		{
			final String value = value(properties, environment, null);
			if (value != null)
			{
				final String previous = values.getProperty(property);
				if (previous != null && !equivalentSetting(previous, value))
				{
					throw new IllegalArgumentException(
						"Conflicting Aeron settings for " + property + ": " + previous + " and " + value);
				}
				values.setProperty(property, value);
			}
		}

		private static boolean equivalentSetting(final String left, final String right)
		{
			return left.trim().replace('-', '_').equalsIgnoreCase(right.trim().replace('-', '_'));
		}

		private static String value(final NodelibraryPropertiesProvider properties, final String name, final String fallback)
		{
			/* The provider owns the precedence rule.  Falling back directly to the
			 * process environment here would let an ambient variable override a
			 * deliberately isolated application/test provider. Nodelibrary's Env
			 * implementation already reads environment variables at its boundary. */
			final String configured = properties.replicationProperty(name);
			return configured == null || configured.isBlank() ? fallback : configured;
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
			try
			{
				ChannelUri.parse(channel);
			}
			catch (final RuntimeException failure)
			{
				throw new IllegalArgumentException("Invalid Aeron channel for " + name + ": " + channel, failure);
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

		/**
		 * Returns whether the configured channel explicitly disables Aeron spy
		 * connection simulation. The driver-level setting must not silently
		 * override an operator's explicit {@code ssc=false} choice.
		 */
		private static boolean explicitlyDisablesSpySimulation(final String channel)
		{
			final int query = channel.indexOf('?');
			if (query < 0) return false;
			for (final String option : channel.substring(query + 1).split("\\|"))
			{
				if (option.trim().equalsIgnoreCase("ssc=false")) return true;
			}
			return false;
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
			return channel.contains("=0.0.0.0:") || channel.contains("=*:") ||
				channel.contains("=[::]:") || channel.contains("=:::");
		}

		private static boolean loopbackEndpoint(final String channel)
		{
			return channel.contains("=localhost:") || channel.contains("=127.0.0.1:") ||
				channel.contains("=[::1]:") || channel.contains("=::1:");
		}

		private static boolean overlaps(final Path left, final Path right)
		{
			return left.startsWith(right) || right.startsWith(left);
		}

		/**
		 * Reject channel-level framing overrides that disagree with the values used
		 * to configure the MediaDriver and replication envelope. Without this check
		 * the channel silently wins and a writer and reader can use different term or
		 * MTU limits even though they share one replication configuration.
		 */
		private static void validateFraming(final String channel, final String name,
			final AeronReplicationConfiguration replication)
		{
			final ChannelUri uri;
			try
			{
				uri = ChannelUri.parse(channel);
			}
			catch (final RuntimeException failure)
			{
				throw new IllegalArgumentException("Invalid Aeron channel for " + name + ": " + channel, failure);
			}
			validateFramingOption(uri, CommonContext.TERM_LENGTH_PARAM_NAME, replication.termLength(), name);
			if (uri.isUdp())
			{
				validateFramingOption(uri, CommonContext.MTU_LENGTH_PARAM_NAME, replication.mtuLength(), name);
			}
		}

		private static void validateFramingOption(final ChannelUri uri, final String option,
			final int expected, final String name)
		{
			final String configured = uri.get(option);
			if (configured == null) return;
			final long value;
			try
			{
				value = SystemUtil.parseSize(option, configured);
			}
			catch (final RuntimeException failure)
			{
				throw new IllegalArgumentException("Invalid " + option + " in " + name + ": " + configured, failure);
			}
			if (value != expected)
			{
				throw new IllegalArgumentException(name + " " + option + "=" + configured +
					" conflicts with replication configuration value " + expected);
			}
		}

		private static void validateWriterTopology(final String channel)
		{
			final ChannelUri uri = ChannelUri.parse(channel);
			if (!uri.isUdp() || !CommonContext.MDC_CONTROL_MODE_DYNAMIC.equalsIgnoreCase(
				uri.get(CommonContext.MDC_CONTROL_MODE_PARAM_NAME)) ||
				!"max".equalsIgnoreCase(uri.get(CommonContext.FLOW_CONTROL_PARAM_NAME)))
			{
				throw new IllegalArgumentException(
					"Aeron writer live channel must use dynamic MDC with fc=max: " + channel);
			}
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

	/** Owns the Aeron resources for one node and one transport lifecycle. */
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
		private volatile AeronWriterBoundary writerBoundary = new AeronWriterBoundary(-1, -1, -1);
		private volatile boolean writerRecoveryInProgress;
		private volatile ReplicationHealth.State writerRecoveryState;
		/** First asynchronous MediaDriver failure; health must not hide it. */
		private volatile RuntimeException driverFailure;
		private ClusterStorageBinaryDataDistributor distributor;
		private String distributorStream;
		private ReplicationPositionProvider positionProvider;
		private ReplicationLogRetention retention;
		private AeronHealth health;
		private volatile boolean closed;
		private volatile boolean closing;
		private volatile long archiveSpaceCheckedNanos;
		private volatile long cachedArchiveUsableSpace = Long.MIN_VALUE;
		private final ThreadLocal<Boolean> deliveryCallback = new ThreadLocal<>();

		private Transport(final AeronSettings settings)
		{
			this.settings = settings;
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
			ensurePrivateDirectory(parent);
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
				},
				this::ensureCoordinator);
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
			this.rejectUncertainReaderImport();
			if (this.settings.recordingId() < 0)
			{
				throw new IllegalStateException("ECLIPSE_DATAGRID_AERON_RECORDING_ID is required for a reader");
			}
			if (this.reader != null)
			{
				final StorageBinaryDataClientAeronArchive previous = this.reader;
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
			if (aeronCursor && cursor.logicalSequence() >= 0 && cursor.storeGeneration() == null)
			{
				throw new IllegalArgumentException("resolved Aeron cursor must identify its Store generation");
			}
			final AtomicReference<StorageBinaryDataClientAeronArchive> readerRef = new AtomicReference<>();
			final long cursorPosition = this.cursorPosition(cursor, aeronCursor);
			if (aeronCursor && cursor.logicalSequence() >= 0 && cursorPosition < 0)
			{
				throw new IllegalArgumentException("Aeron cursor has a sequence but no recording position");
			}
			final StorageBinaryDataClientAeronArchive replacement;
			try
			{
				replacement = StorageBinaryDataClientAeronArchive.New(
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
							/* The reader-to-writer watermark transport is not deployed yet.
							 * Persist the ordinary cursor until a control channel can carry
							 * and authenticate a watermark at the writer. */
							final byte[] position = encodePosition(this.settings.recordingId(), snapshot.position());
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
			return new ClientAdapter(replacement, this.settings.recordingId(), this.settings.storeGeneration());
		}

		/** Returns the reader's own last resolved cursor for position-provider callers. */
		private ReplicationCursor readerCursor()
		{
			final StorageBinaryDataClientAeronArchive current = this.reader;
			if (current == null)
			{
				return new ReplicationCursor("aeron", this.settings.storeGeneration(), -1, new byte[0]);
			}
			final CursorSnapshot snapshot = current.cursorSnapshot();
			final byte[] position = snapshot.position() < 0
				? new byte[0]
				: encodePosition(this.settings.recordingId(), snapshot.position());
			return new ReplicationCursor("aeron", this.settings.storeGeneration(), snapshot.sequence(), position);
		}

		/** Decodes the versioned watermark or the legacy recording-position form. */
		private long cursorPosition(final ReplicationCursor cursor, final boolean aeronCursor)
		{
			final byte[] positionBytes = cursor.providerPosition();
			if (positionBytes.length == 0) return -1L;
			/* Legacy cursors remain valid when an operator enables retention. The
			 * authenticated form has a distinct, versioned length and is verified
			 * below, so the two formats are never guessed from the secret setting. */
			if (positionBytes.length == Long.BYTES || positionBytes.length == Long.BYTES * 2)
			{
				if (positionBytes.length == Long.BYTES) return ByteBuffer.wrap(positionBytes).getLong();
				final ByteBuffer encoded = ByteBuffer.wrap(positionBytes);
				final long recordingId = encoded.getLong();
				if (recordingId != this.settings.recordingId())
				{
					throw new IllegalArgumentException("cursor recording does not match configured recording");
				}
				return encoded.getLong();
			}
			if (this.settings.retentionSecret() != null)
			{
				try
				{
					final AeronAuthenticatedWatermark watermark = AeronAuthenticatedWatermark.decode(positionBytes);
					if (!watermark.verify(this.settings.retentionSecret()) ||
						!watermark.readerId().equals(this.settings.nodeId()) ||
						!watermark.clusterId().equals(this.settings.clusterId()) ||
						!watermark.storeGeneration().equals(this.settings.storeGeneration()) ||
						watermark.writerEpoch() != this.settings.epoch() ||
						watermark.recordingId() != this.settings.recordingId() ||
						(aeronCursor && watermark.sequence() != cursor.logicalSequence()))
					{
						throw new SecurityException(
							"Aeron cursor watermark identity or authentication is invalid");
					}
					return watermark.position();
				}
				catch (final SecurityException failure)
				{
					throw new IllegalStateException(
						"RESEED_REQUIRED: Aeron cursor secret or writer identity does not match", failure);
				}
				catch (final IllegalArgumentException failure)
				{
					/* A non-legacy cursor with a configured secret must be a valid
					 * authenticated watermark. */
					throw new IllegalStateException(
						"RESEED_REQUIRED: invalid authenticated Aeron cursor", failure);
				}
			}
			throw new IllegalArgumentException("invalid Aeron cursor position encoding");
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
				if (checkpoint.recordType() != AeronReplicationCheckpoint.RecordType.READER_CURSOR ||
					checkpoint.state() != AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN ||
					!checkpoint.clusterId().equals(this.settings.clusterId()) ||
					!checkpoint.nodeId().equals(this.settings.nodeId()) ||
					!checkpoint.storeGeneration().equals(this.settings.storeGeneration()) ||
					checkpoint.recordingId() != this.settings.recordingId() ||
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
							settings.clusterId(), settings.nodeId(), settings.storeGeneration(), settings.recordingId(),
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
					this.settings::storeGeneration,
					this::readerCursor);
			}
			return this.positionProvider;
		}

		@Override
		public synchronized ReplicationLogRetention retention()
		{
			this.ensureOpen();
			if (this.retention != null) return this.retention;
			final byte[] retentionSecret = this.settings.retentionSecret();
			if (retentionSecret == null || this.settings.retentionReaders().isEmpty() ||
				!"writer".equals(this.settings.role()) || this.settings.externalArchive())
			{
				/* Retention without a shared authentication key and an embedded Archive
				 * cannot prove that every reader has crossed the requested boundary. */
				this.retention = new ReplicationLogRetention()
				{
					@Override public boolean isSupported() { return false; }
					@Override public void deleteThrough(final ReplicationCursor cursor)
					{
						throw new UnsupportedOperationException(
							"Aeron Archive retention requires an embedded writer and an authenticated watermark secret");
					}
					@Override public void close() { }
				};
				return this.retention;
			}
			this.retention = new AeronArchiveRetention(retentionSecret, this.settings.retentionReaders(),
				this::ensureWriter, () -> this.archive, () -> this.writerRecordingId,
				() -> this.writerBoundary, this.settings.clusterId(), this.settings.storeGeneration(),
				this.settings.epoch(), this.settings.replication()::termLength,
				this.settings::archiveSegmentFileLength,
				false,
				this.settings.checkpointPath().resolveSibling(
					this.settings.checkpointPath().getFileName() + ".retention"));
			return this.retention;
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
					this::archiveCapacityAvailable,
					this::writerReadyForHealth,
					() -> "writer".equals(this.settings.role()),
					this::writerCheckpointState,
					this::archiveUsableSpaceBytes,
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
		private boolean writerReadyForHealth()
		{
			return this.writerReady();
		}

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
				final long discoveredRecordingId = this.writer.recordingId();
				this.writerRecordingId = discoveredRecordingId >= 0 ? discoveredRecordingId : recordingId;
				if (checkpoint == null && this.writerRecordingId >= 0)
				{
					long startPosition = -1L;
					try
					{
						startPosition = this.archive == null ? -1L :
							this.archive.getStartPosition(this.writerRecordingId);
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
				this.writerRecoveryState = failure.getMessage() != null && failure.getMessage().startsWith("RESEED_REQUIRED:")
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
					this::archiveCapacityAvailable);
			}
			return this.coordinator;
		}

		private boolean archiveCapacityAvailable()
		{
			final long minimum = this.settings.minimumArchiveFreeBytes();
			if (minimum == 0 || this.settings.externalArchive()) return true;
			return this.archiveUsableSpace() >= minimum;
		}

		private long archiveUsableSpaceBytes()
		{
			if (this.settings.externalArchive()) return -1L;
			return this.archiveUsableSpace();
		}

		private long archiveUsableSpace()
		{
			final long now = System.nanoTime();
			final long checked = this.archiveSpaceCheckedNanos;
			if (checked != 0L && now - checked < ARCHIVE_CAPACITY_CACHE_NANOS)
			{
				return this.cachedArchiveUsableSpace;
			}
			final long usable;
			try
			{
				usable = Files.getFileStore(this.settings.archiveDirectory()).getUsableSpace();
			}
			catch (final IOException failure)
			{
				/* Unknown capacity must not admit a write when a threshold is configured. */
				this.cachedArchiveUsableSpace = -1L;
				this.archiveSpaceCheckedNanos = now;
				return -1L;
			}
			this.cachedArchiveUsableSpace = usable;
			this.archiveSpaceCheckedNanos = now;
			return usable;
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
				stopPosition = this.archive.getStopPosition(recordingId);
			}
			catch (final RuntimeException failure)
			{
				throw reseedRequired("cannot inspect recording tail " + recordingId, failure);
			}
			try
			{
				validateRecordingPositions(
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
			if (state == AeronReplicationCheckpoint.State.COMMITTED ||
				state == AeronReplicationCheckpoint.State.REJECTED)
			{
				/* The terminal checkpoint is the durable boundary. Publish it to
				 * readers before best-effort cleanup of the diagnostic fence so a
				 * cleanup failure cannot make a durable commit look unavailable. */
				this.writerBoundary = new AeronWriterBoundary(sequence, this.writerRecordingId, position);
			}
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
			if (this.aeron != null && this.archive != null && this.driver != null)
			{
				return;
			}
			if (this.aeron != null || this.archive != null || this.driver != null)
			{
				/* A previous startup attempt may have failed after only part of the
				 * runtime was created.  Never treat that partial graph as usable: close
				 * what can be closed and fail with the cleanup cause instead of allowing a
				 * later writer operation to dereference a missing Archive or driver. */
				final RuntimeException cleanupFailure = this.closeRuntimeQuietly();
				if (cleanupFailure != null)
				{
					throw new IllegalStateException("Aeron runtime is partially initialized", cleanupFailure);
				}
			}
			ensurePrivateDirectory(this.settings.aeronDirectory());
			final Path checkpointParent = this.settings.checkpointPath().toAbsolutePath().getParent();
			if (checkpointParent == null)
			{
				throw new IllegalArgumentException("Aeron checkpoint path must have a parent directory");
			}
			ensurePrivateDirectory(checkpointParent);
			if ("writer".equals(this.settings.role()))
			{
				ensurePrivateDirectory(this.settings.archiveDirectory());
				/* The directory may have been created after an earlier health probe;
				 * force the first post-startup capacity check to observe the real store. */
				this.archiveSpaceCheckedNanos = 0L;
			}
			final MediaDriver.Context media = new MediaDriver.Context()
				.aeronDirectoryName(this.settings.aeronDirectory().toString())
				.threadingMode(this.settings.threadingMode())
				.mtuLength(this.settings.replication().mtuLength())
				.publicationTermBufferLength(this.settings.replication().termLength())
				/* A local Archive spy is the writer's durable publication when no remote
				 * reader is connected.  Make that connection explicit; relying on the
				 * channel's ssc option alone leaves the MediaDriver default (false) in
				 * control and turns a zero-reader writer into NOT_CONNECTED. */
				.spiesSimulateConnection("writer".equals(this.settings.role()) && !this.settings.externalArchive() &&
					!AeronSettings.explicitlyDisablesSpySimulation(this.settings.liveChannel()))
				.errorHandler(this::recordDriverFailure)
				.dirDeleteOnStart(false)
				.dirDeleteOnShutdown(false);
			if (!"writer".equals(this.settings.role()) || this.settings.externalArchive())
			{
				try
				{
					crashPoint("BEFORE_PUBLICATION_CONNECTED", -1L);
					this.driver = launchDriver(media, () -> MediaDriver.launch(media.clone()));
					this.aeron = Aeron.connect(new Aeron.Context()
						.aeronDirectoryName(this.settings.aeronDirectory().toString())
						.errorHandler(this::recordDriverFailure)
						.subscriberErrorHandler(this::recordDriverFailure));
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
				.threadingMode(this.settings.archiveThreadingMode())
				.controlChannel(this.settings.controlChannel())
				.localControlChannel("aeron:ipc")
				.replicationChannel(this.settings.archiveReplicationChannel())
				.segmentFileLength(this.settings.archiveSegmentFileLength())
				.lowStorageSpaceThreshold(this.settings.archiveLowStorageSpaceThreshold())
				.maxConcurrentReplays(this.settings.maxConcurrentReplays())
				.errorHandler(this::recordDriverFailure)
				.fileSyncLevel(this.settings.archiveFileSyncLevel())
				.catalogFileSyncLevel(this.settings.archiveFileSyncLevel());
			try
			{
				crashPoint("BEFORE_PUBLICATION_CONNECTED", -1L);
				this.driver = launchDriver(media,
					() -> ArchivingMediaDriver.launch(media.clone(), archiveContext.clone()));
				this.aeron = Aeron.connect(new Aeron.Context()
					.aeronDirectoryName(this.settings.aeronDirectory().toString())
					.errorHandler(this::recordDriverFailure)
					.subscriberErrorHandler(this::recordDriverFailure));
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
			else if (this.driverFailure != normalized)
			{
				/* Health keeps the first terminal cause, but later callbacks still carry
				 * useful diagnostics (and must not disappear silently). */
				LOGGER.log(System.Logger.Level.WARNING, "Additional Aeron transport failure", normalized);
			}
			final StorageBinaryDataClientAeronArchive current = this.reader;
			if (current != null)
			{
				current.fail(normalized);
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
				/* A symlink here would make the configured durability boundary point at
				 * an operator-unintended location (and could let MediaDriver cleanup
				 * touch another tree).  Resolve links explicitly at provisioning time
				 * instead of silently following one. */
				if (Files.isSymbolicLink(path))
				{
					throw new IOException("symbolic-link directory is not allowed: " + path);
				}
				try
				{
					Files.createDirectories(path,
						PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
				}
				catch (final UnsupportedOperationException ignored)
				{
					Files.createDirectories(path);
				}
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
			if (this.archive != null)
			{
				try
				{
					this.archive.close();
					this.archive = null;
				}
				catch (final RuntimeException closeFailure) { failure = appendFailure(failure, closeFailure); }
			}
			if (this.aeron != null)
			{
				try
				{
					this.aeron.close();
					/* A failed startup may be retried by the health probe. Never retain a
					 * successfully closed client that makes ensureRuntime() return early. */
					this.aeron = null;
				}
				catch (final RuntimeException closeFailure)
				{
					failure = appendFailure(failure, closeFailure);
				}
			}
			if (this.driver != null)
			{
				try
				{
					this.driver.close();
					this.driver = null;
				}
				catch (final Exception closeFailure)
				{
					final RuntimeException wrapped = new IllegalStateException(
						"failed to close Aeron driver during startup cleanup", closeFailure);
					failure = appendFailure(failure, wrapped);
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

		private AeronArchive.Context archiveContext()
		{
			return new AeronArchive.Context()
				.aeron(this.aeron)
				.aeronDirectoryName(this.settings.aeronDirectory().toString())
				.controlRequestChannel(this.settings.controlChannel())
				.controlResponseChannel(this.settings.controlResponseChannel())
				.errorHandler(this::recordDriverFailure)
				.messageTimeoutNs(this.settings.replication().offerTimeoutNanos());
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

		private static byte[] encodePosition(final long recordingId, final long position)
		{
			return ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.BIG_ENDIAN)
				.putLong(recordingId).putLong(position).array();
		}

		@Override
		public synchronized void close()
		{
			this.ensureNotDeliveryCallback();
			if (this.reader == null && this.writer == null && this.coordinator == null &&
				this.archive == null && this.aeron == null && this.driver == null && this.retention == null)
			{
				this.closed = true;
				this.closing = false;
				return;
			}
			this.closing = true;

			RuntimeException failure = null;
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
					failure = appendFailure(failure, readerFailure);
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
					/* The publication can be terminal even when the Archive stop request
					 * failed because the Archive disappeared. A second close observes that
					 * terminal state and lets the shared Aeron resources shut down. If the
					 * publication is not terminal, the retry preserves the existing
					 * retryable-close behavior. */
					try
					{
						this.writer.close();
						this.writer = null;
					}
					catch (final RuntimeException retryFailure)
					{
						if (failure != retryFailure) failure.addSuppressed(retryFailure);
					}
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
			/* Do not close the shared runtime while a writer or coordinator still owns
			 * the publication. A failed abort/stop is retryable; closing Aeron here would
			 * turn that retry into a use-after-close and leak the unresolved sequence. */
			if (this.reader == null && this.writer == null && this.coordinator == null && this.retention == null && this.archive != null)
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
			if (this.reader == null && this.writer == null && this.coordinator == null && this.retention == null && this.archive == null && this.aeron != null)
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
			if (this.reader == null && this.writer == null && this.coordinator == null && this.retention == null && this.archive == null &&
				this.aeron == null && this.driver != null)
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
			this.distributor = null;
			this.distributorStream = null;
			this.closed = true;
			this.closing = false;
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
		UUID storeGeneration
	) implements ClusterStorageBinaryDataClient
	{
		public void start() { this.delegate().start(); }
		public void stopAtLatestMessage() { this.delegate().stopAtLatestMessage(); }
		public MessageInfo messageInfo()
		{
					final CursorSnapshot snapshot = this.delegate().cursorSnapshot();
			return MessageInfo.New(snapshot.sequence(), "aeron", this.storeGeneration(),
				Transport.encodePosition(this.recordingId(), snapshot.position()));
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
