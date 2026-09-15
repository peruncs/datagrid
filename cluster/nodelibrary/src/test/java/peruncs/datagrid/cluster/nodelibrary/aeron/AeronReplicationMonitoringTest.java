package peruncs.datagrid.cluster.nodelibrary.aeron;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import peruncs.datagrid.cluster.nodelibrary.exceptions.ReplicationPositionUnavailableException;
import peruncs.datagrid.cluster.nodelibrary.node.NodelibraryPropertiesProvider;
import peruncs.datagrid.cluster.nodelibrary.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.nodelibrary.replication.ClusterStorageBinaryDataClient;
import peruncs.datagrid.cluster.nodelibrary.replication.ClusterStorageBinaryDataDistributor;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationCursor;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationHealth;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationPositionProvider;
import peruncs.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCursor;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Verifies provider health reflects writer readiness and checkpoint state. */
class AeronReplicationMonitoringTest
{
	@Test
	void aeronProviderCreatesAeronTransport()
	{
		try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(properties("writer")))
		{
			assertEquals("aeron", transport.id());
		}
	}

	/** Verifies writer provider exposes aeron and reports live without reader client. */
	@Test
	void writerProviderExposesAeronAndReportsLiveWithoutReaderClient()
	{
		try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(properties("writer")))
		{
			/* Health inspection must not start the runtime. Start it through the
			 * explicit position-provider lifecycle first. */
			final ReplicationPositionProvider positionProvider = transport.positionProvider("stream");
			positionProvider.init();
			positionProvider.latest();
			final ClusterStorageBinaryDataClient client = transport.client(null, "stream", null, null, false);
			final ReplicationHealth health = transport.health(() -> true, client);
			health.init();
			assertEquals("aeron", transport.id());
			assertTrue(health.isReady());
			assertTrue(health.isHealthy());
			assertEquals(ReplicationHealth.State.LIVE, health.state());
			health.close();
		}
	}

	/** Verifies position provider uses self describing recording position. */
	@Test
	void positionProviderUsesSelfDescribingRecordingPosition()
	{
		try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(properties("writer")))
		{
			final ReplicationPositionProvider positionProvider = transport.positionProvider("stream");
			assertThrows(ReplicationPositionUnavailableException.class, positionProvider::latest);
			positionProvider.init();
			final ReplicationCursor cursor = positionProvider.latest();
			assertEquals("aeron", cursor.transport());
			final AeronReplicationCursor aeronCursor = AeronReplicationCursor.decode(cursor.providerPosition());
			assertEquals(cursor.logicalSequence(), aeronCursor.sequence());
			assertEquals(cursor.storeGeneration(), aeronCursor.storeGeneration());
		}
	}

	/** The embedded writer commits through its local Archive spy with no remote reader. */
	@Test
	void writerCommitsWithoutRemoteReader()
	{
		try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(properties("writer")))
		{
			transport.positionProvider("stream").init();
			final ClusterStorageBinaryDataDistributor distributor = transport.distributor("stream", false);
			final PersistenceTarget<Binary> target = transport.persistenceTargetFactory("stream", distributor)
				.apply(new PersistenceTarget<>()
				{
					public void write(final Binary ignored) { }
					public boolean isWritable() { return true; }
				});
			target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[] { 1, 2, 3 })));
			assertEquals(0L, transport.positionProvider("stream").latest().logicalSequence());
		}
	}

	/** Verifies Store binaries cannot bypass the fenced persistence target. */
	@Test
	void distributorRejectsDataWithoutAStoreTarget()
	{
		try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(properties("writer")))
		{
			final ClusterStorageBinaryDataDistributor distributor = transport.distributor("stream", false);
			assertThrows(IllegalStateException.class,
				() -> distributor.distributeData(ChunksWrapper.New(
					XMemory.toDirectByteBuffer(new byte[] { 3, 2, 1 }))));
		}
	}

	@Test
	void readerCannotChangeWriterMessageIndex()
	{
		final AeronDistributor distributor = new AeronDistributor(() -> false,
			ignored -> { throw new AssertionError("reader must not synchronize a writer sequence"); });
		assertThrows(IllegalStateException.class, () -> distributor.messageIndex(0L));
		assertEquals(-1L, distributor.messageIndex());
	}

	/** Retention must fail explicitly while authenticated watermarks are absent. */
	@Test
	void retentionRejectsDeletionUntilWatermarksAreConfigured()
	{
		try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(properties("writer")))
		{
			assertThrows(UnsupportedOperationException.class,
				() -> transport.retention().deleteThrough(new ReplicationCursor("aeron", null, -1, new byte[0])));
		}
	}

	/** Verifies reader provider surfaces replay and failure states. */
	@Test
	void readerProviderSurfacesReplayAndFailureStates()
	{
		try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(properties("reader")))
		{
			final TestClient replaying = new TestClient(true, null);
			final ReplicationHealth health = transport.health(() -> true, replaying);
			assertFalse(health.isReady(), "a replaying reader is not ready to serve traffic");
			assertTrue(health.isHealthy());
			assertEquals(ReplicationHealth.State.REPLAYING, health.state());

			final TestClient failed = new TestClient(false, new IllegalStateException("archive unavailable"));
			final ReplicationHealth failedHealth = transport.health(() -> true, failed);
			assertFalse(failedHealth.isReady());
			assertFalse(failedHealth.isHealthy());
			assertEquals(ReplicationHealth.State.FAILED, failedHealth.state());
			assertThrows(ReplicationPositionUnavailableException.class,
				() -> transport.positionProvider("stream").latest(),
				"a reader cannot substitute its applied cursor for the writer's durable boundary");
			health.close();
			failedHealth.close();
		}
	}

	/** Verifies rejection of invalid aeron epoch and stream settings. */
	@Test
	void rejectsInvalidAeronEpochAndStreamSettings()
	{
		assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
			.create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_EPOCH", "-1")));
		assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
			.create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_STREAM_ID", "-1")));
	}

	/** Rejects an invalid Archive free-space admission threshold. */
	@Test
	void rejectsNegativeArchiveCapacityThreshold()
	{
		assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
			.create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_MIN_ARCHIVE_FREE_BYTES", "-1")));
	}

	/** The writer admission gate and health state fail closed when usable space is below the threshold. */
	@Test
	void reportsArchiveCapacityDegradationBeforeAcceptingWrites()
	{
		try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_MIN_ARCHIVE_FREE_BYTES",
				Long.toString(Long.MAX_VALUE))))
		{
			final ClusterStorageBinaryDataClient client = transport.client(null, "stream", null, null, false);
			final ReplicationHealth health = transport.health(() -> true, client);
			health.init();
			assertFalse(health.isReady());
			assertFalse(health.isHealthy());
			assertEquals(ReplicationHealth.State.DEGRADED_ARCHIVE, health.state());
			health.close();
		}
	}

	/** Rejects channel framing overrides that disagree with the shared configuration. */
	@Test
	void rejectsConflictingChannelFraming()
	{
		assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
			.create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
				"aeron:udp?control=localhost:40123|control-mode=dynamic|fc=max|term-length=1m")));
		assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
			.create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL",
				"aeron:udp?endpoint=localhost:0|mtu=1024k")));
	}

	/** Writer topology validation is semantic, not a substring match. */
	@Test
	void rejectsNonDynamicWriterTopology()
	{
		assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
			.create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
				"aeron:udp?control=localhost:40123|control-mode=manual|fc=max")));
		assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
			.create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
				"aeron:udp?control=localhost:40123|control-mode=dynamic|fc=min")));
	}

	/** Verifies rejection of malformed numeric and production temporary directory settings. */
	@Test
	void rejectsMalformedNumericAndProductionTemporaryDirectorySettings()
	{
		assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
			.create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_EPOCH", "not-a-number")));
		final NodelibraryPropertiesProvider production = new NodelibraryPropertiesProvider.Env()
		{
			@Override public String replicationRole() { return "writer"; }
			@Override public boolean replicationRoleConfigured() { return true; }
			@Override public boolean isProdMode() { return true; }
			@Override public String replicationProperty(final String name)
			{
				if ("ECLIPSE_DATAGRID_AERON_CLUSTER_ID".equals(name)) return UUID.randomUUID().toString();
				if ("ECLIPSE_DATAGRID_AERON_DIRECTORY".equals(name)) return "/tmp/datagrid-aeron-test";
				return null;
			}
		};
		assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider().create(production));
	}

	/** Production mode rejects the two common configuration forms that weaken network/durability guarantees. */
	@Test
	void rejectsProductionSyncLevelZeroAndIpv6Wildcard()
	{
		assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
			.create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_FILE_SYNC_LEVEL", "0", true)));
		assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
			.create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
				"aeron:udp?control=[::]:40123|control-mode=dynamic|fc=max", true)));
	}

	private static NodelibraryPropertiesProvider properties(final String role)
	{
		return propertiesWith(role, null, null);
	}

	private static NodelibraryPropertiesProvider propertiesWith(
		final String role, final String overrideName, final String overrideValue)
	{
		return propertiesWith(role, overrideName, overrideValue, false);
	}

	private static NodelibraryPropertiesProvider propertiesWith(
		final String role, final String overrideName, final String overrideValue, final boolean production)
	{
		final String clusterId = UUID.randomUUID().toString();
		final Path root = Paths.get(System.getProperty("java.io.tmpdir"),
			"datagrid-aeron-monitoring-" + UUID.randomUUID());
		return new NodelibraryPropertiesProvider.Env()
		{
			@Override public String replicationRole() { return role; }
			@Override public boolean replicationRoleConfigured() { return true; }
			@Override public boolean isProdMode() { return production; }
			@Override public String replicationProperty(final String name)
			{
				if ("ECLIPSE_DATAGRID_AERON_CLUSTER_ID".equals(name)) return clusterId;
				if ("ECLIPSE_DATAGRID_AERON_NODE_ID".equals(name)) return UUID.randomUUID().toString();
				if ("ECLIPSE_DATAGRID_AERON_STORE_GENERATION".equals(name)) return UUID.randomUUID().toString();
				if ("ECLIPSE_DATAGRID_AERON_DIRECTORY".equals(name)) return root.resolve("driver").toString();
				if ("ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY".equals(name)) return root.resolve("archive").toString();
				if ("ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH".equals(name))
				{
					return root.resolve("checkpoint/writer.checkpoint").toString();
				}
				return overrideName != null && overrideName.equals(name) ? overrideValue : null;
			}
		};
	}

	private record TestClient(boolean isRunning, RuntimeException failure) implements ClusterStorageBinaryDataClient
	{
		@Override public void start() { }
		@Override public void stopAtLatestMessage() { }
		@Override public ReplicationCursor cursor()
		{
			return new ReplicationCursor("aeron", null, -1, new byte[0]);
		}
		@Override public void resume() { }
		@Override public boolean isLive() { return false; }
		@Override public void dispose() { }
	}
}
