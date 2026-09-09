package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Aeron Provider
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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies provider health reflects writer readiness and checkpoint state. */
class AeronReplicationMonitoringTest
{
	/** Verifies writer provider exposes aeron and reports live without reader client. */
	@Test
	void writerProviderExposesAeronAndReportsLiveWithoutReaderClient()
	{
		try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(properties("writer")))
		{
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
			final ReplicationCursor cursor = transport.positionProvider("stream").latest();
			assertEquals("aeron", cursor.transport());
			assertEquals(Long.BYTES * 2, cursor.providerPosition().length);
		}
	}

	/** Verifies reader provider surfaces replay and failure states. */
	@Test
	void readerProviderSurfacesReplayAndFailureStates()
	{
		try (final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(properties("reader")))
		{
			final TestClient replaying = new TestClient(true, false, null);
			final ReplicationHealth health = transport.health(() -> true, replaying);
			assertTrue(health.isReady());
			assertTrue(health.isHealthy());
			assertEquals(ReplicationHealth.State.REPLAYING, health.state());

			final TestClient failed = new TestClient(false, false, new IllegalStateException("archive unavailable"));
			final ReplicationHealth failedHealth = transport.health(() -> true, failed);
			assertFalse(failedHealth.isReady());
			assertFalse(failedHealth.isHealthy());
			assertEquals(ReplicationHealth.State.FAILED, failedHealth.state());
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

	private static NodelibraryPropertiesProvider properties(final String role)
	{
		return propertiesWith(role, null, null);
	}

	private static NodelibraryPropertiesProvider propertiesWith(
		final String role, final String overrideName, final String overrideValue)
	{
		final String clusterId = UUID.randomUUID().toString();
		return new NodelibraryPropertiesProvider.Env()
		{
			@Override public String replicationRole() { return role; }
			@Override public boolean replicationRoleConfigured() { return true; }
			@Override public String replicationProperty(final String name)
			{
				if ("ECLIPSE_DATAGRID_AERON_CLUSTER_ID".equals(name)) return clusterId;
				if ("ECLIPSE_DATAGRID_AERON_NODE_ID".equals(name)) return UUID.randomUUID().toString();
				if ("ECLIPSE_DATAGRID_AERON_STORE_GENERATION".equals(name)) return UUID.randomUUID().toString();
				return overrideName != null && overrideName.equals(name) ? overrideValue : null;
			}
		};
	}

    private record TestClient(boolean isRunning, boolean isLive, RuntimeException failure) implements ClusterStorageBinaryDataClient {

        @Override
        public void start() {
        }

        @Override
        public void stopAtLatestMessage() {
        }

        @Override
        public MessageInfo messageInfo() {
            return MessageInfo.New(-1, "aeron", null, new byte[0]);
        }


        @Override
        public void resume() throws NodelibraryException {
        }

        @Override
        public void dispose() {
        }
    }
}
