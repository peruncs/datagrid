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

class AeronReplicationMonitoringTest
{
	@Test
	void writerProviderExposesAeronAndReportsLiveWithoutReaderClient()
	{
		final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(properties("writer"));
		final ClusterStorageBinaryDataClient client = transport.client(null, "stream", null, null, false);
		final ReplicationHealth health = transport.health(() -> true, client);
		health.init();
		assertEquals("aeron", transport.id());
		assertTrue(health.isReady());
		assertTrue(health.isHealthy());
		assertEquals(ReplicationHealth.State.LIVE, health.state());
		health.close();
		transport.close();
	}

	@Test
	void positionProviderUsesSelfDescribingRecordingPosition()
	{
		final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(properties("writer"));
		final ReplicationCursor cursor = transport.positionProvider("stream").latest();
		assertEquals("aeron", cursor.transport());
		assertEquals(Long.BYTES * 2, cursor.providerPosition().length);
		transport.close();
	}

	@Test
	void readerProviderSurfacesReplayAndFailureStates()
	{
		final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(properties("reader"));
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
		transport.close();
	}

	@Test
	void rejectsInvalidAeronEpochAndStreamSettings()
	{
		assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
			.create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_EPOCH", "-1")));
		assertThrows(IllegalArgumentException.class, () -> new AeronClusterReplicationTransportProvider()
			.create(propertiesWith("writer", "ECLIPSE_DATAGRID_AERON_STREAM_ID", "-1")));
	}

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

	private static final class TestClient implements ClusterStorageBinaryDataClient
	{
		private final boolean running;
		private final boolean live;
		private final RuntimeException failure;

		private TestClient(final boolean running, final boolean live, final RuntimeException failure)
		{
			this.running = running;
			this.live = live;
			this.failure = failure;
		}

		@Override public void start() { }
		@Override public void stopAtLatestMessage() { }
		@Override public MessageInfo messageInfo() { return MessageInfo.New(-1, "aeron", null, new byte[0]); }
		@Override public boolean isRunning() { return this.running; }
		@Override public boolean isLive() { return this.live; }
		@Override public RuntimeException failure() { return this.failure; }
		@Override public void resume() throws NodelibraryException { }
		@Override public void dispose() { }
	}
}
