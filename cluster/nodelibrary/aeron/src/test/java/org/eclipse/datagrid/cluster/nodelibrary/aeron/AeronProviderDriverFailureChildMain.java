package org.eclipse.datagrid.cluster.nodelibrary.aeron;

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

import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterReplicationTransport;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterStorageBinaryDataClient;
import org.eclipse.datagrid.cluster.nodelibrary.types.NodelibraryPropertiesProvider;
import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationHealth;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

/** Forked production-provider driver-timeout probe. */
public final class AeronProviderDriverFailureChildMain
{
	private AeronProviderDriverFailureChildMain() { }

	public static void main(final String[] ignored) throws Exception
	{
		final Path root = Path.of(System.getProperty("dg.driver.failure.root"));
		Files.createDirectories(root.resolve("control"));
		final ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider()
			.create(properties(root));
		final ClusterStorageBinaryDataClient client = transport.client(null, "store", null, null, false);
		final ReplicationHealth health = transport.health(() -> true, client);
		final var positionProvider = transport.positionProvider("store");
		positionProvider.init();
		Files.writeString(root.resolve("control/connected"), "connected");
		AeronClusterReplicationTransportProvider.stopDriverForTest(transport);
		final long deadline = System.nanoTime() + 5_000_000_000L;
		while (health.isHealthy() && System.nanoTime() < deadline) LockSupport.parkNanos(1_000_000L);
		Files.writeString(root.resolve("control/outcome"), health.isHealthy() ? "NO_FAILURE" : "FAILED");
		try { transport.close(); }
		catch (final RuntimeException ignoredFailure) { }
	}

	private static NodelibraryPropertiesProvider properties(final Path root)
	{
		final UUID cluster = UUID.randomUUID();
		final UUID node = UUID.randomUUID();
		final UUID generation = UUID.randomUUID();
		return new NodelibraryPropertiesProvider.Env()
		{
			@Override public String replicationRole() { return "writer"; }
			@Override public boolean replicationRoleConfigured() { return true; }
			@Override public String replicationProperty(final String name)
			{
				return switch (name)
				{
					case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID" -> cluster.toString();
					case "ECLIPSE_DATAGRID_AERON_NODE_ID" -> node.toString();
					case "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> generation.toString();
					case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> root.resolve("driver").toString();
					case "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY" -> root.resolve("archive").toString();
					case "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH" -> root.resolve("checkpoint/writer").toString();
					case "ECLIPSE_DATAGRID_AERON_DRIVER_TIMEOUT_MILLIS" -> "250";
					default -> null;
				};
			}
		};
	}
}
