package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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

/**
 * Service-provider entry point for an optional replication transport.
 *
 * <p>Implementations are registered in {@code META-INF/services} by their own
 * modules. The neutral nodelibrary discovers the provider selected by
 * {@code ECLIPSE_DATAGRID_REPLICATION_TRANSPORT} without linking against a
 * transport SDK.</p>
 */
public interface ClusterReplicationTransportProvider
{
		/** Stable configuration id used to select this provider.
		 * @return provider id
		 */
	String id();

		/** Creates a transport using the application's neutral properties.
		 *
		 * @param properties application properties
		 * @return replication transport
		 */
	ClusterReplicationTransport create(NodelibraryPropertiesProvider properties);
}
