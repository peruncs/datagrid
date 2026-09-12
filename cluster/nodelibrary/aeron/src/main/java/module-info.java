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
/**
 * This module supplies the Aeron provider for the neutral cluster lifecycle.
 *
 * <p>The neutral nodelibrary module defines the lifecycle and replication
 * contracts. This module owns the Aeron resources that carry those contracts.
 * The storage Aeron module provides the lower-level Store transport. The
 * provider is selected through {@link java.util.ServiceLoader}.</p>
 *
 * <p>The module exports only the provider entry point. Aeron driver, archive,
 * and stream details remain inside the implementation boundary.</p>
 *
 * @since 1.0
 */
module org.eclipse.datagrid.cluster.nodelibrary.aeron
{
	requires org.eclipse.datagrid.cluster.nodelibrary;
	requires org.eclipse.datagrid.storage.distributed;
	requires org.eclipse.datagrid.storage.distributed.aeron;
	requires org.eclipse.serializer.persistence;
	requires org.eclipse.serializer.persistence.binary;
	requires io.aeron.client;
	requires io.aeron.archive;
	requires io.aeron.driver;
	requires org.agrona;

	provides org.eclipse.datagrid.cluster.nodelibrary.types.ClusterReplicationTransportProvider
		with org.eclipse.datagrid.cluster.nodelibrary.aeron.AeronClusterReplicationTransportProvider;

	exports org.eclipse.datagrid.cluster.nodelibrary.aeron;
}
