/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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
 * This module coordinates the lifecycle and replication contracts of a Data
 * Grid node.
 *
 * <p>It defines the provider-neutral APIs for storage, cursors, health,
 * retention, backup, and node-facing adapters. Optional Aeron and Kafka
 * modules implement the transport provider contract and are discovered through
 * {@link java.util.ServiceLoader}. This module does not choose a messaging
 * client.</p>
 *
 * <p>Applications create the node services, start them in dependency order,
 * and close them in reverse order. The exported packages contain the public
 * contracts and errors used at those boundaries.</p>
 *
 * @since 1.0
 */
module org.eclipse.datagrid.cluster.nodelibrary
{
	requires org.eclipse.datagrid.storage.distributed;
	requires org.eclipse.serializer.persistence;
	requires org.eclipse.serializer.persistence.binary;
	requires org.eclipse.store.storage;
	requires org.eclipse.store.storage.embedded;
	requires org.eclipse.serializer.afs;
	requires org.eclipse.store.afs.nio;
	requires org.quartz;
	requires com.fasterxml.jackson.core;
	requires com.fasterxml.jackson.databind;
	requires java.net.http;

	uses org.eclipse.datagrid.cluster.nodelibrary.types.ClusterReplicationTransportProvider;

	exports org.eclipse.datagrid.cluster.nodelibrary.exceptions;
	exports org.eclipse.datagrid.cluster.nodelibrary.spi;
	exports org.eclipse.datagrid.cluster.nodelibrary.types;
}
