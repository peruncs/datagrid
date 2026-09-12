/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Kafka Provider
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
 * This module supplies the Kafka provider for the neutral cluster lifecycle.
 *
 * <p>The neutral nodelibrary module defines the lifecycle and replication
 * contracts. This module owns the Kafka clients that carry those contracts.
 * The provider is selected through {@link java.util.ServiceLoader}.</p>
 *
 * <p>The module exports only the provider entry point. Kafka client setup and
 * message handling stay inside the implementation boundary.</p>
 *
 * @since 1.0
 */
module org.eclipse.datagrid.cluster.nodelibrary.kafka
{
	requires org.eclipse.datagrid.cluster.nodelibrary;
	requires org.eclipse.datagrid.storage.distributed;
	requires kafka.clients;
	requires org.eclipse.serializer.base;
	requires org.eclipse.serializer.persistence.binary;
	requires org.slf4j;

	provides org.eclipse.datagrid.cluster.nodelibrary.types.ClusterReplicationTransportProvider
		with org.eclipse.datagrid.cluster.nodelibrary.kafka.KafkaClusterReplicationTransportProvider;

	exports org.eclipse.datagrid.cluster.nodelibrary.kafka;
}
