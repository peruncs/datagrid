/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Kafka
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
 * This module carries neutral Store replication packets over Kafka.
 *
 * <p>The distributed-storage module owns packet identity, transaction order,
 * and binary ownership. This module owns the Kafka producer and consumer
 * adapters that transport those packets. It does not define cluster lifecycle
 * or cache semantics.</p>
 *
 * <p>Applications configure the Kafka adapter and close it after producers and
 * consumers have stopped. The exported package is the adapter boundary.</p>
 *
 * @since 1.0
 */
module org.eclipse.datagrid.storage.distributed.kafka
{
	requires org.eclipse.datagrid.storage.distributed;
	requires org.eclipse.serializer.base;
	requires org.eclipse.serializer.persistence.binary;
	requires kafka.clients;

	exports org.eclipse.datagrid.storage.distributed.kafka.types;
}
