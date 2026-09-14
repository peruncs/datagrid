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
 * Kafka transport for the neutral Store replication contracts.
 *
 * <p>This module is the legacy storage-level adapter. New cluster integrations
 * should use the cluster nodelibrary Kafka adapter, while this module remains
 * useful for applications that consume the neutral storage API directly.</p>
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
