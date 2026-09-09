/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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
 * Aeron transport for Eclipse Data Grid replication.
 *
 * <p>The module adds the transport without changing the neutral storage
 * contracts. Applications choose it by adding this module and configuring the
 * Aeron provider. The exported packages cover configuration, restart state,
 * reading, and writing. The envelope format stays private to this module so a
 * wire-format change does not become an application API.</p>
 *
 * @since 1.0
 */
module org.eclipse.datagrid.storage.distributed.aeron
{
	requires org.eclipse.datagrid.storage.distributed;
	requires org.eclipse.serializer.base;
	requires org.eclipse.serializer.persistence;
	requires org.eclipse.serializer.persistence.binary;
	requires org.agrona;
	requires io.aeron.client;
	requires io.aeron.archive;

	exports org.eclipse.datagrid.storage.distributed.aeron.config;
	exports org.eclipse.datagrid.storage.distributed.aeron.checkpoint;
	exports org.eclipse.datagrid.storage.distributed.aeron.reader;
	exports org.eclipse.datagrid.storage.distributed.aeron.writer;
}
