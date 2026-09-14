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
 * This module carries neutral Store replication data over Aeron.
 *
 * <p>The distributed-storage module defines the data and lifecycle contracts.
 * This module supplies the Aeron configuration, restart state, readers, and
 * writers that implement them. The exported packages are the application
 * boundary; the envelope format stays private so wire changes do not become
 * application API changes.</p>
 *
 * <p>A provider owns the Aeron driver, archive, publications, and subscriptions
 * that it creates. Callers must close the provider after the node has stopped
 * producing or consuming data.</p>
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
	/* Benchmark tests use com.sun.management.ThreadMXBean while production
	 * classes have no runtime dependency on the management implementation. */
	requires static jdk.management;
	exports org.eclipse.datagrid.storage.distributed.aeron.config;
	exports org.eclipse.datagrid.storage.distributed.aeron.checkpoint;
	exports org.eclipse.datagrid.storage.distributed.aeron.reader;
	exports org.eclipse.datagrid.storage.distributed.aeron.writer;
}
