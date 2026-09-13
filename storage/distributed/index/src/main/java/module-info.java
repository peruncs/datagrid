/*-
 * #%L
 * Eclipse Data Grid Store Index Integration
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
 * Defines the Store index policy used by clustered Data Grid applications.
 *
 * <p>Cluster replication carries the Eclipse Store object graph. Lucene data
 * must therefore live in the graph, and vector data must use JVector's
 * persisted in-graph state. Files in an application directory are not part
 * of a Store transaction and are rejected at this boundary.</p>
 *
 * <p>The policy is independent of Kafka and Aeron. Both transports receive
 * the same Store bytes and therefore use the same index rules.</p>
 *
 * @since 1.0
 */
module org.eclipse.datagrid.storage.distributed.index
{
	requires org.eclipse.store.gigamap;
	requires org.eclipse.store.gigamap.lucene;
	requires org.eclipes.store.gigamap.jvector;
	requires org.apache.lucene.core;
	requires org.apache.lucene.queryparser;

	exports org.eclipse.datagrid.storage.distributed.index;
}
