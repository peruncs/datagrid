/*-
 * #%L
 * Eclipse Data Grid Cache Clustered
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
 * This module adds clustered invalidation to the Eclipse Store Hibernate
 * cache region factory.
 *
 * <p>The region factory, message contracts, and serialization type provider
 * are transport-neutral. A concrete transport is selected through the
 * {@code ClusteredCacheMessageComProvider} class named in configuration; the
 * sibling Kafka and Aeron modules supply those implementations.</p>
 *
 * @since 1.0
 */
module org.eclipse.datagrid.cache.clustered
{
	requires org.eclipse.serializer;
	requires org.eclipse.serializer.base;
	requires org.eclipse.store.cache;
	requires org.eclipse.store.cache.hibernate;
	requires org.hibernate.orm.core;
	requires cache.api;

	exports org.eclipse.datagrid.cache.clustered.types;
}
