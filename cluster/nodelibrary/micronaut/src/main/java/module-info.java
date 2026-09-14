/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Micronaut
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
 * Micronaut adapter for the transport-neutral Data Grid node lifecycle.
 *
 * @since 1.0
 */
module org.eclipse.datagrid.cluster.nodelibrary.micronaut
{
	requires org.eclipse.datagrid.cluster.nodelibrary;
	requires org.eclipse.datagrid.storage.distributed;
	requires org.eclipse.serializer.base;
	requires org.eclipse.store.storage;
	requires org.eclipse.store.storage.embedded.configuration;
	requires jakarta.inject;
	requires io.micronaut.micronaut_context;
	requires io.micronaut.micronaut_core;
	requires io.micronaut.micronaut_http;
	requires io.micronaut.micronaut_inject;
	requires io.micronaut.serde.micronaut_serde_api;
	requires io.micronaut.eclipsestore.micronaut_eclipsestore;

	exports org.eclipse.datagrid.cluster.nodelibrary.micronaut.types;
	opens org.eclipse.datagrid.cluster.nodelibrary.micronaut.types
		to io.micronaut.micronaut_context, io.micronaut.micronaut_core,
		io.micronaut.micronaut_inject;
}
