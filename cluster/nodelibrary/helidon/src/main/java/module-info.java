/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Helidon
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
 * Helidon MicroProfile adapter for the transport-neutral Data Grid node
 * lifecycle.
 *
 * @since 1.0
 */
module org.eclipse.datagrid.cluster.nodelibrary.helidon
{
	requires org.eclipse.datagrid.cluster.nodelibrary;
	requires org.eclipse.datagrid.storage.distributed;
	requires org.eclipse.serializer.base;
	requires jakarta.cdi;
	requires jakarta.ws.rs;
	requires microprofile.config.api;
	requires microprofile.openapi.api;

	exports org.eclipse.datagrid.cluster.nodelibrary.helidon;
	opens org.eclipse.datagrid.cluster.nodelibrary.helidon
		to jakarta.cdi;
}
