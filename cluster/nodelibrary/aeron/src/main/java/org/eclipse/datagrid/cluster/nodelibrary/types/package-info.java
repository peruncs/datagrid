/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Aeron Provider
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
 * Aeron provider for the transport-neutral cluster API.
 *
 * <p>The provider owns the Aeron driver, Archive, publications, and
 * subscriptions created for one cluster node. The neutral cluster API owns
 * the node lifecycle; this package supplies only the Aeron implementation.
 * Select it with {@code ECLIPSE_DATAGRID_REPLICATION_TRANSPORT=aeron} and
 * configure the {@code ECLIPSE_DATAGRID_AERON_*} settings in the deployment
 * configuration. A provider instance belongs to one node and must not be
 * shared between nodes.</p>
 */
package org.eclipse.datagrid.cluster.nodelibrary.types;
