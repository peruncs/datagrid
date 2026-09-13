/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Aeron Provider
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */
/**
 * This package carries cluster replication through Aeron.
 *
 * <p>The provider delegates driver and Archive ownership, validated settings,
 * capacity admission, health, reader watermarks, retention, and durable
 * positions to package-private lifecycle components. The neutral cluster API
 * starts and stops the node; this package supplies the transport work between
 * those calls. A provider instance belongs to one node and must not be shared
 * between nodes.</p>
 *
 * <p>Choose the provider with
 * {@code ECLIPSE_DATAGRID_REPLICATION_TRANSPORT=aeron}. Keep the Aeron
 * settings consistent for every member that shares a stream.</p>
 */
package org.eclipse.datagrid.cluster.nodelibrary.aeron;
