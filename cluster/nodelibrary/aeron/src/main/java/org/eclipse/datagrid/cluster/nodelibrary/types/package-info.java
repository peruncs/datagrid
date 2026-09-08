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
 * Aeron implementation of the transport-neutral nodelibrary SPI.
 *
 * <p>The provider owns Aeron media-driver and Archive lifecycle while the
 * neutral cluster package exposes only cursors, health, and lifecycle types.
 * Configure it with {@code ECLIPSE_DATAGRID_REPLICATION_TRANSPORT=aeron} and
 * the {@code ECLIPSE_DATAGRID_AERON_*} properties documented by the module
 * README. Provider instances are not shared between cluster nodes.</p>
 */
package org.eclipse.datagrid.cluster.nodelibrary.types;
