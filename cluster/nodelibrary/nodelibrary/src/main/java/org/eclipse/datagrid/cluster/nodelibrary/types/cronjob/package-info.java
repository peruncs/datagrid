/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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
 * This package turns Quartz schedules into node maintenance work.
 *
 * <p>The factory maps Quartz job classes to suppliers. The scheduler installs
 * that factory before it starts, and jobs are registered before schedules can
 * fire. Backup and maintenance jobs must not overlap when they use the same
 * storage, so their Quartz definitions prevent concurrent runs.</p>
 */
package org.eclipse.datagrid.cluster.nodelibrary.types.cronjob;
