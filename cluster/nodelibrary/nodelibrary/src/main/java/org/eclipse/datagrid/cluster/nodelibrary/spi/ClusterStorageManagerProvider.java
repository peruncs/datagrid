package org.eclipse.datagrid.cluster.nodelibrary.spi;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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


import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterStorageManager;

/**
 * This SPI supplies the cluster-aware storage manager for one application.
 *
 * <p>The framework calls the provider during node assembly. The application
 * owns the returned manager and closes it after replication and request
 * handling have stopped.</p>
 */
public interface ClusterStorageManagerProvider
{
	ClusterStorageManager<?> provideClusterStorageManager();
}
