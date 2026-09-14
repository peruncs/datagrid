package org.eclipse.datagrid.cluster.nodelibrary.springboot;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Spring Boot
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


import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterFoundation;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRequestController;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterStorageManager;
import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;
import org.eclipse.serializer.concurrency.LockedExecutor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.concurrent.CompletableFuture;

/**
 * This configuration assembles the cluster services in Spring Boot.
 *
 * <p>Spring owns the beans and their shutdown order. The foundation creates the
 * neutral request controller and cluster-aware storage manager, while the
 * object graph handler applies updates under one lock.</p>
 */
@Configuration
@Import(SpringBootClusterController.class)
public class EclipseDataGridCluster
{
	/** Creates the Spring configuration. */
	public EclipseDataGridCluster()
	{
	}

	/**
	 * Creates the graph update handler used by the cluster services.
	 *
	 * @param executor shared locked executor
	 * @return graph update handler
	 */
	@Bean
	public ObjectGraphUpdateHandler objectGraphUpdateHandler(final LockedExecutor executor)
	{
		return updater ->
		{
			executor.write(updater::updateObjectGraph);
			return CompletableFuture.completedFuture(null);
		};
	}

	/**
	 * Creates the lock used to serialize graph updates.
	 *
	 * @return new locked executor
	 */
	@Bean
	public LockedExecutor lockedExecutor()
	{
		return LockedExecutor.New();
	}

	/**
	 * Creates the cluster foundation.
	 *
	 * @param rootProvider application root provider
	 * @param objectGraphUpdateHandler graph update handler
	 * @param async whether distribution may use asynchronous delivery
	 * @return configured cluster foundation
	 */
	@Bean
	public ClusterFoundation<?> clusterFoundation(
		final RootProvider<?> rootProvider,
		final ObjectGraphUpdateHandler objectGraphUpdateHandler,
		@Value("${eclipsestore.distribution.async:false}") final boolean async
	)
	{
		return ClusterFoundation.New()
			.setEnableAsyncDistribution(async)
			.setObjectGraphUpdateHandler(objectGraphUpdateHandler)
			.setRootSupplier(rootProvider::root);
	}

	/**
	 * Starts the neutral request controller.
	 *
	 * @param foundation cluster foundation
	 * @return started request controller
	 */
	@Bean
	public ClusterRestRequestController nodelibraryClusterController(final ClusterFoundation<?> foundation)
	{
		return foundation.startController();
	}

	/**
	 * Starts the cluster-aware storage manager.
	 *
	 * @param foundation cluster foundation
	 * @return started cluster storage manager
	 */
	@Bean
	public ClusterStorageManager<?> clusterStorageManager(final ClusterFoundation<?> foundation)
	{
		return foundation.startStorageManager();
	}

}
