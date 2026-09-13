package org.eclipse.datagrid.cluster.nodelibrary.helidon;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Helidon
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


import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterFoundation;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRequestController;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterStorageManager;
import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.serializer.concurrency.LockedExecutor;

/**
 * This producer assembles the cluster services in a Helidon application.
 *
 * <p>Each produced service belongs to the application context. The foundation
 * is created before storage and REST controllers, and the shutdown hooks close
 * those services when the process ends.</p>
 */
@ApplicationScoped
public class EclipseDataGridCluster
{
	/** Creates the Helidon application producers. */
	public EclipseDataGridCluster()
	{
	}

	/**
	 * Creates the graph update handler used by the cluster services.
	 *
	 * @param executor shared locked executor
	 * @return graph update handler
	 */
	@ApplicationScoped
	@Produces
	public ObjectGraphUpdateHandler objectGraphUpdateHandler(final LockedExecutor executor)
	{
		return updater ->
		{
			executor.write(updater::updateObjectGraph);
			return java.util.concurrent.CompletableFuture.completedFuture(null);
		};
	}

	/**
	 * Creates the lock used to serialize graph updates.
	 *
	 * @return new locked executor
	 */
	@ApplicationScoped
	@Produces
	public LockedExecutor lockedExecutor()
	{
		return LockedExecutor.New();
	}

	/**
	 * Starts the cluster-aware storage manager and registers its shutdown hook.
	 *
	 * @param foundation cluster foundation
	 * @return started cluster storage manager
	 */
	@SuppressWarnings("rawtypes")
	@ApplicationScoped
	@Produces
	public ClusterStorageManager clusterStorageManager(final ClusterFoundation foundation)
	{
		final var manager = foundation.startStorageManager();
		Runtime.getRuntime().addShutdownHook(new Thread(manager::close, "ShutdownCluster"));
		return manager;
	}

	/**
	 * Starts the neutral request controller and registers its shutdown hook.
	 *
	 * @param foundation cluster foundation
	 * @return started request controller
	 */
	@SuppressWarnings("rawtypes")
	@ApplicationScoped
	@Produces
	public ClusterRestRequestController clusterRequestController(final ClusterFoundation foundation)
	{
		final var controller = foundation.startController();
		Runtime.getRuntime().addShutdownHook(new Thread(controller::close, "ShutdownController"));
		return controller;
	}

	/**
	 * Creates the cluster foundation.
	 *
	 * @param rootProvider application root provider
	 * @param objectGraphUpdateHandler graph update handler
	 * @param async whether distribution may use asynchronous delivery
	 * @return configured cluster foundation
	 */
	@SuppressWarnings("rawtypes")
	@ApplicationScoped
	@Produces
	public ClusterFoundation clusterFoundation(
		final RootProvider rootProvider,
		final ObjectGraphUpdateHandler objectGraphUpdateHandler,
		@ConfigProperty(name = "eclipsestore.distribution.kafka.async", defaultValue = "false") final boolean async
	)
	{
		return ClusterFoundation.New()
			.setEnableAsyncDistribution(async)
			.setObjectGraphUpdateHandler(objectGraphUpdateHandler)
			.setRootSupplier(rootProvider::root);
	}
}
