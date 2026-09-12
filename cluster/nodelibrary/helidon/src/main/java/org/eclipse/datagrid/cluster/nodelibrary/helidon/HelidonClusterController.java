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
import jakarta.ws.rs.*;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.HttpResponseException;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRequestController;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.*;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;

/**
 * This controller exposes the neutral node operations through Helidon REST.
 *
 * <p>It keeps transport annotations at the framework edge and delegates all
 * state changes to {@link ClusterRestRequestController}. The neutral
 * controller remains the single owner of node rules and error decisions.</p>
 */
@ApplicationScoped
@Path(StorageNodeRestRouteConfigurations.ROOT_PATH)
public class HelidonClusterController
{
	private final ClusterRestRequestController requestController;

	/**
	 * Creates an HTTP controller backed by the neutral request controller.
	 *
	 * @param requestController neutral cluster request controller
	 */
	public HelidonClusterController(final ClusterRestRequestController requestController)
	{
		this.requestController = requestController;
	}

	/**
	 * Returns whether the distributor is active.
	 *
	 * @return distributor state
	 * @throws HttpResponseException if the request cannot be served
	 */
	@GET
	@Path(GetDistributor.PATH)
	@Produces(GetDistributor.PRODUCES)
	public boolean getDistributor() throws HttpResponseException
	{
		return this.requestController.getDistributor();
	}

	/**
	 * Starts distributor activation.
	 *
	 * @throws HttpResponseException if activation cannot start
	 */
	@POST
	@Path(PostActivateDistributorStart.PATH)
	@Consumes(PostActivateDistributorStart.CONSUMES)
	@Produces(PostActivateDistributorStart.PRODUCES)
	public void postActivateDistributorStart() throws HttpResponseException
	{
		this.requestController.postActivateDistributorStart();
	}

	/**
	 * Finishes distributor activation.
	 *
	 * @return whether activation finished successfully
	 * @throws HttpResponseException if activation cannot finish
	 */
	@POST
	@Path(PostActivateDistributorFinish.PATH)
	@Consumes(PostActivateDistributorFinish.CONSUMES)
	@Produces(PostActivateDistributorFinish.PRODUCES)
	public boolean postActivateDistributorFinish() throws HttpResponseException
	{
		return this.requestController.postActivateDistributorFinish();
	}

	/**
	 * Checks node health.
	 *
	 * @throws HttpResponseException if the health check fails
	 */
	@GET
	@Path(GetHealth.PATH)
	@Produces(GetHealth.PRODUCES)
	public void getHealth() throws HttpResponseException
	{
		this.requestController.getHealth();
	}

	/**
	 * Checks whether the node is ready to serve traffic.
	 *
	 * @throws HttpResponseException if the readiness check fails
	 */
	@GET
	@Path(GetHealthReady.PATH)
	@Produces(GetHealthReady.PRODUCES)
	public void getHealthReady() throws HttpResponseException
	{
		this.requestController.getHealthReady();
	}

	/**
	 * Returns the storage usage report.
	 *
	 * @return storage usage report
	 * @throws HttpResponseException if the report cannot be read
	 */
	@GET
	@Path(GetStorageBytes.PATH)
	@Produces(GetStorageBytes.PRODUCES)
	public String getStorageBytes() throws HttpResponseException
	{
		return this.requestController.getStorageBytes();
	}

	/**
	 * Returns the replication metrics report.
	 *
	 * @return replication metrics report
	 * @throws HttpResponseException if the report cannot be read
	 */
	@GET
	@Path(GetReplicationMetrics.PATH)
	@Produces(GetReplicationMetrics.PRODUCES)
	public String getReplicationMetrics() throws HttpResponseException
	{
		return this.requestController.getReplicationMetrics();
	}

	/**
	 * Starts the backup operation described by the request body.
	 *
	 * @param body backup request
	 * @throws HttpResponseException if the backup cannot start
	 */
	@POST
	@Path(PostBackup.PATH)
	@Consumes(PostBackup.CONSUMES)
	@Produces(PostBackup.PRODUCES)
	public void postBackup(@RequestBody final PostBackup.Body body) throws HttpResponseException
	{
		this.requestController.postBackup(body);
	}

	/**
	 * Returns whether backup is active.
	 *
	 * @return backup state
	 * @throws HttpResponseException if the state cannot be read
	 */
	@GET
	@Path(GetBackup.PATH)
	@Produces(GetBackup.PRODUCES)
	public boolean getBackup() throws HttpResponseException
	{
		return this.requestController.getBackup();
	}

	/**
	 * Pauses update delivery.
	 *
	 * @throws HttpResponseException if updates cannot be paused
	 */
	@POST
	@Path(PostUpdates.PATH)
	@Consumes(PostUpdates.CONSUMES)
	@Produces(PostUpdates.PRODUCES)
	public void postUpdates() throws HttpResponseException
	{
		this.requestController.postUpdates();
	}

	/**
	 * Returns whether update delivery is active.
	 *
	 * @return update-delivery state
	 * @throws HttpResponseException if the state cannot be read
	 */
	@GET
	@Path(GetUpdates.PATH)
	@Produces(GetUpdates.PRODUCES)
	public boolean getUpdates() throws HttpResponseException
	{
		return this.requestController.getUpdates();
	}

	/**
	 * Resumes update delivery.
	 *
	 * @throws HttpResponseException if updates cannot be resumed
	 */
	@POST
	@Path(PostResumeUpdates.PATH)
	@Consumes(PostResumeUpdates.CONSUMES)
	@Produces(PostResumeUpdates.PRODUCES)
	public void postResumeUpdates() throws HttpResponseException
	{
		this.requestController.postResumeUpdates();
	}

	/**
	 * Starts garbage collection.
	 *
	 * @throws HttpResponseException if garbage collection cannot start
	 */
	@POST
	@Path(PostGc.PATH)
	@Consumes(PostGc.CONSUMES)
	@Produces(PostGc.PRODUCES)
	public void postGc() throws HttpResponseException
	{
		this.requestController.postGc();
	}

	/**
	 * Returns whether garbage collection is active.
	 *
	 * @return garbage-collection state
	 * @throws HttpResponseException if the state cannot be read
	 */
	@GET
	@Path(GetGc.PATH)
	@Produces(GetGc.PRODUCES)
	public boolean getGc() throws HttpResponseException
	{
		return this.requestController.getGc();
	}
}
