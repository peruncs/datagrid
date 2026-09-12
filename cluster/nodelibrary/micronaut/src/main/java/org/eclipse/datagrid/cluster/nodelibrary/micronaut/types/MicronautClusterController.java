package org.eclipse.datagrid.cluster.nodelibrary.micronaut.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Micronaut
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


import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.serde.annotation.SerdeImport;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.HttpResponseException;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRequestController;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.*;

/**
 * This controller exposes neutral node operations through Micronaut HTTP.
 *
 * <p>Micronaut handles routing, serialization, and worker selection. The
 * neutral request controller owns the node state and turns failures into
 * {@link HttpResponseException} instances.</p>
 */
@Controller(StorageNodeRestRouteConfigurations.ROOT_PATH)
@Introspected(classes = PostBackup.Body.class)
@SerdeImport(PostBackup.Body.class)
public class MicronautClusterController
{
	private final ClusterRestRequestController controller;

	/**
	 * Creates an HTTP controller backed by the neutral request controller.
	 *
	 * @param controller neutral cluster request controller
	 */
	public MicronautClusterController(final ClusterRestRequestController controller)
	{
		this.controller = controller;
	}

	/**
	 * Converts a neutral request failure into an HTTP response.
	 *
	 * @param e request failure
	 * @return response with the failure status and headers
	 */
	@io.micronaut.http.annotation.Error
	public HttpResponse<Void> handleNodelibraryException(final HttpResponseException e)
	{
		final MutableHttpResponse<Void> response = HttpResponse.status(HttpStatus.valueOf(e.statusCode()));
		for (final var header : e.extraHeaders())
		{
			response.header(header.key(), header.value());
		}
		return response;
	}

	/**
	 * Returns whether the distributor is active.
	 *
	 * @return distributor state
	 * @throws HttpResponseException if the request cannot be served
	 */
	@Get(value = GetDistributor.PATH, produces = GetDistributor.PRODUCES)
	public boolean getDistributor() throws HttpResponseException
	{
		return this.controller.getDistributor();
	}

	/**
	 * Starts distributor activation.
	 *
	 * @throws HttpResponseException if activation cannot start
	 */
	@Post(
		value = PostActivateDistributorStart.PATH,
		consumes = PostActivateDistributorStart.CONSUMES,
		produces = PostActivateDistributorStart.PRODUCES
	)
	public void postActivateDistributorStart() throws HttpResponseException
	{
		this.controller.postActivateDistributorStart();
	}

	/**
	 * Finishes distributor activation.
	 *
	 * @return whether activation finished successfully
	 * @throws HttpResponseException if activation cannot finish
	 */
	@Post(
		value = PostActivateDistributorFinish.PATH,
		consumes = PostActivateDistributorFinish.CONSUMES,
		produces = PostActivateDistributorFinish.PRODUCES
	)
	public boolean postActivateDistributorFinish() throws HttpResponseException
	{
		return this.controller.postActivateDistributorFinish();
	}

	/**
	 * Checks the node health.
	 *
	 * @throws HttpResponseException if the health check fails
	 */
	@Get(value = GetHealth.PATH, produces = GetHealth.PRODUCES)
	public void getHealth() throws HttpResponseException
	{
		this.controller.getHealth();
	}

	/**
	 * Checks whether the node is ready to serve traffic.
	 *
	 * @throws HttpResponseException if the readiness check fails
	 */
	@Get(value = GetHealthReady.PATH, produces = GetHealthReady.PRODUCES)
	@ExecuteOn(TaskExecutors.IO)
	public void getHealthReady() throws HttpResponseException
	{
		this.controller.getHealthReady();
	}

	/**
	 * Returns the storage usage report.
	 *
	 * @return storage usage report
	 * @throws HttpResponseException if the report cannot be read
	 */
	@Get(value = GetStorageBytes.PATH, produces = GetStorageBytes.PRODUCES)
	@ExecuteOn(TaskExecutors.IO)
	public String getStorageBytes() throws HttpResponseException
	{
		return this.controller.getStorageBytes();
	}

	/**
	 * Returns the replication metrics report.
	 *
	 * @return replication metrics report
	 * @throws HttpResponseException if the report cannot be read
	 */
	@Get(value = GetReplicationMetrics.PATH, produces = GetReplicationMetrics.PRODUCES)
	@ExecuteOn(TaskExecutors.IO)
	public String getReplicationMetrics() throws HttpResponseException
	{
		return this.controller.getReplicationMetrics();
	}

	/**
	 * Starts the backup operation described by the request body.
	 *
	 * @param body backup request
	 * @throws HttpResponseException if the backup cannot start
	 */
	@Post(
		value = PostBackup.PATH,
		consumes = PostBackup.CONSUMES,
		produces = PostBackup.PRODUCES
	)
	public void postBackup(@Body final PostBackup.Body body) throws HttpResponseException
	{
		this.controller.postBackup(body);
	}

	/**
	 * Returns whether backup is active.
	 *
	 * @return backup state
	 * @throws HttpResponseException if the state cannot be read
	 */
	@Get(value = GetBackup.PATH, produces = GetBackup.PRODUCES)
	public boolean getBackup() throws HttpResponseException
	{
		return this.controller.getBackup();
	}

	/**
	 * Pauses update delivery.
	 *
	 * @throws HttpResponseException if updates cannot be paused
	 */
	@Post(
		value = PostUpdates.PATH,
		consumes = PostUpdates.CONSUMES,
		produces = PostUpdates.PRODUCES
	)
	public void postUpdates() throws HttpResponseException
	{
		this.controller.postUpdates();
	}

	/**
	 * Returns whether update delivery is active.
	 *
	 * @return update-delivery state
	 * @throws HttpResponseException if the state cannot be read
	 */
	@Get(value = GetUpdates.PATH, produces = GetUpdates.PRODUCES)
	public boolean getUpdates() throws HttpResponseException
	{
		return this.controller.getUpdates();
	}

	/**
	 * Resumes update delivery.
	 *
	 * @throws HttpResponseException if updates cannot be resumed
	 */
	@Post(
		value = PostResumeUpdates.PATH,
		consumes = PostResumeUpdates.CONSUMES,
		produces = PostResumeUpdates.PRODUCES
	)
	public void postResumeUpdates() throws HttpResponseException
	{
		this.controller.postResumeUpdates();
	}

	/**
	 * Starts garbage collection.
	 *
	 * @throws HttpResponseException if garbage collection cannot start
	 */
	@Post(value = PostGc.PATH, consumes = PostGc.CONSUMES, produces = PostGc.PRODUCES)
	public void postGc() throws HttpResponseException
	{
		this.controller.postGc();
	}

	/**
	 * Returns whether garbage collection is active.
	 *
	 * @return garbage-collection state
	 * @throws HttpResponseException if the state cannot be read
	 */
	@Get(value = GetGc.PATH, produces = GetGc.PRODUCES)
	public boolean getGc() throws HttpResponseException
	{
		return this.controller.getGc();
	}
}
