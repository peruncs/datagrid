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


import org.eclipse.datagrid.cluster.nodelibrary.exceptions.HttpResponseException;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterRestRequestController;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations;
import org.eclipse.datagrid.cluster.nodelibrary.types.StorageNodeRestRouteConfigurations.*;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * This controller exposes neutral node operations through Spring MVC.
 *
 * <p>Spring handles routing and asynchronous response wrappers. The neutral
 * request controller owns node state; this class translates its HTTP failures
 * into Spring response exceptions.</p>
 */
@RestController
@RequestMapping(StorageNodeRestRouteConfigurations.ROOT_PATH)
public class SpringBootClusterController
{
	private final ClusterRestRequestController controller;

	/**
	 * Creates an HTTP controller backed by the neutral request controller.
	 *
	 * @param controller neutral cluster request controller
	 */
	public SpringBootClusterController(final ClusterRestRequestController controller)
	{
		this.controller = controller;
	}

	/**
	 * Returns whether the distributor is active.
	 *
	 * @return distributor state
	 * @throws HttpResponseException if the request cannot be served
	 */
	@GetMapping(value = GetDistributor.PATH, produces = GetDistributor.PRODUCES)
	public boolean getDistributor() throws HttpResponseException
	{
		return this.call(this.controller::getDistributor);
	}

	/**
	 * Starts distributor activation.
	 *
	 * @throws HttpResponseException if activation cannot start
	 */
	@PostMapping(
		value = PostActivateDistributorStart.PATH,
		consumes = PostActivateDistributorStart.CONSUMES,
		produces = PostActivateDistributorStart.PRODUCES
	)
	public void postActivateDistributorStart() throws HttpResponseException
	{
		this.call(this.controller::postActivateDistributorStart);
	}

	/**
	 * Finishes distributor activation.
	 *
	 * @return whether activation finished successfully
	 * @throws HttpResponseException if activation cannot finish
	 */
	@PostMapping(
		value = PostActivateDistributorFinish.PATH,
		consumes = PostActivateDistributorFinish.CONSUMES,
		produces = PostActivateDistributorFinish.PRODUCES
	)
	public boolean postActivateDistributorFinish() throws HttpResponseException
	{
		return this.call(this.controller::postActivateDistributorFinish);
	}

	/**
	 * Checks node health.
	 *
	 * @throws HttpResponseException if the health check fails
	 */
	@GetMapping(value = GetHealth.PATH, produces = GetHealth.PRODUCES)
	public void getHealth() throws HttpResponseException
	{
		this.call(this.controller::getHealth);
	}

	/**
	 * Checks whether the node is ready to serve traffic.
	 *
	 * @return completed asynchronous response
	 * @throws HttpResponseException if the readiness check fails
	 */
	@GetMapping(value = GetHealthReady.PATH, produces = GetHealthReady.PRODUCES)
	@Async
	public CompletableFuture<Void> getHealthReady() throws HttpResponseException
	{
		this.call(this.controller::getHealthReady);
		return CompletableFuture.completedFuture(null);
	}

	/**
	 * Returns the storage usage report.
	 *
	 * @return asynchronous storage usage report
	 * @throws HttpResponseException if the report cannot be read
	 */
	@GetMapping(value = GetStorageBytes.PATH, produces = GetStorageBytes.PRODUCES)
	@Async
	public CompletableFuture<String> getStorageBytes() throws HttpResponseException
	{
		return CompletableFuture.completedFuture(this.call(this.controller::getStorageBytes));
	}

	/**
	 * Returns the replication metrics report.
	 *
	 * @return asynchronous replication metrics report
	 * @throws HttpResponseException if the report cannot be read
	 */
	@GetMapping(value = GetReplicationMetrics.PATH, produces = GetReplicationMetrics.PRODUCES)
	@Async
	public CompletableFuture<String> getReplicationMetrics() throws HttpResponseException
	{
		return CompletableFuture.completedFuture(this.call(this.controller::getReplicationMetrics));
	}

	/**
	 * Starts the backup operation described by the request body.
	 *
	 * @param body backup request
	 * @throws HttpResponseException if the backup cannot start
	 */
	@PostMapping(
		value = PostBackup.PATH,
		consumes = PostBackup.CONSUMES,
		produces = PostBackup.PRODUCES
	)
	public void postBackup(@RequestBody final PostBackup.Body body) throws HttpResponseException
	{
		this.call(() -> this.controller.postBackup(body));
	}

	/**
	 * Pauses update delivery.
	 *
	 * @throws HttpResponseException if updates cannot be paused
	 */
	@PostMapping(
		value = PostUpdates.PATH,
		consumes = PostUpdates.CONSUMES,
		produces = PostUpdates.PRODUCES
	)
	public void postUpdates() throws HttpResponseException
	{
		this.call(this.controller::postUpdates);
	}

	/**
	 * Returns whether backup is active.
	 *
	 * @return backup state
	 * @throws HttpResponseException if the state cannot be read
	 */
	@GetMapping(value = GetBackup.PATH, produces = GetBackup.PRODUCES)
	public boolean getBackup() throws HttpResponseException
	{
		return this.call(this.controller::getBackup);
	}

	/**
	 * Returns whether update delivery is active.
	 *
	 * @return update-delivery state
	 * @throws HttpResponseException if the state cannot be read
	 */
	@GetMapping(value = GetUpdates.PATH, produces = GetUpdates.PRODUCES)
	public boolean getUpdates() throws HttpResponseException
	{
		return this.call(this.controller::getUpdates);
	}

	/**
	 * Resumes update delivery.
	 *
	 * @throws HttpResponseException if updates cannot be resumed
	 */
	@PostMapping(
		value = PostResumeUpdates.PATH,
		consumes = PostResumeUpdates.CONSUMES,
		produces = PostResumeUpdates.PRODUCES
	)
	public void postResumeUpdates() throws HttpResponseException
	{
		this.call(this.controller::postResumeUpdates);
	}

	/**
	 * Starts garbage collection.
	 *
	 * @throws HttpResponseException if garbage collection cannot start
	 */
	@PostMapping(
		value = PostGc.PATH,
		consumes = PostGc.CONSUMES,
		produces = PostGc.PRODUCES
	)
	public void postGc() throws HttpResponseException
	{
		this.call(this.controller::postGc);
	}

	/**
	 * Returns whether garbage collection is active.
	 *
	 * @return garbage-collection state
	 * @throws HttpResponseException if the state cannot be read
	 */
	@GetMapping(value = GetGc.PATH, produces = GetGc.PRODUCES)
	public boolean getGc() throws HttpResponseException
	{
		return this.call(this.controller::getGc);
	}

	private <T> T call(final Supplier<T> s)
	{
		try
		{
			return s.get();
		}
		catch (final HttpResponseException e)
		{
			throw this.createResponseStatusException(e);
		}
	}

	private void call(final Runnable r)
	{
		try
		{
			r.run();
		}
		catch (final HttpResponseException e)
		{
			throw this.createResponseStatusException(e);
		}
	}

	private ResponseStatusException createResponseStatusException(final HttpResponseException e)
	{
		final HttpStatus status = Objects.requireNonNull(
			HttpStatus.resolve(e.statusCode()), "Unsupported HTTP status code: " + e.statusCode());
		final var excp = new ResponseStatusException(status);
		for (final var header : e.extraHeaders())
		{
			excp.getHeaders().add(header.key(), header.value());
		}
		return excp;
	}
}
