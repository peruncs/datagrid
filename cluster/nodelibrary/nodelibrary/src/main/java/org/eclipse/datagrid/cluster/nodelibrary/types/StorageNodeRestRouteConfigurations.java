package org.eclipse.datagrid.cluster.nodelibrary.types;

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

/**
 * This class keeps the node REST paths and media types in one place.
 *
 * <p>Framework adapters use these constants to expose the same route table in
 * Helidon, Micronaut, and Spring Boot. The neutral request controller remains
 * responsible for the behavior behind each route.</p>
 */
public final class StorageNodeRestRouteConfigurations
{
	/** Shared media types used by the node endpoints. */
	public static final class MediaTypes
	{
		private static final String WILDCARD = "*/*";
		private static final String APPLICATION_JSON = "application/json";
		private static final String TEXT_PLAIN = "text/plain";

		private MediaTypes()
		{
		}
	}

	public static final String ROOT_PATH = "/eclipse-datagrid";

	/** Reads whether this node is the distributor. */
	public static final class GetDistributor
	{
		public static final String PATH = "/distributor";
		public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

		private GetDistributor()
		{
		}
	}

	/** Starts the distributor role transition. */
	public static final class PostActivateDistributorStart
	{
		public static final String PATH = "/activate-distributor/start";
		public static final String CONSUMES = MediaTypes.WILDCARD;
		public static final String PRODUCES = MediaTypes.WILDCARD;

		private PostActivateDistributorStart()
		{
		}
	}

	/** Finishes the distributor role transition. */
	public static final class PostActivateDistributorFinish
	{
		public static final String PATH = "/activate-distributor/finish";
		public static final String CONSUMES = MediaTypes.WILDCARD;
		public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

		private PostActivateDistributorFinish()
		{
		}
	}

	/** Reads the node health state. */
	public static final class GetHealth
	{
		public static final String PATH = "/health";
		public static final String PRODUCES = MediaTypes.WILDCARD;

		private GetHealth()
		{
		}
	}

	/** Reads whether the node is ready to serve. */
	public static final class GetHealthReady
	{
		public static final String PATH = "/health/ready";
		public static final String PRODUCES = MediaTypes.WILDCARD;

		private GetHealthReady()
		{
		}
	}

	/** Reads the number of bytes used by storage. */
	public static final class GetStorageBytes
	{
		public static final String PATH = "/storage-bytes";
		public static final String PRODUCES = MediaTypes.TEXT_PLAIN;

		private GetStorageBytes()
		{
		}
	}

	/** Prometheus text endpoint for transport-neutral replication state. */
	/** Reads provider-neutral replication metrics. */
	public static final class GetReplicationMetrics
	{
		public static final String PATH = "/replication-metrics";
		public static final String PRODUCES = MediaTypes.TEXT_PLAIN;

		private GetReplicationMetrics()
		{
		}
	}

	/** Starts a storage backup. */
	public static final class PostBackup
	{
		public static final String PATH = "/backup";
		public static final String CONSUMES = MediaTypes.APPLICATION_JSON;
		public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

		/** Request body that selects the manual backup slot. */
		public static final class Body
		{
			private Boolean useManualSlot;

			public Boolean getUseManualSlot()
			{
				return this.useManualSlot;
			}

			public void setUseManualSlot(final Boolean useManualSlot)
			{
				this.useManualSlot = useManualSlot;
			}
		}

		private PostBackup()
		{
		}
	}

	/** Reads whether a backup is running. */
	public static final class GetBackup
	{
		public static final String PATH = "/backup";
		public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

		private GetBackup()
		{
		}
	}

	/** Stops replication at the latest safe message. */
	public static final class PostUpdates
	{
		public static final String PATH = "/updates";
		public static final String CONSUMES = MediaTypes.WILDCARD;
		public static final String PRODUCES = MediaTypes.WILDCARD;

		private PostUpdates()
		{
		}
	}

	/** Reads whether replication is paused. */
	public static final class GetUpdates
	{
		public static final String PATH = "/updates";
		public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

		private GetUpdates()
		{
		}
	}

	/** Resumes replication after a controlled pause. */
	public static final class PostResumeUpdates
	{
		public static final String PATH = "/resume-updates";
		public static final String CONSUMES = MediaTypes.WILDCARD;
		public static final String PRODUCES = MediaTypes.WILDCARD;

		private PostResumeUpdates()
		{
		}
	}

	/** Starts asynchronous storage checks and cleanup. */
	public static final class PostGc
	{
		public static final String PATH = "/gc";
		public static final String CONSUMES = MediaTypes.WILDCARD;
		public static final String PRODUCES = MediaTypes.WILDCARD;

		private PostGc()
		{
		}
	}

	/** Reads whether storage checks are running. */
	public static final class GetGc
	{
		public static final String PATH = "/gc";
		public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

		private GetGc()
		{
		}
	}

	private StorageNodeRestRouteConfigurations()
	{
	}
}
