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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.cluster.nodelibrary.types.cronjob.*;
import org.eclipse.datagrid.cluster.nodelibrary.types.cronjob.GcWorkaroundQuartzCronJobManager.GcWorkaroundQuartzCronJob;
import org.eclipse.datagrid.cluster.nodelibrary.types.cronjob.StorageBackupQuartzCronJobManager.StorageBackupQuartzCronJob;
import org.eclipse.datagrid.cluster.nodelibrary.types.cronjob.StorageLimitCheckerQuartzCronJobManager.StorageLimitCheckerQuartzCronJob;
import org.eclipse.datagrid.storage.distributed.types.DistributedStorage;
import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;
import org.eclipse.serializer.exceptions.MissingFoundationPartException;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryAssembler;
import org.eclipse.serializer.persistence.types.Unpersistable;
import org.eclipse.serializer.reference.Lazy;
import org.eclipse.serializer.util.InstanceDispatcher;
import org.eclipse.store.afs.nio.types.NioFileSystem;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.exceptions.StorageException;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.eclipse.store.storage.types.StorageExceptionHandler;
import org.eclipse.store.storage.types.StorageLiveFileProvider;
import org.eclipse.store.storage.types.StorageManager;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;

/**
 * This foundation assembles the services that make one cluster node run.
 *
 * <p>Callers set the storage, transport, graph-update, and maintenance parts
 * before starting the storage manager or request controller. Start creates the
 * dependency graph; close releases it in the reverse direction. A foundation
 * belongs to one node and must not be reused after that node is closed.</p>
 */
public interface ClusterFoundation<F extends ClusterFoundation<?>> extends InstanceDispatcher
{
	StorageBackupBackend getStorageBackupBackend();

	F setStorageBackupBackend(StorageBackupBackend backend);

	StorageTaskExecutor getStorageTaskExecutor();

	F setStorageTaskExecutor(StorageTaskExecutor executor);

	StorageBackupTaskExecutor getStorageBackupTaskExecutor();

	F setStorageBackupTaskExecutor(StorageBackupTaskExecutor executor);

	BackupProxyHttpClient getBackupProxyHttpClient();

	F setBackupProxyHttpClient(BackupProxyHttpClient client);

	QuartzCronJobScheduler getQuartzCronJobScheduler();

	F setQuartzCronJobScheduler(QuartzCronJobScheduler scheduler);

	QuartzCronJobJobFactory getQuartzCronJobJobFactory();

	F setQuartzCronJobJobFactory(QuartzCronJobJobFactory factory);

	StorageBackupQuartzCronJobManager getStorageBackupQuartzCronJobManager();

	F setStorageBackupQuartzCronJobManager(StorageBackupQuartzCronJobManager manager);

	StorageLimitCheckerQuartzCronJobManager getStorageLimitCheckerQuartzCronJobManager();

	F setStorageLimitCheckerQuartzCronJobManager(StorageLimitCheckerQuartzCronJobManager manager);

	GcWorkaroundQuartzCronJobManager getGcWorkaroundQuartzCronJobManager();

	F setGcWorkaroundQuartzCronJobManager(GcWorkaroundQuartzCronJobManager manager);

	ClusterReplicationTransport getClusterReplicationTransport();

	F setClusterReplicationTransport(ClusterReplicationTransport transport);

	ClusterStorageBinaryDataPacketAcceptor getClusterStorageBinaryDataPacketAcceptor();

	F setClusterStorageBinaryDataPacketAcceptor(ClusterStorageBinaryDataPacketAcceptor acceptor);

	ClusterStorageBinaryDataMerger getClusterStorageBinaryDataMerger();

	F setClusterStorageBinaryDataMerger(ClusterStorageBinaryDataMerger merger);

	AfterDataMessageConsumedListener getAfterDataMessageConsumedListener();

	F setAfterDataMessageConsumedListener(AfterDataMessageConsumedListener listener);

	StoredMessageInfoManager getStoredMessageInfoManager();

	F setStoredMessageInfoManager(StoredMessageInfoManager manager);

	StorageBackupManager getStorageBackupManager();

	F setStorageBackupManager(StorageBackupManager manager);

	Supplier<Object> getRootSupplier();

	F setRootSupplier(Supplier<Object> supplier);

	ObjectGraphUpdateHandler getObjectGraphUpdateHandler();

	F setObjectGraphUpdateHandler(ObjectGraphUpdateHandler handler);

	EmbeddedStorageFoundation<?> getEmbeddedStorageFoundation();

	F setEmbeddedStorageFoundation(EmbeddedStorageFoundation<?> foundation);

	BackupNodeManager getBackupNodeManager();

	F setBackupNodeManager(BackupNodeManager manager);

	ClusterStorageBinaryDataClient getClusterStorageBinaryDataClient();

	F setClusterStorageBinaryDataClient(ClusterStorageBinaryDataClient client);

	ClusterStorageBinaryDataDistributor getClusterStorageBinaryDataDistributor();

	F setClusterStorageBinaryDataDistributor(ClusterStorageBinaryDataDistributor distributor);

	StorageNodeHealthCheck getStorageNodeHealthCheck();

	F setStorageNodeHealthCheck(StorageNodeHealthCheck check);

	NodelibraryPropertiesProvider getNodelibraryPropertiesProvider();

	F setNodelibraryPropertiesProvider(NodelibraryPropertiesProvider provider);

	StorageDiskSpaceReader getStorageDiskSpaceReader();

	F setStorageDiskSpaceReader(StorageDiskSpaceReader reader);

	StorageNodeManager getStorageNodeManager();

	F setStorageNodeManager(StorageNodeManager manager);

	boolean getEnableAsyncDistribution();

	F setEnableAsyncDistribution(boolean enable);

	ReplicationPositionProvider getReplicationPositionProvider();

	F setReplicationPositionProvider(ReplicationPositionProvider provider);

	ReplicationLogRetention getReplicationLogRetention();

	F setReplicationLogRetention(ReplicationLogRetention retention);

	MessageInfoParser getMessageInfoParser();

	F setMessageInfoParser(MessageInfoParser parser);

	static ClusterFoundation<?> New()
	{
		return new Default<>();
	}

	ClusterRestRequestController startController() throws NodelibraryException;

	ClusterStorageManager<?> startStorageManager() throws NodelibraryException;

	/** Stores the parts and builds the default cluster service graph. */
	class Default<F extends Default<?>> extends InstanceDispatcher.Default
		implements ClusterFoundation<F>, Unpersistable
	{
		private static final Logger LOG = LoggerFactory.getLogger(ClusterFoundation.class);

		private StorageBackupBackend backupBackend;
		private BackupProxyHttpClient backupProxyHttpClient;
		private EmbeddedStorageFoundation<?> embeddedStorageFoundation;
		private QuartzCronJobScheduler cronJobScheduler;
		private QuartzCronJobJobFactory cronJobFactory;
		private StorageBackupQuartzCronJobManager backupCronjobManager;
		private GcWorkaroundQuartzCronJobManager gcWorkaroundManager;
		private StorageLimitCheckerQuartzCronJobManager limitCheckerManager;
		private BackupNodeManager backupNodeManager;
		private ClusterStorageBinaryDataClient dataClient;
		private ClusterStorageBinaryDataDistributor dataDistributor;
		private StorageNodeHealthCheck healthCheck;
		private NodelibraryPropertiesProvider propertiesProvider;
		private StorageTaskExecutor storageTaskExecutor;
		private StorageBackupTaskExecutor storageBackupTaskExecutor;
		private StorageDiskSpaceReader storageDiskSpaceReader;
		private StorageNodeManager storageNodeManager;
		private boolean enableAsyncDistribution;
		private Supplier<Object> rootSupplier;
		private ObjectGraphUpdateHandler graphUpdateHandler;
		private StorageBackupManager storageBackupManager;
		private AfterDataMessageConsumedListener afterDataMessageConsumedListener;
		private ClusterStorageBinaryDataMerger dataMerger;
		private ClusterStorageBinaryDataPacketAcceptor dataPacketAcceptor;
		private StoredMessageInfoManager storedMessageInfoManager;
		private ClusterReplicationTransport replicationTransport;
		private ReplicationPositionProvider positionProvider;
		private ReplicationLogRetention replicationRetention;
		private MessageInfoParser messageInfoParser;

		// cached created types
		private ClusterStorageManager<?> clusterStorageManager;
		private ClusterRestRequestController clusterRequestController;

		private Default()
		{
		}

		@SuppressWarnings("unchecked")
		protected final F $()
		{
			return (F)this;
		}

		protected StorageBackupBackend ensureBackupBackend()
		{
			final var props = this.getNodelibraryPropertiesProvider();
			final StoredMessageInfoManager.Creator messageInfoManagerCreator =
				messageInfoFile -> StoredMessageInfoManager.New(messageInfoFile, this.getMessageInfoParser());

			if (props.backupTarget() == BackupTarget.SAAS)
			{
				final var scratchSpace = this.storageParentPath().resolve("backup");
				if (!Files.exists(scratchSpace))
				{
					try
					{
						Files.createDirectories(scratchSpace);
					}
					catch (final IOException e)
					{
						throw new NodelibraryException("Failed to create scratch space", e);
					}
				}
				return NetworkArchiveBackupBackend.New(
					scratchSpace,
					this.getBackupProxyHttpClient(),
					messageInfoManagerCreator,
					this.getMessageInfoParser()
				);
			}
			else
			{
				return FilesystemVolumeBackupBackend.New(
					Paths.get("/backups"), messageInfoManagerCreator,
					this.getMessageInfoParser()
				);
			}
		}

		protected StorageTaskExecutor ensureStorageTaskExecutor()
		{
			if (this.getNodelibraryPropertiesProvider().isBackupNode())
			{
				return this.getStorageBackupTaskExecutor();
			}
			return StorageTaskExecutor.New(this.clusterStorageManager);
		}

		protected StorageBackupTaskExecutor ensureStorageBackupTaskExecutor()
		{
			return StorageBackupTaskExecutor.New(this.clusterStorageManager, this.getStorageBackupManager());
		}

		protected StorageBackupQuartzCronJobManager ensureStorageBackupQuartzCronJobManager()
		{
			return StorageBackupQuartzCronJobManager.New(this.getStorageBackupManager());
		}

		protected BackupProxyHttpClient ensureBackupProxyHttpClient()
		{
			return BackupProxyHttpClient.New(
				URI.create(this.getNodelibraryPropertiesProvider().backupProxyServiceUrl())
			);
		}

		protected QuartzCronJobJobFactory ensureCronJobFactory()
		{
			return QuartzCronJobJobFactory.New();
		}

		protected QuartzCronJobScheduler ensureCronJobScheduler()
		{
			final Scheduler scheduler;
			try
			{
				scheduler = StdSchedulerFactory.getDefaultScheduler();
			}
			catch (final SchedulerException e)
			{
				throw new NodelibraryException("Failed to get the default quartz cron job scheduler", e);
			}
			return QuartzCronJobScheduler.New(scheduler);
		}

		protected GcWorkaroundQuartzCronJobManager ensureGcWorkaroundManager()
		{
			return GcWorkaroundQuartzCronJobManager.New(this.clusterStorageManager);
		}

		protected StorageLimitCheckerQuartzCronJobManager ensureStorageLimitCheckerManager()
		{
			return StorageLimitCheckerQuartzCronJobManager.New(
				this.getNodelibraryPropertiesProvider().storageLimitGB(),
				this.getStorageDiskSpaceReader()
			);
		}

		protected ClusterReplicationTransport ensureClusterReplicationTransport()
		{
			final String configured = this.getNodelibraryPropertiesProvider().replicationTransport();
			final String requested = configured == null || configured.isBlank() ? "none" : configured.trim();
			for (final ClusterReplicationTransportProvider provider :
				java.util.ServiceLoader.load(ClusterReplicationTransportProvider.class))
			{
				if (provider.id().equalsIgnoreCase(requested))
				{
					return provider.create(this.getNodelibraryPropertiesProvider());
				}
			}
			if (!"none".equalsIgnoreCase(requested))
			{
				throw new NodelibraryException("No replication transport provider installed for " + requested);
			}
			return ClusterReplicationTransport.noOp();
		}

		protected ReplicationPositionProvider ensureReplicationPositionProvider()
		{
			return this.getClusterReplicationTransport().positionProvider(
				this.getNodelibraryPropertiesProvider().replicationStreamName()
			);
		}

		protected ReplicationLogRetention ensureReplicationLogRetention()
		{
			return this.getClusterReplicationTransport().retention();
		}

		protected StoredMessageInfoManager ensureStoredMessageInfoManager()
		{
			final var messageInfoPath = this.storageParentPath().resolve("offset");
			LOG.trace("Creating StoredMessageInfoManager for offset file at {}", messageInfoPath);
			if ("aeron".equalsIgnoreCase(this.getNodelibraryPropertiesProvider().replicationTransport()))
			{
				return StoredMessageInfoManager.NewAtomic(messageInfoPath, this.getMessageInfoParser());
			}
			return StoredMessageInfoManager.New(
				NioFileSystem.New().ensureFile(messageInfoPath).tryUseWriting(), this.getMessageInfoParser()
			);
		}

		/** Returns the configured storage root, defaulting to the historic deployment path. */
		private Path storageParentPath()
		{
			final String configured = this.getNodelibraryPropertiesProvider()
				.replicationProperty(NodelibraryPropertiesProvider.Env.EnvKeys.STORAGE_PATH);
			return Paths.get(configured == null || configured.isBlank() ? "/storage" : configured).normalize();
		}

		protected AfterDataMessageConsumedListener ensureAfterDataMessageConsumedListener()
		{
			final var props = this.getNodelibraryPropertiesProvider();

			final var storedMessageInfoUpdater = new AfterDataMessageConsumedListener()
			{
				final StoredMessageInfoManager delegate = ClusterFoundation.Default.this
					.getStoredMessageInfoManager();

				@Override
				public void onChange(final MessageInfo messageInfo) throws NodelibraryException
				{
					if (props.isBackupNode() || !"writer".equalsIgnoreCase(props.replicationRole()))
					{
						// Every reader must persist its resolved boundary; writers do not consume replication.
						this.delegate.set(messageInfo);
					}
				}

				@Override
				public void close()
				{
					this.delegate.close();
				}
			};
			LOG.trace(
				"Created AfterDataMessageConsumedListener->StoredMessageInfoManager delegate. WillRun={}",
				props.isBackupNode() || !"writer".equalsIgnoreCase(props.replicationRole())
			);
			return storedMessageInfoUpdater;
		}

		protected StorageBackupManager ensureStorageBackupManager()
		{
			final var props = this.getNodelibraryPropertiesProvider();
			final int maxBackupCount = props.keptBackupsCount();

			final Supplier<MessageInfo> messageInfoProvider = this.getClusterStorageBinaryDataClient()::messageInfo;

			return StorageBackupManager.New(
				this.clusterStorageManager,
				maxBackupCount,
				this.getStorageBackupBackend(),
				messageInfoProvider,
				this.getClusterStorageBinaryDataClient(),
				this.getReplicationLogRetention()
			);
		}

		protected Supplier<Object> ensureRootSupplier()
		{
			throw new MissingFoundationPartException(Supplier.class, "Missing root supplier");
		}

		protected ObjectGraphUpdateHandler ensureGraphUpdateHandler()
		{
			return ObjectGraphUpdateHandler.Synchronized();
		}

		protected EmbeddedStorageFoundation<?> ensureEmbeddedStorageFoundation()
		{
			return EmbeddedStorageFoundation.New();
		}

		protected BackupNodeManager ensureBackupNodeManager()
		{
			return BackupNodeManager.New(
				this.getStorageBackupTaskExecutor(),
				this.getClusterStorageBinaryDataClient(),
				this.clusterStorageManager,
				this.getStorageDiskSpaceReader()
			);
		}

		protected ReplicationCursor getReplicationCursorFromStoredInfo()
		{
			final MessageInfo info = this.getStoredMessageInfoManager().get();
			return new ReplicationCursor(
				info.transport(), info.storeGeneration(), info.messageIndex(), info.providerPosition()
			);
		}

		protected ClusterStorageBinaryDataClient ensureClusterStorageBinaryDataClient()
		{
			final var props = this.getNodelibraryPropertiesProvider();
			final boolean commitPosition = props.isBackupNode();
			return this.getClusterReplicationTransport().client(
				this.getClusterStorageBinaryDataPacketAcceptor(),
				props.replicationStreamName(),
				this.getAfterDataMessageConsumedListener(),
				this.getReplicationCursorFromStoredInfo(),
				commitPosition
			);
		}

		protected StorageNodeHealthCheck ensureStorageNodeHealthCheck()
		{
			return StorageNodeHealthCheck.New(
				this.clusterStorageManager,
				this.getClusterReplicationTransport().health(
					() -> this.clusterStorageManager.isRunning() && !this.clusterStorageManager.isStartingUp(),
					this.getClusterStorageBinaryDataClient()
				)
			);
		}

		protected NodelibraryPropertiesProvider ensureNodelibraryPropertiesProvider()
		{
			return NodelibraryPropertiesProvider.Env();
		}

		protected StorageDiskSpaceReader ensureStorageDiskSpaceReader()
		{
			return StorageDiskSpaceReader.New(
				this.getEmbeddedStorageFoundation().getConfiguration().fileProvider().baseDirectory()
			);
		}

		protected StorageNodeManager ensureStorageNodeManager()
		{
			return StorageNodeManager.New(
				this.getClusterStorageBinaryDataDistributor(),
				this.getStorageTaskExecutor(),
				this.getClusterStorageBinaryDataClient(),
				this.getStorageNodeHealthCheck(),
				this.clusterStorageManager,
				this.getStorageDiskSpaceReader(),
				this.getReplicationPositionProvider(),
				this.getClusterReplicationTransport().id()
			);
		}

		protected ClusterStorageBinaryDataDistributor ensureDataDistributor()
		{
			return ClusterStorageBinaryDataDistributor.Caching(
				this.getClusterReplicationTransport().distributor(
					this.getNodelibraryPropertiesProvider().replicationStreamName(),
					this.getEnableAsyncDistribution()
				)
			);
		}

		protected ClusterStorageBinaryDataMerger ensureClusterStorageBinaryDataMerger()
		{
			final Long cachingTimeoutMsNullable = this.getNodelibraryPropertiesProvider().dataMergerTimeoutMs();
			final long cachingTimeoutMs = cachingTimeoutMsNullable == null ? ClusterStorageBinaryDataMerger.Defaults
				.cachingTimeoutMs() : cachingTimeoutMsNullable;

			final Long cachedDataLimitNullable = this.getNodelibraryPropertiesProvider().dataMergerCachedDataLimit();
			final long cachedDataLimit = cachedDataLimitNullable == null ? ClusterStorageBinaryDataMerger.Defaults
				.cachingLimit() : cachedDataLimitNullable;

			return ClusterStorageBinaryDataMerger.New(
				this.getEmbeddedStorageFoundation().getConnectionFoundation(),
				this.clusterStorageManager,
				this.getObjectGraphUpdateHandler(),
				cachingTimeoutMs,
				cachedDataLimit
			);
		}

		protected ClusterStorageBinaryDataPacketAcceptor ensureDataPacketAcceptor()
		{
			return ClusterStorageBinaryDataPacketAcceptor.New(this.getClusterStorageBinaryDataMerger());
		}

		protected MessageInfoParser ensureMessageInfoParser()
		{
			return MessageInfoParser.New();
		}

		@Override
		public StorageBackupBackend getStorageBackupBackend()
		{
			if (this.backupBackend == null)
			{
				this.backupBackend = this.dispatch(this.ensureBackupBackend());
			}
			return this.backupBackend;
		}

		@Override
		public F setStorageBackupBackend(final StorageBackupBackend backend)
		{
			this.backupBackend = backend;
			return this.$();
		}

		@Override
		public StorageTaskExecutor getStorageTaskExecutor()
		{
			if (this.storageTaskExecutor == null)
			{
				this.storageTaskExecutor = this.dispatch(this.ensureStorageTaskExecutor());
			}
			return this.storageTaskExecutor;
		}

		@Override
		public F setStorageTaskExecutor(final StorageTaskExecutor executor)
		{
			this.storageTaskExecutor = executor;
			return this.$();
		}

		@Override
		public StorageBackupTaskExecutor getStorageBackupTaskExecutor()
		{
			if (this.storageBackupTaskExecutor == null)
			{
				this.storageBackupTaskExecutor = this.dispatch(this.ensureStorageBackupTaskExecutor());
			}
			return this.storageBackupTaskExecutor;
		}

		@Override
		public F setStorageBackupTaskExecutor(final StorageBackupTaskExecutor executor)
		{
			this.storageBackupTaskExecutor = executor;
			return this.$();
		}

		@Override
		public StorageBackupQuartzCronJobManager getStorageBackupQuartzCronJobManager()
		{
			if (this.backupCronjobManager == null)
			{
				this.backupCronjobManager = this.dispatch(this.ensureStorageBackupQuartzCronJobManager());
			}
			return this.backupCronjobManager;
		}

		@Override
		public F setStorageBackupQuartzCronJobManager(final StorageBackupQuartzCronJobManager manager)
		{
			this.backupCronjobManager = manager;
			return this.$();
		}

		@Override
		public BackupProxyHttpClient getBackupProxyHttpClient()
		{
			if (this.backupProxyHttpClient == null)
			{
				this.backupProxyHttpClient = this.dispatch(this.ensureBackupProxyHttpClient());
			}
			return this.backupProxyHttpClient;
		}

		@Override
		public F setBackupProxyHttpClient(final BackupProxyHttpClient client)
		{
			this.backupProxyHttpClient = client;
			return this.$();
		}

		@Override
		public QuartzCronJobJobFactory getQuartzCronJobJobFactory()
		{
			if (this.cronJobFactory == null)
			{
				this.cronJobFactory = this.dispatch(this.ensureCronJobFactory());
			}
			return this.cronJobFactory;
		}

		@Override
		public F setQuartzCronJobJobFactory(final QuartzCronJobJobFactory factory)
		{
			this.cronJobFactory = factory;
			return this.$();
		}

		@Override
		public QuartzCronJobScheduler getQuartzCronJobScheduler()
		{
			if (this.cronJobScheduler == null)
			{
				this.cronJobScheduler = this.dispatch(this.ensureCronJobScheduler());
			}
			return this.cronJobScheduler;
		}

		@Override
		public F setQuartzCronJobScheduler(final QuartzCronJobScheduler scheduler)
		{
			this.cronJobScheduler = scheduler;
			return this.$();
		}

		@Override
		public GcWorkaroundQuartzCronJobManager getGcWorkaroundQuartzCronJobManager()
		{
			if (this.gcWorkaroundManager == null)
			{
				this.gcWorkaroundManager = this.dispatch(this.ensureGcWorkaroundManager());
			}
			return this.gcWorkaroundManager;
		}

		@Override
		public F setGcWorkaroundQuartzCronJobManager(final GcWorkaroundQuartzCronJobManager manager)
		{
			this.gcWorkaroundManager = manager;
			return this.$();
		}

		@Override
		public StorageLimitCheckerQuartzCronJobManager getStorageLimitCheckerQuartzCronJobManager()
		{
			if (this.limitCheckerManager == null)
			{
				this.limitCheckerManager = this.dispatch(this.ensureStorageLimitCheckerManager());
			}
			return this.limitCheckerManager;
		}

		@Override
		public F setStorageLimitCheckerQuartzCronJobManager(final StorageLimitCheckerQuartzCronJobManager manager)
		{
			this.limitCheckerManager = manager;
			return this.$();
		}

		@Override
		public ClusterReplicationTransport getClusterReplicationTransport()
		{
			if (this.replicationTransport == null)
			{
				this.replicationTransport = this.dispatch(this.ensureClusterReplicationTransport());
			}
			return this.replicationTransport;
		}

		@Override
		public F setClusterReplicationTransport(final ClusterReplicationTransport transport)
		{
			this.replicationTransport = transport;
			return this.$();
		}

		@Override
		public StorageBackupManager getStorageBackupManager()
		{
			if (this.storageBackupManager == null)
			{
				this.storageBackupManager = this.dispatch(this.ensureStorageBackupManager());
			}
			return this.storageBackupManager;
		}

		@Override
		public F setStorageBackupManager(final StorageBackupManager manager)
		{
			this.storageBackupManager = manager;
			return this.$();
		}

		@Override
		public ObjectGraphUpdateHandler getObjectGraphUpdateHandler()
		{
			if (this.graphUpdateHandler == null)
			{
				this.graphUpdateHandler = this.dispatch(this.ensureGraphUpdateHandler());
			}
			return this.graphUpdateHandler;
		}

		@Override
		public F setObjectGraphUpdateHandler(final ObjectGraphUpdateHandler handler)
		{
			this.graphUpdateHandler = handler;
			return this.$();
		}

		@Override
		public Supplier<Object> getRootSupplier()
		{
			if (this.rootSupplier == null)
			{
				this.rootSupplier = this.dispatch(this.ensureRootSupplier());
			}
			return this.rootSupplier;
		}

		@Override
		public F setRootSupplier(final Supplier<Object> supplier)
		{
			this.rootSupplier = supplier;
			return this.$();
		}

		@Override
		public boolean getEnableAsyncDistribution()
		{
			return this.enableAsyncDistribution;
		}

		@Override
		public F setEnableAsyncDistribution(final boolean enable)
		{
			this.enableAsyncDistribution = enable;
			return this.$();
		}

		@Override
		public EmbeddedStorageFoundation<?> getEmbeddedStorageFoundation()
		{
			if (this.embeddedStorageFoundation == null)
			{
				this.embeddedStorageFoundation = this.dispatch(this.ensureEmbeddedStorageFoundation());
			}
			return this.embeddedStorageFoundation;
		}

		@Override
		public F setEmbeddedStorageFoundation(final EmbeddedStorageFoundation<?> foundation)
		{
			this.embeddedStorageFoundation = foundation;
			return this.$();
		}

		@Override
		public BackupNodeManager getBackupNodeManager()
		{
			if (this.backupNodeManager == null)
			{
				this.backupNodeManager = this.dispatch(this.ensureBackupNodeManager());
			}
			return this.backupNodeManager;
		}

		@Override
		public F setBackupNodeManager(final BackupNodeManager manager)
		{
			this.backupNodeManager = manager;
			return this.$();
		}

		@Override
		public ClusterStorageBinaryDataClient getClusterStorageBinaryDataClient()
		{
			if (this.dataClient == null)
			{
				this.dataClient = this.dispatch(this.ensureClusterStorageBinaryDataClient());
			}
			return this.dataClient;
		}

		@Override
		public F setClusterStorageBinaryDataClient(final ClusterStorageBinaryDataClient client)
		{
			this.dataClient = client;
			return this.$();
		}

		@Override
		public ClusterStorageBinaryDataDistributor getClusterStorageBinaryDataDistributor()
		{
			if (this.dataDistributor == null)
			{
				this.dataDistributor = this.dispatch(this.ensureDataDistributor());
			}
			return this.dataDistributor;
		}

		@Override
		public F setClusterStorageBinaryDataDistributor(final ClusterStorageBinaryDataDistributor distributor)
		{
			this.dataDistributor = distributor;
			return this.$();
		}

		@Override
		public StorageNodeHealthCheck getStorageNodeHealthCheck()
		{
			if (this.healthCheck == null)
			{
				this.healthCheck = this.dispatch(this.ensureStorageNodeHealthCheck());
			}
			return this.healthCheck;
		}

		@Override
		public F setStorageNodeHealthCheck(final StorageNodeHealthCheck check)
		{
			this.healthCheck = check;
			return this.$();
		}

		@Override
		public NodelibraryPropertiesProvider getNodelibraryPropertiesProvider()
		{
			if (this.propertiesProvider == null)
			{
				this.propertiesProvider = this.dispatch(this.ensureNodelibraryPropertiesProvider());
			}
			return this.propertiesProvider;
		}

		@Override
		public F setNodelibraryPropertiesProvider(final NodelibraryPropertiesProvider provider)
		{
			this.propertiesProvider = provider;
			return this.$();
		}

		@Override
		public StorageDiskSpaceReader getStorageDiskSpaceReader()
		{
			if (this.storageDiskSpaceReader == null)
			{
				this.storageDiskSpaceReader = this.dispatch(this.ensureStorageDiskSpaceReader());
			}
			return this.storageDiskSpaceReader;
		}

		@Override
		public F setStorageDiskSpaceReader(final StorageDiskSpaceReader reader)
		{
			this.storageDiskSpaceReader = reader;
			return this.$();
		}

		@Override
		public StorageNodeManager getStorageNodeManager()
		{
			if (this.storageNodeManager == null)
			{
				this.storageNodeManager = this.dispatch(this.ensureStorageNodeManager());
			}
			return this.storageNodeManager;
		}

		@Override
		public F setStorageNodeManager(final StorageNodeManager manager)
		{
			this.storageNodeManager = manager;
			return this.$();
		}

		@Override
		public AfterDataMessageConsumedListener getAfterDataMessageConsumedListener()
		{
			if (this.afterDataMessageConsumedListener == null)
			{
				this.afterDataMessageConsumedListener = this.dispatch(this.ensureAfterDataMessageConsumedListener());
			}
			return this.afterDataMessageConsumedListener;
		}

		@Override
		public F setAfterDataMessageConsumedListener(final AfterDataMessageConsumedListener listener)
		{
			this.afterDataMessageConsumedListener = listener;
			return this.$();
		}

		@Override
		public ClusterStorageBinaryDataMerger getClusterStorageBinaryDataMerger()
		{
			if (this.dataMerger == null)
			{
				this.dataMerger = this.dispatch(this.ensureClusterStorageBinaryDataMerger());
			}
			return this.dataMerger;
		}

		@Override
		public F setClusterStorageBinaryDataMerger(final ClusterStorageBinaryDataMerger merger)
		{
			this.dataMerger = merger;
			return this.$();
		}

		@Override
		public ClusterStorageBinaryDataPacketAcceptor getClusterStorageBinaryDataPacketAcceptor()
		{
			if (this.dataPacketAcceptor == null)
			{
				this.dataPacketAcceptor = this.dispatch(this.ensureDataPacketAcceptor());
			}
			return this.dataPacketAcceptor;
		}

		@Override
		public F setClusterStorageBinaryDataPacketAcceptor(final ClusterStorageBinaryDataPacketAcceptor acceptor)
		{
			this.dataPacketAcceptor = acceptor;
			return this.$();
		}

		@Override
		public StoredMessageInfoManager getStoredMessageInfoManager()
		{
			if (this.storedMessageInfoManager == null)
			{
				this.storedMessageInfoManager = this.dispatch(this.ensureStoredMessageInfoManager());
			}
			return this.storedMessageInfoManager;
		}

		@Override
		public F setStoredMessageInfoManager(final StoredMessageInfoManager manager)
		{
			this.storedMessageInfoManager = manager;
			return this.$();
		}

		@Override
		public ReplicationPositionProvider getReplicationPositionProvider()
		{
			if (this.positionProvider == null)
			{
				this.positionProvider = this.dispatch(this.ensureReplicationPositionProvider());
			}
			return this.positionProvider;
		}

		@Override
		public F setReplicationPositionProvider(final ReplicationPositionProvider provider)
		{
			this.positionProvider = provider;
			return this.$();
		}

		@Override
		public ReplicationLogRetention getReplicationLogRetention()
		{
			if (this.replicationRetention == null)
			{
				this.replicationRetention = this.dispatch(this.ensureReplicationLogRetention());
			}
			return this.replicationRetention;
		}

		@Override
		public F setReplicationLogRetention(final ReplicationLogRetention retention)
		{
			this.replicationRetention = retention;
			return this.$();
		}

		@Override
		public MessageInfoParser getMessageInfoParser()
		{
			if (this.messageInfoParser == null)
			{
				this.messageInfoParser = this.dispatch(this.ensureMessageInfoParser());
			}
			return this.messageInfoParser;
		}

		@Override
		public F setMessageInfoParser(final MessageInfoParser messageInfoParser)
		{
			this.messageInfoParser = messageInfoParser;
			return this.$();
		}

		@Override
		public ClusterRestRequestController startController() throws NodelibraryException
		{
			if (this.clusterRequestController == null)
			{
				this.start();
			}

			return this.clusterRequestController;
		}

		@Override
		public ClusterStorageManager<?> startStorageManager() throws NodelibraryException
		{
			if (this.clusterStorageManager == null)
			{
				this.start();
			}

			return this.clusterStorageManager;
		}

		protected void start() throws NodelibraryException
		{
			final var properties = this.getNodelibraryPropertiesProvider();

			if (!properties.isProdMode())
			{
				this.startDevNode();
			}
			else if (properties.isBackupNode())
			{
				this.startBackupNode();
			}
			else
			{
				this.startStorageNode();
			}
		}

		protected void startBackupNode() throws NodelibraryException
		{
			LOG.info("Starting backup cluster node");

			this.getReplicationPositionProvider().init();

			final var storageParentPath = this.storageParentPath();
			final var storageRootPath = storageParentPath.resolve("storage");

			// if we use a downloaded storage, always scroll to the latest message so we don't read old messages
			boolean useLatestMessageInfo = false;
			boolean requiresStorageUpload = false;

			// don't send messages generated by starting the storage and storing the empty root
			this.getClusterStorageBinaryDataDistributor().ignoreDistribution(true);

			final var backend = this.getStorageBackupBackend();

			final boolean containsBackups = backend.containsBackups();

			/*
			 * If there are backups already available, use those instead as a fresh cluster
			 * has none, but an upgraded cluster has the previous storage backed up
			 */

			// user uploaded a new storage
			if (backend.hasUserUploadedStorage())
			{
				LOG.info("Downloading user uploaded storage");

                useLatestMessageInfo = true;
				// since the storage is now different from before,
				// the storage nodes also need the exact same storage
				requiresStorageUpload = true;
				this.deleteDirectory(storageRootPath);
				backend.downloadUserUploadedStorage(storageParentPath);
				backend.deleteUserUploadedStorage();
			}
			else if (containsBackups && !Files.exists(storageRootPath))
			{
				LOG.info("Downloading latest storage backup");
				backend.downloadLatestBackup(storageParentPath);
			}
			else
			{
				LOG.info("Starting with local storage");
			}

			if (useLatestMessageInfo)
			{
				final ReplicationCursor cursor;
				try
				{
					cursor = this.getReplicationPositionProvider().latest();
				}
				catch (final UnsupportedOperationException failure)
				{
					throw new NodelibraryException(
						"Cannot bootstrap uploaded storage: replication transport does not expose a writer latest position",
						failure);
				}
				final var info = MessageInfo.New(
					cursor.logicalSequence(), cursor.transport(), cursor.storeGeneration(), cursor.providerPosition()
				);
				LOG.debug("Set starting message info to: {}", info);
				this.getStoredMessageInfoManager().set(info);
			}

			LOG.info("Creating nodelibrary cluster controller");

			final var embeddedStorageManager = this.prepareEmbeddedStorage(storageRootPath).start();
			this.initializeRoot(embeddedStorageManager);

			this.getClusterStorageBinaryDataDistributor().ignoreDistribution(false);
			this.queueAeronWriterDictionary(embeddedStorageManager);

			final var scheduler = this.getQuartzCronJobScheduler();

				this.clusterStorageManager = ClusterStorageManager.Wrapper(embeddedStorageManager, () ->
				{
					scheduler.shutdown();
					this.closeReplicationTransportAndPositionProvider();
				});

			this.getClusterStorageBinaryDataClient().start();

			this.clusterRequestController = ClusterRestRequestController.BackupNode(
				this.getBackupNodeManager(),
				this.getNodelibraryPropertiesProvider()
			);

			final var jobFactory = this.getQuartzCronJobJobFactory();
			scheduler.setFactory(jobFactory);

			final var gcWorkaround = this.getGcWorkaroundQuartzCronJobManager();
			jobFactory.setJobFactory(GcWorkaroundQuartzCronJob.class, gcWorkaround::create);
			scheduler.schedule(
				JobBuilder.newJob(GcWorkaroundQuartzCronJob.class).withIdentity("GcWorkaround").build(),
				// Once every 30 minutes
				TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule("0 */30 * * * ? *")).build()
			);

			final var storageBackup = this.getStorageBackupQuartzCronJobManager();
			jobFactory.setJobFactory(StorageBackupQuartzCronJob.class, storageBackup::create);
			scheduler.schedule(
				JobBuilder.newJob(StorageBackupQuartzCronJob.class).withIdentity("StorageBackup").build(),
				// Once at the start of every 2 hours
				TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule("0 0 */2 * * ? *")).build()
			);

			// storage nodes need an initial backup to start from
			if (requiresStorageUpload)
			{
				LOG.info("Uploading starter backup for storage nodes");
				this.getBackupNodeManager().createStorageBackup(false);
			}

			scheduler.start();
		}

		protected void startStorageNode() throws NodelibraryException
		{
			LOG.info("Starting storage cluster node");

			final var storageParentPath = this.storageParentPath();
			final var storageRootPath = storageParentPath.resolve("storage");

			// don't send messages generated by starting the storage and storing the empty root
			this.getClusterStorageBinaryDataDistributor().ignoreDistribution(true);

			final var backend = this.getStorageBackupBackend();
			final boolean containsBackups = backend.containsBackups();

			/*
			 * If there are backups already available, use those instead
			 */

			if (containsBackups && !Files.exists(storageRootPath))
			{
				LOG.info("Downloading latest storage backup");
				backend.downloadLatestBackup(storageParentPath);
			}
			else
			{
				LOG.info(Files.exists(storageRootPath)
					? "Resuming existing local storage and cursor"
					: "Starting with local storage");
			}

			// don't send messages generated by starting the storage and storing the empty root
			this.getClusterStorageBinaryDataDistributor().ignoreDistribution(true);

			this.getReplicationPositionProvider().init();

			final var dataDistributor = this.getClusterStorageBinaryDataDistributor();
			final var embeddedStorageFoundation = this.prepareEmbeddedStorage(storageRootPath);
			DistributedStorage.configureWriting(
				embeddedStorageFoundation,
				dataDistributor,
				this.getClusterReplicationTransport().persistenceTargetFactory(
					this.getNodelibraryPropertiesProvider().replicationStreamName(), dataDistributor
				)
			);

			final var embeddedStorageManager = embeddedStorageFoundation.start();
			this.initializeRoot(embeddedStorageManager);

			this.getClusterStorageBinaryDataDistributor().ignoreDistribution(false);
			this.queueAeronWriterDictionary(embeddedStorageManager);

			final var scheduler = this.getQuartzCronJobScheduler();

			this.clusterStorageManager = ClusterStorageManager.New(
				embeddedStorageManager,
				() -> this.getStorageLimitCheckerQuartzCronJobManager().limitReached(),
				() ->
				{
					this.getClusterReplicationTransport().close();
					scheduler.shutdown();
				}
			);

			this.getClusterStorageBinaryDataClient().start();

			this.getStorageNodeHealthCheck().init();

			this.clusterRequestController = ClusterRestRequestController.StorageNode(
				this.getStorageNodeManager(),
				this.getNodelibraryPropertiesProvider()
			);

			final var jobFactory = this.getQuartzCronJobJobFactory();

			final var gcWorkaround = this.getGcWorkaroundQuartzCronJobManager();
			jobFactory.setJobFactory(GcWorkaroundQuartzCronJob.class, gcWorkaround::create);

			final var limitChecker = this.getStorageLimitCheckerQuartzCronJobManager();
			jobFactory.setJobFactory(StorageLimitCheckerQuartzCronJob.class, limitChecker::create);

			scheduler.setFactory(jobFactory);
			scheduler.schedule(
				JobBuilder.newJob(GcWorkaroundQuartzCronJob.class).withIdentity("GcWorkaround").build(),
				// Once at the start of every hour
				TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule("0 0 * * * ? *")).build()
			);
			scheduler.schedule(
				JobBuilder.newJob(StorageLimitCheckerQuartzCronJob.class).withIdentity("StorageLimitChecker").build(),
				TriggerBuilder.newTrigger()
					.withSchedule(
						SimpleScheduleBuilder.repeatMinutelyForever(
							this.getNodelibraryPropertiesProvider().storageLimitCheckerIntervalMinutes()
						)
					)
					.build()
			);

			scheduler.start();
		}

		/**
		 * Queues the complete persisted dictionary for the first post-restart Aeron
		 * transaction. This covers types introduced by a rejected transaction whose
		 incremental export was consumed before the writer crashed.
		 */
		private void queueAeronWriterDictionary(final org.eclipse.store.storage.embedded.types.EmbeddedStorageManager storage)
		{
			final NodelibraryPropertiesProvider props = this.getNodelibraryPropertiesProvider();
			if (!"aeron".equalsIgnoreCase(this.getClusterReplicationTransport().id()) ||
				!"writer".equalsIgnoreCase(props.replicationRole()))
			{
				return;
			}
			final String dictionary = PersistenceTypeDictionaryAssembler.New().assemble(storage.typeDictionary());
			this.getClusterStorageBinaryDataDistributor().queueTypeDictionaryForNextTransaction(dictionary);
		}

		private EmbeddedStorageFoundation<?> prepareEmbeddedStorage(final Path storageRootPath)
		{
			final var foundation = this.getEmbeddedStorageFoundation();
			final StorageConfiguration current = foundation.getConfiguration();
			foundation.setConfiguration(StorageConfiguration.Builder()
				.setBackupSetup(current.backupSetup())
				.setChannelCountProvider(current.channelCountProvider())
				.setDataFileEvaluator(current.dataFileEvaluator())
				.setEntityCacheEvaluator(current.entityCacheEvaluator())
				.setHousekeepingController(current.housekeepingController())
				.setStorageFileProvider(
					StorageLiveFileProvider.New(NioFileSystem.New().ensureDirectory(storageRootPath))
				)
				.createConfiguration());
			foundation.setExceptionHandler((throwable, channel) ->
			{
				try
				{
					StorageExceptionHandler.defaultHandleException(throwable, channel);
				}
				catch (final StorageException exception)
				{
					GlobalErrorHandling.handleFatalError(exception);
				}
			});
			return foundation;
		}

		private void initializeRoot(final StorageManager storage)
		{
			if (storage.root() == null)
			{
				LOG.debug("Setting and storing new root from root supplier");
				final Object root = this.getRootSupplier().get();
				storage.setRoot(root instanceof Lazy ? root : Lazy.Reference(root));
				storage.storeRoot();
			}
		}

		protected void startDevNode() throws NodelibraryException
		{
			LOG.info("Starting dev cluster node");
			final var storage = this.getEmbeddedStorageFoundation().start();
			if (storage.root() == null)
			{
				final var root = this.getRootSupplier().get();
				if (root instanceof Lazy)
				{
					storage.setRoot(root);
				}
				else
				{
					storage.setRoot(Lazy.Reference(root));
				}
				storage.storeRoot();
			}

			this.clusterStorageManager = ClusterStorageManager.Wrapper(
				storage,
				() -> this.getClusterReplicationTransport().close()
			);
			this.clusterRequestController = ClusterRestRequestController.DevNode();
		}

		private void deleteDirectory(final Path path)
		{
			if (!Files.exists(path))
			{
				return;
			}

			LOG.info("Deleting files at {}", path);
			StorageFileOperations.deleteDirectory(path);
		}

		private void closeReplicationTransportAndPositionProvider()
		{
			RuntimeException failure = null;
			if (this.replicationTransport != null)
			{
				try
				{
					this.replicationTransport.close();
				}
				catch (final RuntimeException closeFailure)
				{
					failure = closeFailure;
				}
			}
			if (this.positionProvider != null)
			{
				try
				{
					this.positionProvider.close();
				}
				catch (final RuntimeException closeFailure)
				{
					if (failure == null) failure = closeFailure;
					else failure.addSuppressed(closeFailure);
				}
			}
			if (this.replicationRetention != null)
			{
				try
				{
					this.replicationRetention.close();
				}
				catch (final RuntimeException closeFailure)
				{
					if (failure == null) failure = closeFailure;
					else failure.addSuppressed(closeFailure);
				}
			}
			if (failure != null)
			{
				throw new IllegalStateException("failed to close replication resources", failure);
			}
		}
	}
}
