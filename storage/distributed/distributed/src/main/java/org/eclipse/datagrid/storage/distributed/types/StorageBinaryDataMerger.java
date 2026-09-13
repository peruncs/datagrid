package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
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


import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistence;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinition;
import org.eclipse.serializer.persistence.types.PersistenceTypeDescription;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.util.logging.Logging;
import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.eclipse.serializer.util.X.notNull;

/** Applies imported Store binaries and schedules object-graph refresh work. */
	public interface StorageBinaryDataMerger extends StorageBinaryDataReceiver
	{
		/** Creates a merger for one Store connection.
		 *
		 * @param foundation persistence foundation used for remote types
		 * @param storage Store connection that receives data
		 * @param objectGraphUpdateHandler callback that protects graph updates
		 * @return configured merger
		 */
		static StorageBinaryDataMerger New(
		final BinaryPersistenceFoundation<?> foundation,
		final StorageConnection storage,
		final ObjectGraphUpdateHandler objectGraphUpdateHandler
	)
	{
		return new StorageBinaryDataMerger.Default(
			notNull(foundation),
			notNull(storage),
			notNull(objectGraphUpdateHandler)
		);
	}

	/** Imports each binary and schedules its graph update. */
	class Default implements StorageBinaryDataMerger
	{
		private final static Logger logger = Logging.getLogger(StorageBinaryDataMerger.class);

		private final BinaryPersistenceFoundation<?> foundation;
		private final StorageConnection storage;
		private final ObjectGraphUpdateHandler objectGraphUpdateHandler;

		Default(
			final BinaryPersistenceFoundation<?> foundation,
			final StorageConnection storage,
			final ObjectGraphUpdateHandler objectGraphUpdateHandler
		)
		{
			super();
			this.foundation = foundation;
			this.storage = storage;
			this.objectGraphUpdateHandler = objectGraphUpdateHandler;
		}

		@Override
		public synchronized void receiveData(final Binary data)
		{
			logger.debug("Importing data");
			final ByteBuffer[] sourceBuffers = StorageBinaryDataChunker.buffers(data).toArray(ByteBuffer[]::new);
			final ByteBuffer[] ownedBuffers = StorageBinaryDataImporter.importOwned(this.storage, sourceBuffers);
			/* scheduleMaterialization releases the buffers when callback registration
			 * fails.  Do not release again here: native buffers must have exactly one
			 * owner after importOwned returns. */
			this.scheduleMaterialization(ownedBuffers);
		}

		private void scheduleMaterialization(final ByteBuffer[] ownedBuffers)
		{
			final AtomicBoolean released = new AtomicBoolean();
			final Runnable release = () ->
			{
				if (released.compareAndSet(false, true))
				{
					StorageBinaryDataImporter.release(ownedBuffers);
				}
			};

			final ObjectGraphUpdater updater = () ->
			{
				logger.debug("Updating object graph");

				try
				{
					StorageBinaryDataMaterializer.materialize(this.storage, ownedBuffers);
				}
				finally
				{
					release.run();
				}
			};
			try
			{
				this.objectGraphUpdateHandler.objectGraphUpdateAvailable(updater).toCompletableFuture().join();
			}
			catch (final RuntimeException | Error failure)
			{
				release.run();
				throw failure;
			}
		}

		@Override
		public synchronized void receiveTypeDictionary(final String typeDictionaryData)
		{
			final PersistenceTypeDictionary remoteTypeDictionary = BinaryPersistence.Foundation()
				.setClassLoaderProvider(this.foundation.getClassLoaderProvider())
				.setFieldEvaluatorPersister(this.foundation.getFieldEvaluatorPersistable())
				.setTypeDictionaryLoader(() -> typeDictionaryData)
				.getTypeDictionaryProvider()
				.provideTypeDictionary();
			final PersistenceTypeDictionary localTypeDictionary = this.storage.persistenceManager().typeDictionary();

			remoteTypeDictionary.iterateAllTypeDefinitions(remoteType ->
			{
				final PersistenceTypeDefinition localType = localTypeDictionary.lookupTypeById(remoteType.typeId());
				if (localType == null)
				{
					logger.debug("New type: {}", remoteType.typeName());
					this.foundation.getTypeHandlerManager().ensureTypeHandler(remoteType);

				}
				else if (!PersistenceTypeDescription.equalStructure(localType, remoteType))
				{
					throw new RuntimeException(localType + " <> " + remoteType);
				}
			});
			/* Replicated imports do not execute a local Store operation that would
			 * normally flush the exporting dictionary manager.  Flush explicitly before
			 * importing data that can reference the newly registered type ids. */
			this.foundation.getTypeHandlerManager().exportPendingTypeDictionaryChanges();
		}

	}

}
