package peruncs.datagrid.storage.distributed.types;


import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistence;
import org.eclipse.serializer.persistence.binary.types.BinaryPersistenceFoundation;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinition;
import org.eclipse.serializer.persistence.types.PersistenceTypeDescription;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.store.storage.types.StorageConnection;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
		private static final System.Logger LOGGER =
			System.getLogger(StorageBinaryDataMerger.class.getName());

		private final BinaryPersistenceFoundation<?> foundation;
		private final StorageConnection storage;
		private final ObjectGraphUpdateHandler objectGraphUpdateHandler;
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();

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
		public void receiveData(final Binary data)
		{
			this.ensureUsable();
			LOGGER.log(System.Logger.Level.DEBUG, "Importing data");
			final ByteBuffer[] sourceBuffers = StorageBinaryDataChunker.importArray(data);
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
				LOGGER.log(System.Logger.Level.DEBUG, "Updating object graph");

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
				this.objectGraphUpdateHandler.objectGraphUpdateAvailable(updater)
					.toCompletableFuture().get(60L, TimeUnit.SECONDS);
			}
			catch (final InterruptedException failure)
			{
				Thread.currentThread().interrupt();
				/* The update callback may still be running after this wait is
				 * interrupted. Its finally block remains the sole owner of release. */
				this.fail("Interrupted while applying Store data", failure);
				throw new StorageBinaryDataException("Interrupted while applying Store data", failure);
			}
			catch (final ExecutionException failure)
			{
				release.run();
				final StorageBinaryDataException terminal = new StorageBinaryDataException(
					"Timed out or failed while applying Store data",
					failure.getCause() == null ? failure : failure.getCause());
				this.fail(terminal.getMessage(), terminal);
				throw terminal;
			}
			catch (final TimeoutException failure)
			{
				/* A timed-out Future does not cancel the update callback. Releasing
				 * here would deallocate buffers while Store materialization still uses
				 * them; the callback's finally block owns that release. */
				final StorageBinaryDataException terminal = new StorageBinaryDataException(
					"Timed out while applying Store data", failure);
				this.fail(terminal.getMessage(), terminal);
				throw terminal;
			}
			catch (final RuntimeException | Error failure)
			{
				release.run();
				if (failure instanceof RuntimeException runtime)
				{
					this.fail("Store graph update failed", runtime);
				}
				throw failure;
			}
		}

		@Override
		public synchronized void receiveTypeDictionary(final String typeDictionaryData)
		{
			this.ensureUsable();
			try
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
					LOGGER.log(System.Logger.Level.DEBUG, "New type: " + remoteType.typeName());
					this.foundation.getTypeHandlerManager().ensureTypeHandler(remoteType);

				}
				else if (!PersistenceTypeDescription.equalStructure(localType, remoteType))
				{
					throw new StorageBinaryDataException(
						"Remote type definition conflicts with local definition: "
							+ localType + " <> " + remoteType
					);
				}
			});
			/* Replicated imports do not execute a local Store operation that would
			 * normally flush the exporting dictionary manager.  Flush explicitly before
			 * importing data that can reference the newly registered type ids. */
			this.foundation.getTypeHandlerManager().exportPendingTypeDictionaryChanges();
			}
			catch (final RuntimeException failure)
			{
				this.fail("Store type dictionary update failed", failure);
				throw failure;
			}
			catch (final Error failure)
			{
				this.fail("Store type dictionary update failed", failure);
				throw failure;
			}
		}

		private void ensureUsable()
		{
			final RuntimeException terminal = this.failure.get();
			if (terminal != null)
			{
				throw new IllegalStateException("Storage binary merger has failed", terminal);
			}
		}

		private void fail(final String message, final Throwable cause)
		{
			this.failure.compareAndSet(null, new IllegalStateException(message, cause));
		}

	}

}
