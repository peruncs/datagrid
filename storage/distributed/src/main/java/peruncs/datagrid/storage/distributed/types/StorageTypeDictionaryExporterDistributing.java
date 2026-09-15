package peruncs.datagrid.storage.distributed.types;


import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryAssembler;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;

import static org.eclipse.serializer.util.X.notNull;

/**
 * This exporter sends the persistence type dictionary along with storage data.
 *
 * <p>It first updates the local delegate and then distributes the assembled
 * dictionary. Receivers can therefore learn the type definitions before they
 * materialize a later binary message.</p>
 */
public interface StorageTypeDictionaryExporterDistributing extends PersistenceTypeDictionaryExporter
{
	/** Creates an exporter that publishes each local dictionary.
	 *
	 * @param delegate local dictionary exporter
	 * @param distributor destination for the dictionary
	 * @return distributing exporter
	 */
	static StorageTypeDictionaryExporterDistributing New(
            final PersistenceTypeDictionaryExporter delegate,
            final StorageBinaryDataDistributor distributor
    )
	{
		return new StorageTypeDictionaryExporterDistributing.Default(
			notNull(delegate),
			PersistenceTypeDictionaryAssembler.New(), // use default assembler
			notNull(distributor)
		);
	}

	/** Exports locally before publishing the matching type dictionary. */
	class Default implements StorageTypeDictionaryExporterDistributing
	{
		private final PersistenceTypeDictionaryExporter delegate;
		private final PersistenceTypeDictionaryAssembler assembler;
		private final StorageBinaryDataDistributor distributor;

		Default(
			final PersistenceTypeDictionaryExporter delegate,
			final PersistenceTypeDictionaryAssembler assembler,
			final StorageBinaryDataDistributor distributor
		)
		{
			super();
			this.delegate = delegate;
			this.assembler = assembler;
			this.distributor = distributor;
		}

		@Override
		public void exportTypeDictionary(final PersistenceTypeDictionary typeDictionary)
		{
			this.delegate.exportTypeDictionary(typeDictionary);
			this.distributor.distributeTypeDictionary(
				this.assembler.assemble(typeDictionary)
			);
		}

	}

}
