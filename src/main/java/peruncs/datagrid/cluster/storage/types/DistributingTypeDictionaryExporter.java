package peruncs.datagrid.cluster.storage.types;


import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryAssembler;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;

import static org.eclipse.serializer.util.X.notNull;

/// Sends the persistence type dictionary alongside storage data.
///
/// The local delegate is updated first and the assembled dictionary is
/// distributed second, so receivers can learn the type definitions before
/// they materialize a later binary message.
final class DistributingTypeDictionaryExporter implements PersistenceTypeDictionaryExporter {
    /* The assembler is stateless apart from its configuration; one shared
     * instance serves every exporter instead of one per foundation. */
    private static final PersistenceTypeDictionaryAssembler ASSEMBLER = PersistenceTypeDictionaryAssembler.New();

    /// Creates an exporter that publishes each local dictionary.
    ///
    /// @param delegate    local dictionary exporter
    /// @param distributor destination for the dictionary
    /// @return distributing exporter
    static DistributingTypeDictionaryExporter New(
            final PersistenceTypeDictionaryExporter delegate,
            final StorageBinaryDataDistributor distributor) {
        return new DistributingTypeDictionaryExporter(notNull(delegate), ASSEMBLER, notNull(distributor));
    }

    private final PersistenceTypeDictionaryExporter delegate;
    private final PersistenceTypeDictionaryAssembler assembler;
    private final StorageBinaryDataDistributor distributor;

    DistributingTypeDictionaryExporter(
            final PersistenceTypeDictionaryExporter delegate,
            final PersistenceTypeDictionaryAssembler assembler,
            final StorageBinaryDataDistributor distributor
    ) {
        this.delegate = delegate;
        this.assembler = assembler;
        this.distributor = distributor;
    }

    @Override
    public void exportTypeDictionary(final PersistenceTypeDictionary typeDictionary) {
        this.delegate.exportTypeDictionary(typeDictionary);
        this.distributor.distributeTypeDictionary(
                this.assembler.assemble(typeDictionary)
        );
    }
}
