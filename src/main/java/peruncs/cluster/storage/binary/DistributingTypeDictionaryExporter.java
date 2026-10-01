package peruncs.cluster.storage.binary;


import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryAssembler;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;

import static org.eclipse.serializer.util.X.notNull;

/// Sends the persistence type dictionary alongside storage data.
///
/// The local delegate is updated first and the assembled dictionary is
/// staged in the outbox second, so receivers learn the type definitions before
/// they materialize a later binary message.
public final class DistributingTypeDictionaryExporter implements PersistenceTypeDictionaryExporter {
    /* The assembler is stateless apart from its configuration; one shared
     * instance serves every exporter instead of one per foundation. */
    private static final PersistenceTypeDictionaryAssembler ASSEMBLER = PersistenceTypeDictionaryAssembler.New();

    /// Creates an exporter that publishes each local dictionary.
    ///
    /// @param delegate    local dictionary exporter
    /// @param outbox      destination for the dictionary
    /// @return distributing exporter
    public static DistributingTypeDictionaryExporter create(
            final PersistenceTypeDictionaryExporter delegate,
            final TypeDictionaryOutbox outbox) {
        return new DistributingTypeDictionaryExporter(notNull(delegate), ASSEMBLER, notNull(outbox));
    }

    private final PersistenceTypeDictionaryExporter delegate;
    private final PersistenceTypeDictionaryAssembler assembler;
    private final TypeDictionaryOutbox outbox;

    DistributingTypeDictionaryExporter(
            final PersistenceTypeDictionaryExporter delegate,
            final PersistenceTypeDictionaryAssembler assembler,
            final TypeDictionaryOutbox outbox
    ) {
        this.delegate = delegate;
        this.assembler = assembler;
        this.outbox = outbox;
    }

    @Override
    public void exportTypeDictionary(final PersistenceTypeDictionary typeDictionary) {
        this.delegate.exportTypeDictionary(typeDictionary);
        this.outbox.stageIncremental(this.assembler.assemble(typeDictionary));
    }
}
