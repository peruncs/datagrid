package peruncs.datagrid.cluster.storage.types;


import org.eclipse.serializer.functional.InstanceDispatcherLogic;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageConnectionFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;

import java.util.function.UnaryOperator;

import static org.eclipse.serializer.util.X.notNull;

/// This utility installs distributed writing into an embedded Store foundation.
///
/// The configured foundation keeps its normal local target and type
/// dictionary, then wraps both so committed binary data and type definitions
/// reach the supplied distributor. The utility changes the foundation in place
/// and returns it for fluent setup.
public final class DistributedStorage {
    private DistributedStorage() {
        throw new UnsupportedOperationException();
    }

        /// Adds distributed writing to an embedded storage foundation.
    ///
    /// The target factory owns the durability semantics: production must use
    /// the transport's coordinated factory so local acceptance and publication
    /// remain one operation, never a local commit followed by an uncoordinated
    /// distribution.
    ///
    /// @param foundation    foundation to configure
    /// @param distributor   destination for committed data
    /// @param targetFactory wrapper for the local persistence target
    /// @return the configured foundation
    public static EmbeddedStorageFoundation<?> configureWriting(
            final EmbeddedStorageFoundation<?> foundation,
            final StorageBinaryDataDistributor distributor,
            final UnaryOperator<PersistenceTarget<Binary>> targetFactory) {
        final EmbeddedStorageConnectionFoundation<?> connectionFoundation = foundation.getConnectionFoundation();
        connectionFoundation.setInstanceDispatcher(new Configurator(distributor, targetFactory));
        return foundation;
    }

        /// Internal assembly helper that wraps Store components with
    /// distributed-writing behavior. It is not an application API.
    ///
    /// Persistence targets distribute committed binary data, and type dictionary
    /// exporters distribute type definitions. Other objects pass through unchanged
    /// so the normal Store foundation keeps its existing behavior.
    static final class Configurator implements InstanceDispatcherLogic {
        private final StorageBinaryDataDistributor distributor;
        private final UnaryOperator<PersistenceTarget<Binary>> targetFactory;
        private final InstanceDispatcherLogic previous;

        Configurator(final StorageBinaryDataDistributor distributor, final UnaryOperator<PersistenceTarget<Binary>> targetFactory) {
            this(distributor, targetFactory, null);
        }

            /// Creates a configurator that preserves an already installed dispatcher.
        Configurator(
                final StorageBinaryDataDistributor distributor,
                final UnaryOperator<PersistenceTarget<Binary>> targetFactory,
                final InstanceDispatcherLogic previous) {
            super();
            this.distributor = notNull(distributor);
            this.targetFactory = notNull(targetFactory);
            this.previous = previous;
        }

        @SuppressWarnings("unchecked") // Store supplies the Binary persistence target to this typed factory
        @Override
        public <T> T apply(final T subject) {
            final T dispatched = this.previous == null ? subject : this.previous.apply(subject);
            return switch (dispatched) {
                case null -> null;
                case PersistenceTarget<?> target when dispatched instanceof PersistenceTypeDictionaryExporter dictionaryExporter ->
                    /* Store foundations may expose one object through both SPIs. Returning
                     * only the target decorator silently drops dictionary publication, so
                     * preserve both contracts in one adapter. */
                        (T) new TargetAndDictionaryExporter(
                                this.targetFactory.apply((PersistenceTarget<Binary>) target),
                                StorageTypeDictionaryExporterDistributing.New(dictionaryExporter, this.distributor)
                        );
                case PersistenceTarget<?> target -> (T) this.targetFactory.apply((PersistenceTarget<Binary>) target);
                case PersistenceTypeDictionaryExporter persistenceTypeDictionaryExporter -> (T) StorageTypeDictionaryExporterDistributing.New(
                        persistenceTypeDictionaryExporter,
                        this.distributor
                );
                default -> dispatched;
            };

        }

            /// Combines the two Store extension contracts when one subject implements both.
        private record TargetAndDictionaryExporter(PersistenceTarget<Binary> target, PersistenceTypeDictionaryExporter dictionaryExporter)
                    implements PersistenceTarget<Binary>, PersistenceTypeDictionaryExporter {

            private TargetAndDictionaryExporter {
                notNull(target);
                notNull(dictionaryExporter);
            }

            @Override
            public void write(final Binary data) {
                this.target.write(data);
            }

            @Override
            public boolean isWritable() {
                return this.target.isWritable();
            }

            @Override
            public void exportTypeDictionary(final PersistenceTypeDictionary typeDictionary) {
                this.dictionaryExporter.exportTypeDictionary(typeDictionary);
            }
        }
    }
}
