package peruncs.datagrid.cluster.storage.types;


import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageConnectionFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import peruncs.datagrid.cluster.storage.internal.DistributedStorageConfigurator;

import java.util.function.UnaryOperator;

/**
 * This utility installs distributed writing into an embedded Store foundation.
 *
 * <p>The configured foundation keeps its normal local target and type
 * dictionary, then wraps both so committed binary data and type definitions
 * reach the supplied distributor. The utility changes the foundation in place
 * and returns it for fluent setup.</p>
 */
public final class DistributedStorage {
    private DistributedStorage() {
        throw new UnsupportedOperationException();
    }

    /**
     * Adds distributed writing to an embedded storage foundation.
     *
     * @param foundation  foundation to configure
     * @param distributor destination for committed data
     * @return the configured foundation
     */
    public static EmbeddedStorageFoundation<?> configureWriting(
            final EmbeddedStorageFoundation<?> foundation,
            final StorageBinaryDataDistributor distributor
    ) {
        final EmbeddedStorageConnectionFoundation<?> connectionFoundation = foundation.getConnectionFoundation();
        connectionFoundation.setInstanceDispatcher(new DistributedStorageConfigurator(
                distributor,
                delegate -> StorageBinaryTargetDistributing.New(delegate, distributor)
        ));
        return foundation;
    }

    /**
     * Adds distributed writing with a custom local target wrapper.
     *
     * @param foundation    foundation to configure
     * @param distributor   destination for committed data
     * @param targetFactory wrapper for the local persistence target
     * @return the configured foundation
     */
    public static EmbeddedStorageFoundation<?> configureWriting(
            final EmbeddedStorageFoundation<?> foundation,
            final StorageBinaryDataDistributor distributor,
            final UnaryOperator<PersistenceTarget<Binary>> targetFactory
    ) {
        final EmbeddedStorageConnectionFoundation<?> connectionFoundation = foundation.getConnectionFoundation();
        connectionFoundation.setInstanceDispatcher(new DistributedStorageConfigurator(
                distributor, targetFactory));
        return foundation;
    }
}
