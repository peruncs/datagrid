package peruncs.cluster.storage.index;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTypeHandlerManager;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;

/// Creates an isolated Serializer manager for package-level index validation tests.
public final class ClusterIndexTestSupport {
    private ClusterIndexTestSupport() {
    }

    /// Creates an isolated Store type-handler manager for validation tests.
    public static PersistenceTypeHandlerManager<Binary> typeHandlers() {
        return EmbeddedStorageFoundation.New().getConnectionFoundation().getTypeHandlerManager();
    }
}
