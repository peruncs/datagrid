package peruncs.datagrid.cluster.node;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies the foundation's single close boundary.
class ClusterFoundationLifecycleTest {
    @Test
    void closeIsIdempotentAndPreventsRestart() throws Exception {
        final ClusterFoundation foundation = ClusterFoundation.New().build();
        foundation.close();
        foundation.close();

        assertThrows(IllegalStateException.class, foundation::startStorageManager);
    }

    @Test
    void builderConfigurationIsCopiedIntoTheNode(@TempDir final Path storagePath) throws Exception {
        final ClusterFoundation.Builder builder = ClusterFoundation.New()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setRootSupplier(Object::new);
        builder.setEnableAsyncDistribution(true);

        try (final ClusterFoundation foundation = builder.build()) {
            assertDoesNotThrow(foundation::startStorageManager);
            assertDoesNotThrow(foundation::startStorageManager);
        }
    }
}
