package peruncs.cluster.storage.index;

import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

/// Verifies reader-side index maintenance is inert on a Store that holds no vector index.
class ClusterIndexMaintenanceTest {
    @Test
    void aStoreWithoutVectorIndexesNeedsNoRebuildOrWarmup(@TempDir final Path path) {
        final EmbeddedStorageFoundation<?> foundation = EmbeddedStorage.Foundation(StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(path)).createConfiguration());
        try (EmbeddedStorageManager storage = foundation.start(new ArrayList<String>())) {
            final ClusterIndexMaintenance maintenance = new ClusterIndexMaintenance(
                    foundation.getConnectionFoundation().getTypeHandlerManager());
            assertDoesNotThrow(() -> maintenance.beforeApply(storage, new ByteBuffer[0], 0, 1_000));
            assertFalse(maintenance.afterApply(storage, 1_000), "no vector graph was invalidated");
            assertDoesNotThrow(maintenance::warmupVectorSearchGraphs);
            assertDoesNotThrow(maintenance::resetScratch);
        }
    }
}
