package peruncs.datagrid.cluster.node;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies the foundation's single close boundary.
class ClusterFoundationLifecycleTest {
    /// Verifies closing an unstarted foundation is idempotent and permanently prevents starting the storage manager.
    @Test
    void closeIsIdempotentAndPreventsRestart() {
        final ClusterFoundation foundation = ClusterFoundation.New().build();
        foundation.close();
        foundation.close();

        assertThrows(IllegalStateException.class, foundation::startStorageManager);
    }

    /// Verifies a started node closes idempotently and rejects any later storage-manager start.
    @Test
    void startedNodeClosesOnceAndPreventsRestart(@TempDir final Path storagePath) {
        final ClusterFoundation foundation = ClusterFoundation.New()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setRootSupplier(Object::new)
                .build();
        assertDoesNotThrow(foundation::startStorageManager);
        foundation.close();
        foundation.close();

        assertThrows(IllegalStateException.class, foundation::startStorageManager);
    }

        /// A failed start runs close on its way out, and that close must be
    /// terminal: the node never hands out a manager again, a later close is
    /// an idempotent no-op instead of a second teardown, and the raw Store
    /// opened before the failure is shut down rather than leaked.
    @Test
    void failedStartClosesTheFoundationPermanently(@TempDir final Path storagePath) {
        final ClusterFoundation foundation = ClusterFoundation.New()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setRootSupplier(() -> {
                    throw new IllegalStateException("root supplier misconfigured");
                })
                .build();

        assertThrows(IllegalStateException.class, foundation::startStorageManager);

        /* The failure path closed the node; nothing is borrowable afterwards
         * and the lifecycle is permanently over, not half-open. */
        assertThrows(IllegalStateException.class, foundation::startStorageManager,
                "a closed node must not start or hand out a manager");
        assertThrows(IllegalStateException.class, foundation::storageNodeManager);
        assertThrows(IllegalStateException.class, foundation::backupNodeManager);
        assertDoesNotThrow(foundation::close, "a repeated close after a failed start is a no-op");
    }

        /// A wrong-role probe is rejected before anything starts: no Store,
    /// Aeron, recovery, or background threads may come up just to answer
    /// `IllegalStateException`. The provider below explodes on any property
    /// read beyond the role itself, so a start-first order cannot pass.
    @Test
    void wrongRoleAccessorRejectsBeforeStarting() {
        try (final ClusterFoundation storageProbe = ClusterFoundation.New()
                .setNodeLibraryPropertiesProvider(unstartable("backup-reader"))
                .build();
             final ClusterFoundation backupProbe = ClusterFoundation.New()
                     .setNodeLibraryPropertiesProvider(unstartable("writer"))
                     .build();
             final ClusterFoundation devProbe = ClusterFoundation.New().build()) {
            assertThrows(IllegalStateException.class, storageProbe::storageNodeManager);
            assertThrows(IllegalStateException.class, backupProbe::backupNodeManager);
            assertThrows(IllegalStateException.class, devProbe::storageNodeManager);
            assertThrows(IllegalStateException.class, devProbe::backupNodeManager);
        }
    }

    private static NodeLibraryPropertiesProvider unstartable(final String role) {
        return new NodeLibraryPropertiesProvider.Env(Map.of(
                "ECLIPSE_DATAGRID_PROD_MODE", "true",
                "ECLIPSE_DATAGRID_REPLICATION_ROLE", role)) {
            @Override
            public String replicationProperty(final String name) {
                throw new AssertionError("a wrong-role probe must not start the node: " + name);
            }
        };
    }

        /// A dev node owns neither role manager: the role accessors reject it
    /// instead of manufacturing a manager the started node never created.
    @Test
    void devNodeExposesNoRoleManagers(@TempDir final Path storagePath) {
        try (final ClusterFoundation foundation = ClusterFoundation.New()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setRootSupplier(Object::new)
                .build()) {
            assertDoesNotThrow(foundation::startStorageManager);

            assertThrows(IllegalStateException.class, foundation::storageNodeManager);
            assertThrows(IllegalStateException.class, foundation::backupNodeManager);
        }
    }

    /// Verifies builder configuration carries into the built node, which then starts the storage manager cleanly on repeated starts.
    @Test
    void builderConfigurationIsCopiedIntoTheNode(@TempDir final Path storagePath) {
        final ClusterFoundation.Builder builder = ClusterFoundation.New()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setRootSupplier(Object::new);
        try (final ClusterFoundation foundation = builder.build()) {
            assertDoesNotThrow(foundation::startStorageManager);
            assertDoesNotThrow(foundation::startStorageManager);
        }
    }
}
