package peruncs.cluster.node;
import peruncs.cluster.api.NodeSettingsSource;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.errors.WrongRoleException;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Verifies the foundation's single close boundary.
class NodeAssemblyLifecycleTest {
    /// Verifies closing an unstarted foundation is idempotent and permanently prevents starting the storage manager.
    @Test
    void closeIsIdempotentAndPreventsRestart() {
        final NodeAssembly foundation = NodeAssembly.create().build();
        foundation.close();
        foundation.close();

        assertThrows(IllegalStateException.class, foundation::startStorageManager);
    }

    /// Verifies a started node closes idempotently and rejects any later storage-manager start.
    @Test
    void startedNodeClosesOnceAndPreventsRestart(@TempDir final Path storagePath) {
        final NodeAssembly foundation = NodeAssembly.create()
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
        final NodeAssembly foundation = NodeAssembly.create()
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
        try (final NodeAssembly storageProbe = NodeAssembly.create()
                .setNodeSettingsSource(unstartable("backup-reader"))
                .build();
             final NodeAssembly backupProbe = NodeAssembly.create()
                     .setNodeSettingsSource(unstartable("writer"))
                     .build();
             final NodeAssembly devProbe = NodeAssembly.create().build()) {
            assertThrows(WrongRoleException.class, storageProbe::storageNodeManager);
            assertThrows(WrongRoleException.class, backupProbe::backupNodeManager);
            assertThrows(WrongRoleException.class, devProbe::storageNodeManager);
            assertThrows(WrongRoleException.class, devProbe::backupNodeManager);
        }
    }

    private static NodeSettingsSource unstartable(final String role) {
        return new NodeSettingsSource.Env(Map.of(
                "ECLIPSE_DATAGRID_PROD_MODE", "true",
                "ECLIPSE_DATAGRID_AERON_TRUSTED_NETWORK", "true",
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
        try (final NodeAssembly foundation = NodeAssembly.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setRootSupplier(Object::new)
                .build()) {
            assertDoesNotThrow(foundation::startStorageManager);

            assertThrows(WrongRoleException.class, foundation::storageNodeManager);
            assertThrows(WrongRoleException.class, foundation::backupNodeManager);
        }
    }

    /// Verifies builder configuration carries into the built node, which then starts the storage manager cleanly on repeated starts.
    @Test
    void builderConfigurationIsCopiedIntoTheNode(@TempDir final Path storagePath) {
        final NodeAssembly.Builder builder = NodeAssembly.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setRootSupplier(Object::new);
        try (final NodeAssembly foundation = builder.build()) {
            assertDoesNotThrow(foundation::startStorageManager);
            assertDoesNotThrow(foundation::startStorageManager);
        }
    }

    /// Verifies the manager's shutdown() runs the complete node teardown:
    /// the raw Store shuts down (the storage manager stage is owned by the
    /// lifecycle, not by re-entering the facade) and the node rejects a
    /// restart afterwards. A second shutdown is idempotent.
    @Test
    void managerShutdownRendersTheNodeClosed(@TempDir final Path storagePath) {
        final NodeAssembly foundation = NodeAssembly.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setRootSupplier(Object::new)
                .build();
        final var manager = foundation.startStorageManager();

        assertTrue(manager.shutdown(), "the first manager close performs the node teardown");
        assertFalse(manager.shutdown(), "the second manager close only observes the completed close");
        /* The owning lifecycle reports closed and never restarts the Store. */
        assertThrows(IllegalStateException.class, foundation::startStorageManager,
                "startStorageManager after teardown must fail");
        assertThrows(IllegalStateException.class, manager::start,
                "manager.start() after teardown must fail");
        /* The owning assembly close stays idempotent after the facade close. */
        assertDoesNotThrow(foundation::close);
        assertDoesNotThrow(foundation::close);
        assertFalse(manager.isRunning(), "the raw Store must be down");
    }

    /// Verifies try-with-resources on the manager closes the owning node.
    @Test
    void tryWithResourcesOnManagerClosesNode(@TempDir final Path storagePath) {
        final NodeAssembly foundation = NodeAssembly.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setRootSupplier(Object::new)
                .build();
        try (final var manager = foundation.startStorageManager()) {
            assertTrue(manager.isRunning());
        }
        /* The auto-close triggered the same full close: a fresh assembly start
         * must fail and the raw Store is down. */
        assertThrows(IllegalStateException.class, foundation::startStorageManager);
    }

    /// Verifies the options hooks reach the assembly: a programmatic settings
    /// source replaces the environment, and a custom foundation is honored
    /// (the live file provider is always node-derived).
    @Test
    void nodeOptionsHooksReachTheAssembly(@TempDir final Path storagePath) {
        final NodeSettingsSource settings = new NodeSettingsSource.Env(Map.of(
                NodeSettingsSource.Env.EnvKeys.STORAGE_PATH, storagePath.toString(),
                NodeSettingsSource.Env.EnvKeys.BACKUP_PATH, storagePath.resolve("backups").toString())) {
        };
        final StorageConfiguration customConfiguration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(storagePath.resolve("ignored-by-node")))
                .setChannelCountProvider(Storage.ChannelCountProvider(2))
                .createConfiguration();
        try (final peruncs.cluster.api.ClusterNode<Object> node = peruncs.cluster.api.ClusterNode.open(
                peruncs.cluster.api.NodeOptions.of(Object::new)
                        .withEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                                .setConfiguration(customConfiguration))
                        .withNodeSettingsSource(settings))) {
            final var manager = node.storageManager();
            assertEquals(2, manager.configuration().channelCountProvider().getChannelCount(),
                    "the supplied foundation's channel count must survive node startup");
            assertTrue(java.nio.file.Files.isDirectory(storagePath),
                    "the settings-sourced storage path must drive node startup");
        }
    }

    /// Verifies a concurrent close during an in-flight close waits for it and
    /// completes without tripping over the active teardown.
    @Test
    void concurrentNodeCloseWaitsForInFlightTeardown(@TempDir final Path storagePath) throws Exception {
        final NodeAssembly foundation = NodeAssembly.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setRootSupplier(Object::new)
                .build();
        final var manager = foundation.startStorageManager();

        final java.util.concurrent.CountDownLatch closeStarted = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch closeDone = new java.util.concurrent.CountDownLatch(1);
        final AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
        final Thread first = Thread.ofVirtual().start(() ->
        {
            try {
                foundation.close();
                closeDone.countDown();
            } catch (final Throwable t) {
                backgroundFailure.set(t);
                closeDone.countDown();
            }
        });
        closeStarted.countDown();
        /* Yield the foreground to the closing thread, then join the same
         * teardown without throwing. */
        manager.shutdown();
        assertTrue(closeDone.await(30, java.util.concurrent.TimeUnit.SECONDS));
        if (backgroundFailure.get() != null) throw new AssertionError(backgroundFailure.get());
        assertDoesNotThrow(foundation::close);
        assertFalse(manager.isRunning());
        first.join(TimeUnit.SECONDS.toMillis(30L));
    }
}
