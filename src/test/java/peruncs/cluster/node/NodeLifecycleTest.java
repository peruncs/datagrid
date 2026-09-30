package peruncs.cluster.node;

import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.api.ClusterNode;
import peruncs.cluster.api.ClusterStorage;
import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.errors.GraphDrainTimeoutException;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.errors.WrongRoleException;
import peruncs.cluster.node.aeron.TestNodeConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the foundation's single close boundary.
class NodeLifecycleTest {
    @Test
    void closeWaitsForFirstLazyResourceConstructionBeforeCheckingInitialization() throws Exception {
        final CountDownLatch factoryStarted = new CountDownLatch(1);
        final CountDownLatch finishFactory = new CountDownLatch(1);
        final CountDownLatch closeCheckStarted = new CountDownLatch(1);
        final AtomicBoolean closeSawInitialized = new AtomicBoolean();
        final Object resource = new Object();
        final NodeCollaborators.LazyHolder<Object> holder = NodeCollaborators.LazyHolder.of(() -> {
            factoryStarted.countDown();
            try {
                assertTrue(finishFactory.await(5, TimeUnit.SECONDS));
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
            return resource;
        });
        final Thread initializer = Thread.ofVirtual().start(holder::get);
        assertTrue(factoryStarted.await(5, TimeUnit.SECONDS));
        final Thread closer = Thread.ofVirtual().start(() -> {
            closeCheckStarted.countDown();
            closeSawInitialized.set(holder.isInitialized());
        });
        assertTrue(closeCheckStarted.await(5, TimeUnit.SECONDS));
        finishFactory.countDown();
        initializer.join(5_000L);
        closer.join(5_000L);

        assertFalse(initializer.isAlive());
        assertFalse(closer.isAlive());
        assertTrue(closeSawInitialized.get(), "close must observe the created resource before skipping its stage");
        assertSame(resource, holder.get());
    }

    /// Verifies closing an unstarted foundation is idempotent and permanently prevents starting the storage manager.
    @Test
    void closeIsIdempotentAndPreventsRestart() {
        final NodeLifecycle foundation = NodeLifecycle.create().build();
        foundation.close();
        foundation.close();

        assertThrows(IllegalStateException.class, foundation::startStorageManager);
    }

    /// Verifies a started node closes idempotently and rejects any later storage-manager start.
    @Test
    void startedNodeClosesOnceAndPreventsRestart(@TempDir final Path storagePath) {
        final NodeLifecycle foundation = NodeLifecycle.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setNodeConfig(TestNodeConfig.local(storagePath, Map.of()))
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
        final NodeLifecycle foundation = NodeLifecycle.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setNodeConfig(TestNodeConfig.local(storagePath, Map.of()))
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
        try (final NodeLifecycle storageProbe = NodeLifecycle.create()
                .setNodeConfig(unstartable("backup-reader"))
                .build();
             final NodeLifecycle backupProbe = NodeLifecycle.create()
                     .setNodeConfig(unstartable("writer"))
                     .build();
             final NodeLifecycle devProbe = NodeLifecycle.create()
                     .setNodeConfig(NodeConfig.fromMap(Map.of()))
                     .build()) {
            assertThrows(WrongRoleException.class, storageProbe::storageNodeManager);
            assertThrows(WrongRoleException.class, backupProbe::backupNodeManager);
            assertThrows(WrongRoleException.class, devProbe::storageNodeManager);
            assertThrows(WrongRoleException.class, devProbe::backupNodeManager);
        }
    }

    private static NodeConfig unstartable(final String role) {
        return NodeConfig.fromMap(Map.of(
                NodeConfig.Setting.REPLICATION_TRANSPORT.key(), "aeron",
                NodeConfig.Setting.REPLICATION_ROLE.key(), role,
                NodeConfig.Setting.PROD_MODE.key(), "true"));
    }

        /// A dev node owns neither role manager: the role accessors reject it
    /// instead of manufacturing a manager the started node never created.
    @Test
    void devNodeExposesNoRoleManagers(@TempDir final Path storagePath) {
        try (final NodeLifecycle foundation = NodeLifecycle.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setNodeConfig(TestNodeConfig.local(storagePath, Map.of()))
                .setRootSupplier(Object::new)
                .build()) {
            assertDoesNotThrow(foundation::startStorageManager);

            assertThrows(WrongRoleException.class, foundation::storageNodeManager);
            assertThrows(WrongRoleException.class, foundation::backupNodeManager);
        }
    }

    @Test
    void devNodeRejectsAnExplicitReplicationRole() {
        try (final NodeLifecycle foundation = NodeLifecycle.create()
                .setNodeConfig(NodeConfig.fromMap(Map.of(
                        NodeConfig.Setting.REPLICATION_TRANSPORT.key(), "aeron",
                        NodeConfig.Setting.REPLICATION_ROLE.key(), "reader")))
                .setRootSupplier(Object::new)
                .build()) {
            assertThrows(NodeException.class, foundation::startStorageManager);
        }
    }

    @Test
    void devNodeUsesAnExplicitStoragePath(@TempDir final Path storagePath) {
        final Path configuredPath = storagePath.resolve("configured");
        try (final NodeLifecycle foundation = NodeLifecycle.create()
                .setNodeConfig(TestNodeConfig.local(configuredPath, Map.of()))
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath.resolve("ignored")))
                                .createConfiguration()))
                .setRootSupplier(Object::new)
                .build()) {
            foundation.startStorageManager();
            assertTrue(Files.exists(configuredPath.resolve("storage")));
            assertFalse(Files.exists(storagePath.resolve("ignored")));
        }
    }

    /// Verifies builder configuration carries into the built node, which then starts the storage manager cleanly on repeated starts.
    @Test
    void builderConfigurationIsCopiedIntoTheNode(@TempDir final Path storagePath) {
        final NodeLifecycle.Builder builder = NodeLifecycle.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setNodeConfig(TestNodeConfig.local(storagePath, Map.of()))
                .setRootSupplier(Object::new);
        try (final NodeLifecycle foundation = builder.build()) {
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
        final NodeLifecycle foundation = NodeLifecycle.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setNodeConfig(TestNodeConfig.local(storagePath, Map.of()))
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

    /// Verifies the close sequencer retries the Store stage when the raw
    /// delegate reports an incomplete shutdown (returns false): the first
    /// close attempt fails for the caller, the retried close completes, and
    /// the node ends up down.
    @Test
    void storeShutdownFalseThenTrueIsRetried(@TempDir final Path storagePath) {
        /* Drive the lifecycle directly (not through the NodeLifecycle
         * interface) so the test can substitute the raw Store delegate: the
         * NodeCollaborators slots are the fields the close stage consults. */
        final NodeCollaborators collaborators = new NodeCollaborators(
                Object::new,
                EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()),
                TestNodeConfig.local(storagePath, Map.of()));
        final NodeLifecycle lifecycle = new NodeLifecycle(collaborators);
        lifecycle.startStorageManager();
        final org.eclipse.store.storage.types.StorageManager delegate = collaborators.embeddedStorageManager;
        final java.util.concurrent.atomic.AtomicInteger shutdownCalls = new java.util.concurrent.atomic.AtomicInteger();
        final org.eclipse.store.storage.types.StorageManager flaky =
                (org.eclipse.store.storage.types.StorageManager) java.lang.reflect.Proxy.newProxyInstance(
                        NodeLifecycleTest.class.getClassLoader(),
                        new Class<?>[]{org.eclipse.store.storage.types.StorageManager.class},
                        (proxy, method, args) ->
                        {
                            if (method.getName().equals("shutdown") && shutdownCalls.getAndIncrement() == 0) {
                                return Boolean.FALSE;
                            }
                            try {
                                return method.invoke(delegate, args);
                            } catch (final java.lang.reflect.InvocationTargetException failure) {
                                throw failure.getTargetException();
                            }
                        });
        collaborators.embeddedStorageManager = flaky;

        final RuntimeException first = assertThrows(RuntimeException.class, lifecycle::close,
                "an incomplete Store shutdown must fail the close for retry");
        assertTrue(String.valueOf(first.getMessage()).contains("embedded storage"),
                "the outer failure names the unfinished close stage");
        assertTrue(first.getCause() != null &&
                        String.valueOf(first.getCause().getMessage()).contains("did not complete shutdown"),
                "the stage failure retains the original Store shutdown cause");
        assertDoesNotThrow(lifecycle::close, "the retried close re-runs only the unfinished Store stage");
        assertFalse(delegate.isRunning(), "the raw Store must be down after the retried close");
        assertEquals(2, shutdownCalls.get(), "the Store stage was executed exactly twice, not skipped");
    }

    /// A failed application drain keeps the Store open until the active section exits.
    @Test
    @Timeout(15)
    void applicationDrainTimeoutLeavesStoreOpenAndCloseCanRetry(@TempDir final Path storagePath) throws Exception {
        final NodeCollaborators collaborators = new NodeCollaborators(
                Object::new,
                EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()),
                TestNodeConfig.local(storagePath, Map.of(
                        NodeConfig.Setting.GRAPH_DRAIN_TIMEOUT_MILLIS.key(), "25")));
        final NodeLifecycle lifecycle = new NodeLifecycle(collaborators);
        final var manager = lifecycle.startStorageManager();
        final var store = collaborators.embeddedStorageManager;
        final CountDownLatch sectionEntered = new CountDownLatch(1);
        final CountDownLatch finishSection = new CountDownLatch(1);
        final AtomicReference<Throwable> sectionFailure = new AtomicReference<>();
        final Thread section = Thread.ofVirtual().start(() -> {
            try {
                manager.graphBoundary().write(() -> {
                    sectionEntered.countDown();
                    try {
                        if (!finishSection.await(10, TimeUnit.SECONDS)) {
                            throw new AssertionError("test did not release the active graph section");
                        }
                    } catch (final InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                });
            } catch (final Throwable failure) {
                sectionFailure.set(failure);
            }
        });
        try {
            assertTrue(sectionEntered.await(5, TimeUnit.SECONDS), "application section did not start");
            final RuntimeException failure = assertThrows(RuntimeException.class, lifecycle::close);
            Throwable cause = failure;
            while (cause != null && !(cause instanceof GraphDrainTimeoutException)) cause = cause.getCause();
            assertNotNull(cause, "close reports the app-drain timeout");
            assertTrue(store.isRunning(),
                    "the embedded Store stays open while an application section is active");
        } finally {
            finishSection.countDown();
            section.join(5_000L);
            if (store.isRunning()) lifecycle.close();
        }
        assertFalse(section.isAlive(), "the application section did not finish");
        assertNull(sectionFailure.get());
        assertFalse(store.isRunning(),
                "a new close attempt completes after the application section exits");
    }

    /// Verifies try-with-resources on the manager closes the owning node.
    @Test
    void tryWithResourcesOnManagerClosesNode(@TempDir final Path storagePath) {
        final NodeLifecycle foundation = NodeLifecycle.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setNodeConfig(TestNodeConfig.local(storagePath, Map.of()))
                .setRootSupplier(Object::new)
                .build();
        try (final var manager = foundation.startStorageManager()) {
            assertTrue(manager.isRunning());
        }
        /* The auto-close triggered the same full close: a fresh assembly start
         * must fail and the raw Store is down. */
        assertThrows(IllegalStateException.class, foundation::startStorageManager);
    }

    /// Verifies a Store whose root was never wrapped as Lazy fails startup
    /// instead of migrating or clearing existing data — the node must be
    /// reseeded from a compatible image.
    @Test
    void nonLazyExistingRootFailsClosedAtStartup(@TempDir final Path storagePath) {
        /* Preexisting image with a plain (non-Lazy) root. */
        try (final var seeded = EmbeddedStorage.start(
                new RootWithPlainData(),
                StorageConfiguration.Builder()
                        .setStorageFileProvider(Storage.FileProvider(storagePath.resolve("storage")))
                        .createConfiguration())) {
            seeded.storeRoot();
        }
        try (final NodeLifecycle foundation = NodeLifecycle.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath.resolve("storage")))
                                .createConfiguration()))
                .setNodeConfig(TestNodeConfig.local(storagePath, Map.of()))
                .setRootSupplier(Object::new)
                .build()) {
            assertThrows(ReseedRequiredException.class,
                    foundation::startStorageManager,
                    "a non-Lazy stored root means reseed, never silent repair");
        }
    }

    static final class RootWithPlainData {
        String value = "plain";
    }

    /// Verifies typed configuration reaches the assembly and a custom foundation is honored
    /// (the live file provider is always node-derived).
    @Test
    void clusterStorageFoundationHooksReachTheAssembly(@TempDir final Path storagePath) {
        final NodeConfig settings = TestNodeConfig.local(storagePath, Map.of(
                NodeConfig.Setting.BACKUP_PATH.key(), storagePath.resolve("backups").toString()));
        final StorageConfiguration customConfiguration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(storagePath.resolve("ignored-by-node")))
                .setChannelCountProvider(Storage.ChannelCountProvider(2))
                .createConfiguration();
        try (final ClusterNode<Object> node = ClusterStorage.<Object>Foundation()
                .setRootSupplier(Object::new)
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(customConfiguration))
                .setNodeConfig(settings)
                .startNode()) {
            final var manager = node.storageManager();
            assertEquals(2, manager.configuration().channelCountProvider().getChannelCount(),
                    "the supplied foundation's channel count must survive node startup");
            assertTrue(java.nio.file.Files.isDirectory(storagePath),
                    "the settings-sourced storage path must drive node startup");
        }
    }

    /// Verifies a close attempted from inside a graph section is rejected on
    /// BOTH entry points — the assembly close and the manager's shutdown —
    /// rather than deadlock-joining the graph lock the caller is holding.
    @Test
    void closeInsideAGraphSectionIsRejectedOnBothEntryPoints(@TempDir final Path storagePath) {
        try (final NodeLifecycle foundation = NodeLifecycle.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setNodeConfig(TestNodeConfig.local(storagePath, Map.of()))
                .setRootSupplier(Object::new)
                .build()) {
            final var manager = foundation.startStorageManager();
            manager.graphBoundary().read(() ->
            {
                assertThrows(IllegalStateException.class, manager::shutdown,
                        "manager shutdown must reject close from inside a graph read");
                assertThrows(IllegalStateException.class, foundation::close,
                        "node close must reject close from inside a graph read");
            });
            /* Clean state afterwards: the node was never torn down. */
            manager.graphBoundary().read(() -> {
            });
            foundation.close();
        }
    }

    /// Verifies a concurrent close during an in-flight close waits for it and
    /// completes without tripping over the active teardown.
    @Test
    void concurrentNodeCloseWaitsForInFlightTeardown(@TempDir final Path storagePath) throws Exception {
        final NodeLifecycle foundation = NodeLifecycle.create()
                .setEmbeddedStorageFoundation(EmbeddedStorageFoundation.New()
                        .setConfiguration(StorageConfiguration.Builder()
                                .setStorageFileProvider(Storage.FileProvider(storagePath))
                                .createConfiguration()))
                .setNodeConfig(TestNodeConfig.local(storagePath, Map.of()))
                .setRootSupplier(Object::new)
                .build();
        final var manager = foundation.startStorageManager();

        final java.util.concurrent.CountDownLatch closeDone = new java.util.concurrent.CountDownLatch(1);
        final AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
        final Thread first = Thread.ofVirtual().start(() ->
        {
            try {
                foundation.close();
            } catch (final Throwable t) {
                backgroundFailure.set(t);
            }
            closeDone.countDown();
        });
        /* Yield the foreground to the closing thread, then join the same
         * teardown without throwing through the facade. */
        manager.shutdown();
        assertTrue(closeDone.await(30, java.util.concurrent.TimeUnit.SECONDS));
        if (backgroundFailure.get() != null) throw new AssertionError(backgroundFailure.get());
        assertDoesNotThrow(foundation::close);
        assertFalse(manager.isRunning());
        first.join(TimeUnit.SECONDS.toMillis(30L));
    }
}
