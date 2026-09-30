package peruncs.cluster.node.aeron;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.api.ClusterNode;
import peruncs.cluster.api.ClusterStorage;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.errors.GraphDrainTimeoutException;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.storage.io.FaultInjection;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the node close sequencer drains a writer parked after its Aeron prepare.
class AeronApplicationSectionDrainIT {
    @Test
    @Timeout(60)
    void closeWaitsForPreparedWriteAndRestartResumesWithoutReseed(@TempDir final Path directory)
            throws InterruptedException {
        final Path storagePath = directory.resolve("storage");
        final UUID clusterId = UUID.randomUUID();
        final UUID nodeId = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final NodeConfig settings = settings(directory, storagePath, clusterId, nodeId, generation,
                Duration.ofMillis(100));
        final var options = ClusterStorage.<AeronStoreIntegrationIT.Root>Foundation()
                .setRootSupplier(AeronStoreIntegrationIT.Root::new)
                .setNodeConfig(settings);
        final CountDownLatch prepared = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicReference<Throwable> writeFailure = new AtomicReference<>();
        final AtomicReference<Thread> writer = new AtomicReference<>();

        try (ClusterNode<AeronStoreIntegrationIT.Root> node = options.startNode()) {
            final ClusterStorageManager<AeronStoreIntegrationIT.Root> manager = node.storageManager();
            final AeronStoreIntegrationIT.Root root = manager.graphBoundary().read(() -> manager.root().get());
            manager.storeRoot();

            FaultInjection.runWithHook((name, sequence, path) -> {
                if ("AFTER_PREPARE_BEFORE_LOCAL_WRITE".equals(name)) await(prepared, release);
            }, () -> writer.set(Thread.ofVirtual().start(FaultInjection.inheritCurrent(() -> {
                try {
                    manager.graphBoundary().write(() -> {
                        root.values.add("prepared-write");
                        manager.store(root.values);
                    });
                } catch (final Throwable failure) {
                    writeFailure.set(failure);
                }
            }))));

            try {
                assertTrue(prepared.await(10, TimeUnit.SECONDS), "writer did not reach the post-prepare hook");
                final NodeException closeFailure = assertThrows(NodeException.class, node::close);
                assertInstanceOf(GraphDrainTimeoutException.class, closeFailure.getCause());
                assertTrue(manager.isRunning(), "drain timeout must leave the Store open");
            } finally {
                release.countDown();
                writer.get().join(10_000L);
            }

            assertFalse(writer.get().isAlive());
            assertNull(writeFailure.get());
            assertTrue(root.values.contains("prepared-write"),
                    "the prepared transaction must finish after the close attempt times out");
            node.close();
            assertFalse(manager.isRunning(), "the retry must finish the Store close");
        }

        try (ClusterNode<AeronStoreIntegrationIT.Root> restarted = options.startNode()) {
            assertTrue(restarted.storageManager().graphBoundary().read(
                            () -> restarted.storageManager().root().get().values.contains("prepared-write")),
                    "restart must resume the committed Store without reseeding");
        }
    }

    private static NodeConfig settings(final Path directory, final Path storagePath,
                                       final UUID clusterId, final UUID nodeId,
                                       final UUID generation, final Duration closeTimeout) {
        final NodeConfig base = AeronStoreIntegrationIT.properties(directory, clusterId, nodeId, generation);
        final NodeConfig.Timeouts current = base.timeouts();
        final NodeConfig.Timeouts timeouts = new NodeConfig.Timeouts(
                closeTimeout, current.mergerCache(), current.applyBudget(), current.offer(),
                current.recordingStart(), current.recordedPosition(), current.recordingStop(), current.readerStop(),
                current.reconnect(), current.archiveControl(), current.watermarkClose(), current.driver());
        final NodeConfig.StorageConfig currentStorage = base.storage();
        final NodeConfig.StorageConfig storage = new NodeConfig.StorageConfig(storagePath,
                currentStorage.limitBytes(), Duration.ofMinutes(1), currentStorage.gcInterval());
        final NodeConfig.AeronConfig currentAeron = base.aeron();
        final NodeConfig.Channels channels = currentAeron.channels();
        final String host = localClusterAddress();
        final NodeConfig.Channels routableChannels = new NodeConfig.Channels(
                channels.live().replace("localhost", host), channels.replay().replace("localhost", host),
                channels.archiveReplication().replace("localhost", host),
                channels.watermark().replace("localhost", host), channels.control().replace("localhost", host),
                channels.controlResponse().replace("localhost", host));
        final NodeConfig.AeronConfig aeron = new NodeConfig.AeronConfig(currentAeron.clusterId(),
                currentAeron.nodeId(), currentAeron.storeGeneration(), currentAeron.epoch(), currentAeron.streamId(),
                currentAeron.recordingId(), currentAeron.watermarkStreamId(), currentAeron.retentionInterval(),
                routableChannels, currentAeron.directories(), currentAeron.archivePolicy(),
                currentAeron.threadingMode());
        return new NodeConfig(base.role(), storage, base.backup(), timeouts, base.limits(), aeron,
                true, base.replicationTransport(), base.operations());
    }

    private static String localClusterAddress() {
        try {
            return NetworkInterface.networkInterfaces()
                    .flatMap(NetworkInterface::inetAddresses)
                    .filter(address -> address instanceof Inet4Address && !address.isLoopbackAddress() &&
                            !address.isLinkLocalAddress())
                    .map(InetAddress::getHostAddress)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("a routable IPv4 interface is required"));
        } catch (final SocketException failure) {
            throw new IllegalStateException("cannot inspect local network interfaces", failure);
        }
    }

    private static void await(final CountDownLatch entered, final CountDownLatch release) {
        entered.countDown();
        try {
            if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("prepared Store write was not released");
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }
}
