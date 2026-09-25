package peruncs.cluster.storage.aeron.writer;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.junit.jupiter.api.Test;
import peruncs.cluster.storage.aeron.checkpoint.AeronReplicationCursor;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.reader.ReplicationApplierAeron;
import peruncs.cluster.storage.binary.StorageBinaryDataReceiver;

import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies live UDP delivery, reconnect, and multi-buffer transactions.
class AeronUdpReplicationIT {
    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void await(final Check check) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (!check.value()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("timed out waiting for Aeron state");
            }
            LockSupport.parkNanos(1_000_000L);
        }
    }

        /// Verifies fragments large transaction and delivers after commit.
    @Test
    void fragmentsLargeTransactionAndDeliversAfterCommit() throws Exception {
        final int port = freePort();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(1024 * 1024)
                .mtuLength(1024)
                .chunkSize(32 * 1024)
                .maxTransactionBytes(256 * 1024)
                .offerTimeoutNanos(10_000_000_000L)
                .build();
        final String channel = "aeron:udp?endpoint=localhost:%s|term-length=1048576|mtu=1024".formatted(port);
        final UUID clusterId = UUID.randomUUID();
        final RecordingReceiver receiver = new RecordingReceiver();

        try (MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
             ExclusivePublication publication = aeron.addExclusivePublication(channel, 1001);
             Subscription subscription = aeron.addSubscription(channel, 1001)) {
            await(() -> publication.isConnected() && subscription.isConnected());
            final ReplicationApplierAeron client = new ReplicationApplierAeron(
                    subscription, configuration, clusterId, 1, -1, receiver
            );
            client.start();
            final byte[] data = new byte[100_000];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (i * 31);
            }
            final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                    AeronReplicationPublisher.onPublication(publication, configuration, clusterId, 1, 0)
            );
            final ByteBuffer first = XMemory.toDirectByteBuffer(Arrays.copyOfRange(data, 0, 37_000));
            final ByteBuffer second = XMemory.toDirectByteBuffer(Arrays.copyOfRange(data, 37_000, data.length));
            final int firstPosition = first.position();
            final int secondPosition = second.position();
            final boolean[] localAccepted = {false};
            coordinator.distributeTypeDictionary("class=example.Type");
            final PersistenceTarget<Binary> localTarget = new PersistenceTarget<>() {
                @Override
                public void write(final Binary value) {
                    localAccepted[0] = true;
                }

                @Override
                public boolean isWritable() {
                    return true;
                }
            };
            receiver.localAccepted = () -> localAccepted[0];
            AeronStorageBinaryReplicationTarget.create(localTarget, coordinator)
                    .write(ChunksWrapper.New(first, second));
            assertEquals(firstPosition, first.position());
            assertEquals(secondPosition, second.position());

            await(() -> client.lastResolvedSequence() == 0);
            assertEquals("class=example.Type", receiver.dictionary);
            assertArrayEquals(data, receiver.data);
            assertFalse(receiver.observedBeforeLocal);
            final AeronReplicationCursor cursor = client.cursor(UUID.randomUUID(), UUID.randomUUID(), 11);
            assertEquals(0, cursor.sequence());
            assertEquals(11, cursor.recordingId());
            if (cursor.recordingPosition() < 0) {
                throw new AssertionError("Aeron header position was not captured");
            }
            assertNull(client.failure());
            client.dispose();
        }
    }

        /// Verifies local enqueue failure publishes abort and reader does not apply.
    @Test
    void localEnqueueFailurePublishesAbortAndReaderDoesNotApply() throws Exception {
        final int port = freePort();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(1024 * 1024)
                .mtuLength(1024)
                .chunkSize(32 * 1024)
                .maxTransactionBytes(64 * 1024)
                .offerTimeoutNanos(10_000_000_000L)
                .build();
        final String channel = "aeron:udp?endpoint=localhost:%s|term-length=1048576|mtu=1024".formatted(port);
        final UUID clusterId = UUID.randomUUID();
        final RecordingReceiver receiver = new RecordingReceiver();

        try (MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
             ExclusivePublication publication = aeron.addExclusivePublication(channel, 1002);
             Subscription subscription = aeron.addSubscription(channel, 1002)) {
            await(() -> publication.isConnected() && subscription.isConnected());
            final ReplicationApplierAeron client = new ReplicationApplierAeron(
                    subscription, configuration, clusterId, 1, -1, receiver
            );
            client.start();
            final AeronReplicationWriteCoordinator coordinator = new AeronReplicationWriteCoordinator(
                    AeronReplicationPublisher.onPublication(publication, configuration, clusterId, 1, 0)
            );
            final PersistenceTarget<Binary> failingTarget = new PersistenceTarget<>() {
                public void write(final Binary value) {
                    throw new IllegalStateException("injected local rejection");
                }

                public boolean isWritable() {
                    return true;
                }
            };
            final PersistenceTarget<Binary> target = AeronStorageBinaryReplicationTarget.create(failingTarget, coordinator);
            assertThrows(IllegalStateException.class,
                    () -> target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{4, 5, 6}))));
            await(() -> client.lastResolvedSequence() == 0 || client.failure() != null);
            assertEquals(0, client.lastResolvedSequence());
            assertNull(receiver.data);
            assertNull(client.failure());
            client.dispose();
        }
    }

        /// Verifies dynamic MDC uses max flow control and reconnects.
    @Test
    void dynamicMdcUsesMaxFlowControlAndReconnects() throws Exception {
        final int controlPort = freePort();
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(1024 * 1024).mtuLength(1024).chunkSize(16 * 1024).maxTransactionBytes(128 * 1024)
                .offerTimeoutNanos(10_000_000_000L).build();
        final String publicationChannel = "aeron:udp?control=localhost:%s|control-mode=dynamic|fc=max|term-length=1048576|mtu=1024".formatted(controlPort);
        final String subscriptionChannel = "aeron:udp?control=localhost:%s|control-mode=dynamic|fc=max|term-length=1048576|mtu=1024".formatted(controlPort);
        final UUID clusterId = UUID.randomUUID();
        final RecordingReceiver receiver = new RecordingReceiver();
        try (MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
             ExclusivePublication publication = aeron.addExclusivePublication(publicationChannel, 1101);
             Subscription subscription = aeron.addSubscription(subscriptionChannel, 1101)) {
            await(() -> publication.isConnected() && subscription.isConnected());
            final ReplicationApplierAeron client = new ReplicationApplierAeron(
                    subscription, configuration, clusterId, 5, -1, receiver);
            client.start();
            final AeronReplicationPublisher publisher = AeronReplicationPublisher.onPublication(
                    publication, configuration, clusterId, 5, 0);
            publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{9, 8, 7})});
            await(() -> client.lastResolvedSequence() == 0);
            assertArrayEquals(new byte[]{9, 8, 7}, receiver.data);
            client.dispose();
            final RecordingReceiver reconnectedReceiver = new RecordingReceiver();
            try (Subscription reconnectedSubscription = aeron.addSubscription(subscriptionChannel, 1101)) {
                await(() -> publication.isConnected() && reconnectedSubscription.isConnected());
                final ReplicationApplierAeron reconnected = new ReplicationApplierAeron(
                        reconnectedSubscription, configuration, clusterId, 5, 0, reconnectedReceiver);
                reconnected.start();
                publisher.publishTransaction(null, new ByteBuffer[]{ByteBuffer.wrap(new byte[]{6, 6, 6})});
                await(() -> reconnected.lastResolvedSequence() == 1 || reconnected.failure() != null);
                if (reconnected.failure() != null) {
                    throw reconnected.failure();
                }
                assertArrayEquals(new byte[]{6, 6, 6}, reconnectedReceiver.data);
                reconnected.dispose();
            }
            publisher.close();
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean value();
    }

    private static final class RecordingReceiver implements StorageBinaryDataReceiver {
        private volatile String dictionary;
        private volatile byte[] data;
        private volatile boolean observedBeforeLocal;
        private BooleanSupplier localAccepted = () -> true;

        @Override
        public void receiveData(final Binary value) {
            this.observedBeforeLocal = !this.localAccepted.getAsBoolean();
            final ByteBuffer source = value.buffers()[0].duplicate();
            source.flip();
            final byte[] bytes = new byte[source.remaining()];
            source.get(bytes);
            this.data = bytes;
        }

        @Override
        public void receiveTypeDictionary(final String value) {
            this.dictionary = value;
        }
    }
}
