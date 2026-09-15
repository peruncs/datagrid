package peruncs.datagrid.cluster.node.aeron;

import io.aeron.Aeron;
import io.aeron.Subscription;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.NodelibraryPropertiesProvider;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.node.replication.ClusterStorageBinaryDataDistributor;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpointStore;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Verifies provider restart uses the Archive position in the writer checkpoint.
class AeronProviderCheckpointTest {
    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

        /// Verifies persistence target writes committed writer checkpoint.
    @Test
    void persistenceTargetWritesCommittedWriterCheckpoint() throws Exception {
        final Path directory = Files.createTempDirectory("datagrid-aeron-provider-");
        final Path archive = directory.resolveSibling("%s.archive".formatted(directory.getFileName()));
        final Path checkpointDirectory = directory.resolveSibling("%s.checkpoint".formatted(directory.getFileName()));
        final Path checkpoint = checkpointDirectory.resolve("writer.checkpoint");
        final int controlPort = freePort();
        final int livePort = freePort();
        final String clusterId = UUID.randomUUID().toString();
        final NodelibraryPropertiesProvider properties = new NodelibraryPropertiesProvider.Env() {
            @Override
            public String replicationRole() {
                return "writer";
            }

            @Override
            public boolean replicationRoleConfigured() {
                return true;
            }

            @Override
            public String replicationProperty(final String name) {
                return switch (name) {
                    case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID" -> clusterId;
                    case "ECLIPSE_DATAGRID_AERON_NODE_ID" -> UUID.nameUUIDFromBytes(directory.toString().getBytes()).toString();
                    case "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> UUID.nameUUIDFromBytes(
                            ("generation:%s".formatted(directory)).getBytes()).toString();
                    case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> directory.toString();
                    case "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY" -> archive.toString();
                    case "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH" -> checkpoint.toString();
                    case "ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL" -> "aeron:udp?control=localhost:%s|control-mode=dynamic|fc=max".formatted(livePort);
                    case "ECLIPSE_DATAGRID_AERON_CONTROL_CHANNEL" -> "aeron:udp?endpoint=localhost:%s".formatted(controlPort);
                    case "ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL",
                         "ECLIPSE_DATAGRID_AERON_CONTROL_RESPONSE_CHANNEL" -> "aeron:udp?endpoint=localhost:0";
                    case "ECLIPSE_DATAGRID_AERON_TERM_LENGTH" -> "1048576";
                    case "ECLIPSE_DATAGRID_AERON_MTU_LENGTH" -> "1024";
                    case "ECLIPSE_DATAGRID_AERON_CHUNK_SIZE" -> "16384";
                    case "ECLIPSE_DATAGRID_AERON_MAX_TRANSACTION_BYTES" -> "262144";
                    default -> null;
                };
            }
        };
        ClusterReplicationTransport transport = new AeronClusterReplicationTransportProvider().create(properties);
        final ClusterStorageBinaryDataDistributor distributor = transport.distributor("stream", false);
        final PersistenceTarget<Binary> target = transport.persistenceTargetFactory("stream", distributor)
                .apply(new PersistenceTarget<>() {
                    @Override
                    public void write(final Binary ignored) {
                    }

                    @Override
                    public boolean isWritable() {
                        return true;
                    }
                });
        try {
            target.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{1, 2, 3})));
            final AeronReplicationCheckpoint saved = AeronReplicationCheckpointStore.read(checkpoint);
            assertEquals(AeronReplicationCheckpoint.State.COMMITTED, saved.state());
            assertEquals(0, saved.transactionSequence());
            assertTrue(saved.recordingId() >= 0);
            transport.close();
            transport = new AeronClusterReplicationTransportProvider().create(properties);
            final ClusterStorageBinaryDataDistributor resumedDistributor = transport.distributor("stream", false);
            final PersistenceTarget<Binary> resumedTarget = transport.persistenceTargetFactory("stream", resumedDistributor)
                    .apply(new PersistenceTarget<>() {
                        @Override
                        public void write(final Binary ignored) {
                        }

                        @Override
                        public boolean isWritable() {
                            return true;
                        }
                    });
            final AtomicInteger receivedFrames = new AtomicInteger();
            try (Aeron subscriberAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(directory.toString()));
                 final Subscription subscriber = subscriberAeron.addSubscription(
                         "aeron:udp?endpoint=localhost:0|control=localhost:%s|control-mode=dynamic".formatted(livePort), 1001)) {
                final long connectDeadline = System.nanoTime() + 10_000_000_000L;
                while (!subscriber.isConnected() && System.nanoTime() < connectDeadline) {
                    LockSupport.parkNanos(1_000_000L);
                }
                assertTrue(subscriber.isConnected(), "dynamic-MDC subscriber did not connect");
                resumedTarget.write(ChunksWrapper.New(XMemory.toDirectByteBuffer(new byte[]{4, 5})));
                final long receiveDeadline = System.nanoTime() + 10_000_000_000L;
                while (receivedFrames.get() == 0 && System.nanoTime() < receiveDeadline) {
                    subscriber.poll((buffer, offset, length, header) -> receivedFrames.incrementAndGet(), 10);
                    LockSupport.parkNanos(1_000_000L);
                }
            }
            assertTrue(receivedFrames.get() > 0, "dynamic-MDC subscriber received no published envelopes");
            final AeronReplicationCheckpoint resumed = AeronReplicationCheckpointStore.read(checkpoint);
            assertEquals(AeronReplicationCheckpoint.State.COMMITTED, resumed.state());
            assertEquals(1, resumed.transactionSequence());
            assertEquals(saved.recordingId(), resumed.recordingId());
        } finally {
            transport.close();
            try (var paths = Files.walk(directory)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path ->
                {
                    try {
                        Files.deleteIfExists(path);
                    } catch (final Exception ignored) {
                    }
                });
            }
            Files.deleteIfExists(checkpoint);
            if (Files.exists(checkpointDirectory)) {
                try (var paths = Files.walk(checkpointDirectory)) {
                    paths.sorted(java.util.Comparator.reverseOrder()).forEach(path ->
                    {
                        try {
                            Files.deleteIfExists(path);
                        } catch (final Exception ignored) {
                        }
                    });
                }
            }
            if (Files.exists(archive)) {
                try (var paths = Files.walk(archive)) {
                    paths.sorted(java.util.Comparator.reverseOrder()).forEach(path ->
                    {
                        try {
                            Files.deleteIfExists(path);
                        } catch (final Exception ignored) {
                        }
                    });
                }
            }
        }
    }
}
