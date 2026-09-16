package peruncs.datagrid.cache.aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cache.types.ClusteredCacheMessageAcceptor;
import peruncs.datagrid.cache.types.TimestampsRegionUpdateMessage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

/// Proves a receiver with a configured peer quorum refuses reads until a peer proves liveness.
///
/// A volatile broadcast cannot name a peer it has never heard from, so without
/// a quorum a fresh receiver with only self traffic reports healthy while
/// partitioned from a peer whose first frame never arrives — and caches query
/// results against that unverified stream. With the quorum, the receiver stays
/// unhealthy (without failing closed, so a late peer still completes it) until
/// the expected peer's first frame, and fails closed when that peer later goes
/// silent while self traffic keeps flowing.
@Timeout(90)
class AeronClusteredCacheReceiverQuorumTest {
    private static MediaDriver launchDriver(final Path root) {
        return MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(root.resolve("driver").toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .threadingMode(ThreadingMode.SHARED));
    }

    private static AeronClusteredCacheConfiguration configuration(
            final Path driverDirectory, final Path cursorDirectory,
            final long heartbeatIntervalMillis, final long freshnessTimeoutMillis,
            final int expectedRemoteSenders) {
        return new AeronClusteredCacheConfiguration(
                "aeron:ipc", 2001, null, driverDirectory.toString(), false,
                10_000L, 10_000L, 1 << 20,
                heartbeatIntervalMillis, freshnessTimeoutMillis, expectedRemoteSenders,
                cursorDirectory.toString(), null, false, false, null);
    }

    private static void awaitConnected(final Publication publication) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!publication.isConnected() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(100_000L);
        }
        if (!publication.isConnected()) {
            throw new IllegalStateException("raw publication did not connect to the receiver subscription");
        }
    }

    private static void offer(final Publication raw, final ExpandableArrayBuffer frame,
                              final byte[] sender, final String table, final long timestamp, final long sequence) {
        final byte[] payload = AeronClusteredCachePayloadCodec.encode(
                new TimestampsRegionUpdateMessage("timestamps", table, timestamp));
        final int length = AeronClusteredCacheMessageCodec.encode(frame, sender, sequence, payload);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (raw.offer(frame, 0, length) < 0 && System.nanoTime() < deadline) {
            LockSupport.parkNanos(10_000L);
        }
    }

    private static byte[] senderId(final long high, final long low) {
        return ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.BIG_ENDIAN)
                .putLong(high).putLong(low).array();
    }

    @Test
    void partitionedPeerBeforeItsFirstFrameKeepsReadsUnverified(@TempDir final Path root) throws Exception {
        try (MediaDriver driver = launchDriver(root)) {
            final AeronClusteredCacheConfiguration properties = configuration(
                    root.resolve("driver"), root.resolve("cursors"), 50L, 800L, 1);
            final BlockingQueue<TimestampsRegionUpdateMessage> received = new ArrayBlockingQueue<>(64);
            final AeronClusteredCacheMessageCommunicationProvider provider =
                    new AeronClusteredCacheMessageCommunicationProvider();
            /* The node's own sender heartbeats throughout: self traffic keeps
             * the global freshness deadline fresh, isolating the quorum gate
             * from the total-silence tripwire. */
            provider.provideUpdateTimestampsCacheMessageSender(properties);
            final AeronClusteredCacheMessageReceiver receiver = provider.provideMessageReceiver(
                    properties, new ClusteredCacheMessageAcceptor(null) {
                        @Override
                        public void accept(final TimestampsRegionUpdateMessage message) {
                            received.add(message);
                        }
                    });
            try {
                receiver.start();
                try (Aeron aeron = Aeron.connect(new Aeron.Context()
                        .aeronDirectoryName(root.resolve("driver").toString()));
                     Publication raw = aeron.addPublication(properties.channel(), properties.streamId())) {
                    awaitConnected(raw);
                    /* Partition before the peer's first frame: only self
                     * heartbeats flow. Reads must stay refused past the
                     * freshness window, without a terminal failure — the peer
                     * may still arrive. */
                    final long partitionEnd = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1_200L);
                    while (System.nanoTime() < partitionEnd) {
                        assertFalse(receiver.isRunning(),
                                "reads must stay unverified while the expected peer never arrived");
                        assertNull(receiver.failure(),
                                "a missing peer must refuse reads, not fail the receiver closed");
                        LockSupport.parkNanos(50_000_000L);
                    }
                    assertTrue(received.isEmpty(), "no peer frame means no delivery");
                    /* The partition heals: the peer's first frame completes the
                     * quorum and verifies reads. */
                    final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(256);
                    offer(raw, frame, senderId(51L, 52L), "healed", 1L, 0L);
                    assertNotNull(received.poll(10, TimeUnit.SECONDS),
                            "the peer's first frame must be delivered");
                    final long healthyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (!receiver.isRunning() && System.nanoTime() < healthyDeadline) {
                        LockSupport.parkNanos(50_000_000L);
                    }
                    assertTrue(receiver.isRunning(), "the peer's first frame must verify reads");
                    /* The peer partitions again while self traffic flows: the
                     * now-tracked sender must fail the receiver closed instead
                     * of serving stale results. */
                    final long failureDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    while (receiver.failure() == null && System.nanoTime() < failureDeadline) {
                        LockSupport.parkNanos(50_000_000L);
                    }
                    assertNotNull(receiver.failure(), "the silent peer must fail the receiver");
                    assertTrue(receiver.failure().getMessage().contains("stale"),
                            "unexpected failure: " + receiver.failure().getMessage());
                    assertFalse(receiver.isRunning());
                }
            } finally {
                receiver.dispose();
            }
        }
    }
}
