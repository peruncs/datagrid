package peruncs.datagrid.cluster.storage.aeron.config;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;

import static org.junit.jupiter.api.Assertions.*;

/// Pins the validation that keeps writer and reader framing compatible.
class AeronReplicationConfigurationTest {
        /// Verifies that defaults expose Aeron and Data Grid limits.
    @Test
    void defaultsExposeAeronAndDataGridLimits() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.defaults();
        assertEquals(16 * 1024 * 1024, configuration.termLength());
        assertEquals(1408, configuration.mtuLength());
        assertEquals(2 * 1024 * 1024, configuration.maxMessageLength());
        assertEquals(ReplicationDurabilityMode.ARCHIVE_FIRST, configuration.durabilityMode());
        assertEquals(256, configuration.readerFragmentsPerPoll(),
                "a replay backlog must drain in a few polls rather than ten fragments at a time");
    }

        /// Verifies the reader fragment limit is configurable and must be positive.
    @Test
    void readerFragmentLimitIsConfigurableAndValidated() {
        final AeronReplicationConfiguration tuned = AeronReplicationConfiguration.builder()
                .readerFragmentsPerPoll(4096)
                .build();
        assertEquals(4096, tuned.readerFragmentsPerPoll());
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .readerFragmentsPerPoll(0).build());
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .readerFragmentsPerPoll(-1).build());
    }

        /// Verifies the validated record is value-based: equal limits are equal objects.
    @Test
    void equalLimitsAreEqualRecords() {
        final AeronReplicationConfiguration first = AeronReplicationConfiguration.builder()
                .chunkSize(32768).readerFragmentsPerPoll(512).build();
        final AeronReplicationConfiguration second = AeronReplicationConfiguration.builder()
                .chunkSize(32768).readerFragmentsPerPoll(512).build();
        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

        /// Verifies rejection of chunk that cannot fit one aeron message.
    @Test
    void rejectsChunkThatCannotFitOneAeronMessage() {
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .termLength(64 * 1024)
                .chunkSize(8 * 1024)
                .build());
    }

        /// Verifies acceptance of tuned values when the invariant holds.
    @Test
    void acceptsTunedValuesWhenTheInvariantHolds() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(32 * 1024 * 1024)
                .mtuLength(8192)
                .chunkSize(256 * 1024)
                .maxTransactionBytes(16 * 1024 * 1024)
                .build();
        assertEquals(4 * 1024 * 1024, configuration.maxMessageLength());
    }

        /// Verifies that all tunable limits are set through the typed builder.
    @Test
    void readsAllTunableLimitsFromTheBuilder() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .termLength(1048576)
                .mtuLength(1024)
                .chunkSize(32768)
                .maxTransactionBytes(262144)
                .offerTimeoutNanos(5000)
                .recordingStartTimeoutNanos(6000)
                .recordedPositionTimeoutNanos(7000)
                .recordingStopTimeoutNanos(8000)
                .durabilityMode(ReplicationDurabilityMode.ARCHIVE_FIRST)
                .build();
        assertEquals(1024, configuration.mtuLength());
        assertEquals(32768, configuration.chunkSize());
        assertEquals(262144, configuration.maxTransactionBytes());
        assertEquals(5000, configuration.offerTimeoutNanos());
        assertEquals(6000, configuration.recordingStartTimeoutNanos());
        assertEquals(7000, configuration.recordedPositionTimeoutNanos());
        assertEquals(8000, configuration.recordingStopTimeoutNanos());
        assertEquals(ReplicationDurabilityMode.ARCHIVE_FIRST, configuration.durabilityMode());
    }

        /// Verifies rejection of invalid term mtu chunk and timeout values.
    @Test
    void rejectsInvalidTermMtuChunkAndTimeoutValues() {
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .termLength(1000).build());
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .mtuLength(511).build());
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .chunkSize(0).build());
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .offerTimeoutNanos(0).build());
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .recordingStartTimeoutNanos(0).build());
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .recordedPositionTimeoutNanos(0).build());
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .recordingStopTimeoutNanos(0).build());
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .maxTransactionBytes(0).build());
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .maxTransactionBytes(1024 * 1024 * 1024 + 1).build());
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .termLength(1 << 30).chunkSize(20 * 1024 * 1024).maxTransactionBytes(20 * 1024 * 1024).build());
    }

        /// Verifies that the packet-count limit is inclusive and rejects its first overflow.
    @Test
    void validatesMaximumPacketCount() {
        assertDoesNotThrow(() -> AeronReplicationConfiguration.builder()
                .chunkSize(1)
                .maxTransactionBytes(AeronReplicationEnvelope.MAX_PACKET_COUNT)
                .build());
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .chunkSize(1)
                .maxTransactionBytes(AeronReplicationEnvelope.MAX_PACKET_COUNT + 1)
                .build());
    }

        /// Verifies rejection of a null durability mode through the builder.
    @Test
    void rejectsNullDurabilityMode() {
        assertThrows(NullPointerException.class, () -> AeronReplicationConfiguration.builder()
                .durabilityMode(null));
    }

        /// Verifies a custom retry policy is carried into the built configuration
        /// and drives the reader idle strategy.
    @Test
    void propagatesCustomRetryPolicy() {
        final AeronRetryPolicy custom = new AeronRetryPolicy(
                4, 8, 2L, 500_000L,
                2_000L, 2_000_000L,
                5_000_000L,
                500_000L, 50_000_000L);

        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.builder()
                .retryPolicy(custom)
                .build();

        assertSame(custom, configuration.retryPolicy());
        assertInstanceOf(org.agrona.concurrent.BackoffIdleStrategy.class, configuration.retryPolicy().idleStrategy());
    }

}
