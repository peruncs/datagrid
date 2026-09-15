package peruncs.datagrid.cluster.storage.aeron.config;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;
import peruncs.datagrid.cluster.storage.types.ReplicationLimits;

import static org.junit.jupiter.api.Assertions.*;

/** Pins the validation that keeps writer and reader framing compatible. */
class AeronReplicationConfigurationTest {
    /** Verifies that defaults expose Aeron and Data Grid limits. */
    @Test
    void defaultsExposeAeronAndDataGridLimits() {
        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.defaults();
        assertEquals(16 * 1024 * 1024, configuration.termLength());
        assertEquals(1408, configuration.mtuLength());
        assertEquals(2 * 1024 * 1024, configuration.maxMessageLength());
        assertEquals(ReplicationDurabilityMode.ARCHIVE_FIRST, configuration.durabilityMode());
    }

    /** Verifies rejection of chunk that cannot fit one aeron message. */
    @Test
    void rejectsChunkThatCannotFitOneAeronMessage() {
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .termLength(64 * 1024)
                .chunkSize(8 * 1024)
                .build());
    }

    /** Verifies acceptance of tuned values when the invariant holds. */
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

    /** Verifies that all tunable limits are read from properties. */
    @Test
    void readsAllTunableLimitsFromProperties() {
        final java.util.Properties properties = new java.util.Properties();
        properties.setProperty(AeronReplicationConfiguration.TERM_LENGTH_PROPERTY, "1048576");
        properties.setProperty(AeronReplicationConfiguration.MTU_LENGTH_PROPERTY, "1024");
        properties.setProperty(AeronReplicationConfiguration.CHUNK_SIZE_PROPERTY, "32768");
        properties.setProperty(AeronReplicationConfiguration.MAX_TRANSACTION_BYTES_PROPERTY, "262144");
        properties.setProperty(AeronReplicationConfiguration.OFFER_TIMEOUT_NANOS_PROPERTY, "5000");
        properties.setProperty(AeronReplicationConfiguration.RECORDING_START_TIMEOUT_NANOS_PROPERTY, "6000");
        properties.setProperty(AeronReplicationConfiguration.RECORDED_POSITION_TIMEOUT_NANOS_PROPERTY, "7000");
        properties.setProperty(AeronReplicationConfiguration.RECORDING_STOP_TIMEOUT_NANOS_PROPERTY, "8000");
        properties.setProperty(AeronReplicationConfiguration.DURABILITY_MODE_PROPERTY, "enqueue-then-archive");

        final AeronReplicationConfiguration configuration = AeronReplicationConfiguration.from(properties);
        assertEquals(1024, configuration.mtuLength());
        assertEquals(32768, configuration.chunkSize());
        assertEquals(262144, configuration.maxTransactionBytes());
        assertEquals(5000, configuration.offerTimeoutNanos());
        assertEquals(6000, configuration.recordingStartTimeoutNanos());
        assertEquals(7000, configuration.recordedPositionTimeoutNanos());
        assertEquals(8000, configuration.recordingStopTimeoutNanos());
        assertEquals(ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE, configuration.durabilityMode());
    }

    /** Verifies rejection of invalid term mtu chunk and timeout values. */
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

    /** Verifies that the packet-count limit is inclusive and rejects its first overflow. */
    @Test
    void validatesMaximumPacketCount() {
        assertDoesNotThrow(() -> AeronReplicationConfiguration.builder()
                .chunkSize(1)
                .maxTransactionBytes(ReplicationLimits.MAX_PACKET_COUNT)
                .build());
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.builder()
                .chunkSize(1)
                .maxTransactionBytes(ReplicationLimits.MAX_PACKET_COUNT + 1)
                .build());
    }

    /** Verifies rejection of invalid properties before aeron starts. */
    @Test
    void rejectsInvalidPropertiesBeforeAeronStarts() {
        final java.util.Properties properties = new java.util.Properties();
        properties.setProperty(AeronReplicationConfiguration.DURABILITY_MODE_PROPERTY, "unknown");
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.from(properties));
        assertThrows(NullPointerException.class, () -> AeronReplicationConfiguration.from(null));
    }

    /** The Store API has no durable-first callback, so the mode is not part of the Aeron contract. */
    @Test
    void rejectsRemovedLocalDurableFirstMode() {
        final java.util.Properties properties = new java.util.Properties();
        properties.setProperty(AeronReplicationConfiguration.DURABILITY_MODE_PROPERTY, "local-durable-first");
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.from(properties));
    }

    /** Verifies reporting of invalid numeric properties with their key. */
    @Test
    void reportsInvalidNumericPropertiesWithTheirKey() {
        final java.util.Properties properties = new java.util.Properties();
        properties.setProperty(AeronReplicationConfiguration.MTU_LENGTH_PROPERTY, "not-a-number");
        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronReplicationConfiguration.from(properties));
        assertTrue(failure.getMessage().contains(AeronReplicationConfiguration.MTU_LENGTH_PROPERTY));
    }

    /** Unknown Aeron-prefixed settings must not be silently ignored. */
    @Test
    void rejectsUnknownAeronProperties() {
        final java.util.Properties properties = new java.util.Properties();
        properties.setProperty("eclipsestore.distribution.aeron.typo", "true");
        assertThrows(IllegalArgumentException.class, () -> AeronReplicationConfiguration.from(properties));
    }
}
