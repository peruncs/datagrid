package peruncs.datagrid.cache.aeron;

import java.util.UUID;

/// Immutable Aeron configuration for clustered-cache invalidation.
///
/// The embedding application builds and injects this value; the Aeron core
/// never parses property maps, files, or environment variables. The Hibernate
/// adapter translates its setting map into this record once, at the framework
/// boundary.
public record AeronClusteredCacheConfiguration(
        /// Aeron channel shared by all participants. Defaults to `aeron:ipc`,
        /// which is single-host; multi-host deployments must configure a UDP
        /// channel with `control-mode=dynamic`.
        String channel,
        /// Aeron stream id shared by all participants.
        int streamId,
        /// Optional node identity shared by every provider of one node.
        /// When `null`, each provider uses a random identity.
        UUID nodeId,
        /// Optional Aeron driver directory; `null` uses Aeron's default.
        String directory,
        /// Whether to launch a private embedded MediaDriver.
        boolean embeddedDriver,
        /// How long the Aeron client waits for a driver connection, in millis.
        long driverTimeoutMillis,
        /// How long a sender waits for the publication to accept a frame
        /// before failing the cache write, in millis.
        long offerTimeoutMillis,
        /// Largest accepted serialized payload, in bytes.
        int maxPayloadBytes
) {
        /// Default single-host channel.
    public static final String DEFAULT_CHANNEL = "aeron:ipc";
        /// Default stream id.
    public static final int DEFAULT_STREAM_ID = 2001;
        /// Default driver-connection wait in millis.
    public static final long DEFAULT_DRIVER_TIMEOUT_MILLIS = 10_000L;
        /// Default publication wait in millis.
    public static final long DEFAULT_OFFER_TIMEOUT_MILLIS = 5_000L;
        /// Default payload limit in bytes.
    public static final int DEFAULT_MAX_PAYLOAD_BYTES = 1 << 20;
        /// Absolute payload ceiling imposed by frame framing.
    public static final int MAX_PAYLOAD_BYTES =
            Integer.MAX_VALUE - AeronClusteredCacheMessageCodec.HEADER_LENGTH -
            AeronClusteredCacheMessageCodec.CRC_LENGTH;

    /// Compact constructor validating format-independent value ranges.
    public AeronClusteredCacheConfiguration {
        if (channel == null || channel.isBlank()) {
            throw new IllegalArgumentException("channel must not be blank");
        }
        if (streamId < 0) {
            throw new IllegalArgumentException("streamId must not be negative: %s".formatted(streamId));
        }
        if (nodeId != null && nodeId.equals(new UUID(0L, 0L))) {
            throw new IllegalArgumentException("the zero UUID is not a valid node id");
        }
        if (driverTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "driverTimeoutMillis must be positive: %s".formatted(driverTimeoutMillis));
        }
        if (offerTimeoutMillis < 0) {
            throw new IllegalArgumentException(
                    "offerTimeoutMillis must not be negative: %s".formatted(offerTimeoutMillis));
        }
        try {
            Math.multiplyExact(offerTimeoutMillis, 1_000_000L);
        } catch (final ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "offerTimeoutMillis is too large: %s".formatted(offerTimeoutMillis), overflow);
        }
        if (maxPayloadBytes < 1 || maxPayloadBytes > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                    "maxPayloadBytes must be between 1 and %s".formatted(MAX_PAYLOAD_BYTES));
        }
    }

        /// Returns the default single-host configuration.
    ///
    /// @return default configuration
    public static AeronClusteredCacheConfiguration defaults() {
        return new AeronClusteredCacheConfiguration(
                DEFAULT_CHANNEL, DEFAULT_STREAM_ID, null, null, false,
                DEFAULT_DRIVER_TIMEOUT_MILLIS, DEFAULT_OFFER_TIMEOUT_MILLIS, DEFAULT_MAX_PAYLOAD_BYTES);
    }

        /// Returns the publication wait in nanoseconds.
    ///
    /// @return wait in nanoseconds
    public long offerTimeoutNanos() {
        return this.offerTimeoutMillis * 1_000_000L;
    }
}
