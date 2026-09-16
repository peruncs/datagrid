package peruncs.datagrid.cache.aeron;

import java.util.UUID;

/// Immutable Aeron configuration for clustered-cache invalidation.
///
/// The embedding application builds and injects this value; the Aeron core
/// never parses property maps, files, or environment variables. The Hibernate
/// adapter translates its setting map into this record once, at the framework
/// boundary.
///
/// @param channel            Aeron channel shared by all participants; defaults to `aeron:ipc`,
///                           which is single-host, so multi-host deployments must configure
///                           a UDP channel with `control-mode=dynamic`
/// @param streamId           Aeron stream id shared by all participants
/// @param nodeId             optional node identity shared by every provider of one node;
///                           when `null`, each provider uses a random identity
/// @param directory          optional Aeron driver directory; `null` uses Aeron's default
/// @param embeddedDriver     whether to launch a private embedded MediaDriver
/// @param driverTimeoutMillis how long the Aeron client waits for a driver connection, in millis
/// @param offerTimeoutMillis how long a sender waits for the publication to accept a frame
///                           before failing the cache write, in millis; the same budget
///                           bounds the wait for the shared per-node sequence lock
/// @param maxPayloadBytes    largest accepted serialized payload, in bytes
public record AeronClusteredCacheConfiguration(
        String channel,
        int streamId,
        UUID nodeId,
        String directory,
        boolean embeddedDriver,
        long driverTimeoutMillis,
        long offerTimeoutMillis,
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
        if (offerTimeoutMillis > Long.MAX_VALUE / 1_000_000L) {
            throw new IllegalArgumentException(
                    "offerTimeoutMillis is too large: %s".formatted(offerTimeoutMillis));
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
