package peruncs.datagrid.cache.aeron;

import java.util.Arrays;
import java.util.UUID;

/// Immutable Aeron configuration for clustered-cache invalidation.
///
/// The embedding application builds and injects this value; the Aeron core
/// never parses property maps, files, or environment variables. The Hibernate
/// adapter translates its setting map into this record once, at the framework
/// boundary.
///
/// @param channel                 Aeron channel shared by all participants; defaults to `aeron:ipc`,
///                                which is single-host, so multi-host deployments must configure
///                                a UDP channel with `control-mode=dynamic`
/// @param streamId                Aeron stream id shared by all participants
/// @param nodeId                  optional node identity shared by every provider of one node;
///                                when `null`, each provider uses a random identity
/// @param directory               optional Aeron driver directory; `null` uses Aeron's default
/// @param embeddedDriver          whether to launch a private embedded MediaDriver
/// @param driverTimeoutMillis     how long the Aeron client waits for a driver connection, in millis
/// @param offerTimeoutMillis      how long a sender waits for the publication to accept a frame
///                                before failing the cache write, in millis; the same budget
///                                bounds the wait for the shared per-node sequence lock
/// @param maxPayloadBytes         largest accepted serialized payload, in bytes
/// @param heartbeatIntervalMillis how often an idle sender publishes a payload-less heartbeat,
///                                in millis; heartbeats consume the per-sender sequence so a
///                                receiver can tell a quiet sender from a lost one
/// @param freshnessTimeoutMillis  how long a receiver tolerates total silence before marking
///                                itself stale, in millis; must exceed the heartbeat interval.
///                                A stale receiver fails closed and requires re-synchronization
/// @param expectedRemoteSenders   how many distinct remote senders must prove liveness with a
///                                post-start frame before the receiver reports healthy, `0`
///                                disables the quorum. A volatile broadcast cannot name a
///                                peer it has never heard from, so without a quorum a fresh
///                                receiver serves cache reads while partitioned from a peer
///                                whose first frame never arrived. Multi-node deployments
///                                must set this to the number of remote peers (cluster
///                                size minus one); single-node deployments keep `0`
/// @param cursorDirectory         directory holding the persisted per-sender cursors; `null`
///                                selects a subdirectory of the JVM temporary directory.
///                                A blank value is normalized to `null`
/// @param hmacSecret                copied HMAC-SHA256 key authenticating every cache frame, or `null`
///                                for unsigned frames. Unsigned frames carry only a CRC32C, which
///                                detects corruption but never forgery: any peer that can publish
///                                on the channel can poison or deny the cache. Configure a secret
///                                on every node, or explicitly acknowledge the risk.
/// @param productionMode            whether production-only validation is enabled. In production
///                                mode unsigned frames are rejected unless `allowUnsignedFrames`
///                                explicitly acknowledges them, and loopback channels are rejected
///                                even with an embedded driver
/// @param allowUnsignedFrames       explicit acknowledgement that unsigned frames are acceptable.
///                                Only meaningful without a secret; combining it with a secret is
///                                rejected as contradictory. Required in production mode when no
///                                secret is configured
/// @param previousHmacSecret        retiring HMAC-SHA256 key accepted during rotation overlap, or
///                                `null`. Verification tries the primary key first and falls back
///                                to this key; signing never uses it
public record AeronClusteredCacheConfiguration(
        String channel,
        int streamId,
        UUID nodeId,
        String directory,
        boolean embeddedDriver,
        long driverTimeoutMillis,
        long offerTimeoutMillis,
        int maxPayloadBytes,
        long heartbeatIntervalMillis,
        long freshnessTimeoutMillis,
        int expectedRemoteSenders,
        String cursorDirectory,
        byte[] hmacSecret,
        boolean productionMode,
        boolean allowUnsignedFrames,
        byte[] previousHmacSecret
) {
        /// Creates a configuration with no rotation overlap.
    ///
    /// Every argument behaves as in the canonical constructor; the previous
    /// HMAC key is `null`.
    public AeronClusteredCacheConfiguration(
            final String channel,
            final int streamId,
            final UUID nodeId,
            final String directory,
            final boolean embeddedDriver,
            final long driverTimeoutMillis,
            final long offerTimeoutMillis,
            final int maxPayloadBytes,
            final long heartbeatIntervalMillis,
            final long freshnessTimeoutMillis,
            final String cursorDirectory,
            final byte[] hmacSecret,
            final boolean productionMode,
            final boolean allowUnsignedFrames
    ) {
        this(channel, streamId, nodeId, directory, embeddedDriver, driverTimeoutMillis, offerTimeoutMillis,
                maxPayloadBytes, heartbeatIntervalMillis, freshnessTimeoutMillis, 0, cursorDirectory, hmacSecret,
                productionMode, allowUnsignedFrames, null);
    }

        /// Creates a configuration with no remote-sender quorum.
    ///
    /// Every argument behaves as in the canonical constructor; no remote
    /// sender must prove liveness first.
    public AeronClusteredCacheConfiguration(
            final String channel,
            final int streamId,
            final UUID nodeId,
            final String directory,
            final boolean embeddedDriver,
            final long driverTimeoutMillis,
            final long offerTimeoutMillis,
            final int maxPayloadBytes,
            final long heartbeatIntervalMillis,
            final long freshnessTimeoutMillis,
            final String cursorDirectory,
            final byte[] hmacSecret,
            final boolean productionMode,
            final boolean allowUnsignedFrames,
            final byte[] previousHmacSecret
    ) {
        this(channel, streamId, nodeId, directory, embeddedDriver, driverTimeoutMillis, offerTimeoutMillis,
                maxPayloadBytes, heartbeatIntervalMillis, freshnessTimeoutMillis, 0, cursorDirectory, hmacSecret,
                productionMode, allowUnsignedFrames, previousHmacSecret);
    }

        /// Minimum HMAC secret length in bytes.
    public static final int MIN_HMAC_SECRET_BYTES = 16;
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
        /// Default idle-sender heartbeat interval in millis.
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 500L;
        /// Default receiver silence tolerance in millis.
    public static final long DEFAULT_FRESHNESS_TIMEOUT_MILLIS = 5_000L;
        /// Default remote-sender quorum: disabled for single-node deployments.
    public static final int DEFAULT_EXPECTED_REMOTE_SENDERS = 0;
        /// Default cursor directory; `null` selects a JVM-temporary subdirectory.
    public static final String DEFAULT_CURSOR_DIRECTORY = null;
        /// Absolute payload ceiling imposed by frame framing.
    public static final int MAX_PAYLOAD_BYTES =
            Integer.MAX_VALUE - AeronClusteredCacheMessageCodec.HEADER_LENGTH -
            AeronClusteredCacheMessageCodec.CRC_LENGTH;

    /// Compact constructor validating format-independent value ranges.
    public AeronClusteredCacheConfiguration {
        if (cursorDirectory != null && cursorDirectory.isBlank()) {
            cursorDirectory = null;
        }
        hmacSecret = hmacSecret == null ? null : hmacSecret.clone();
        previousHmacSecret = previousHmacSecret == null ? null : previousHmacSecret.clone();
        if (hmacSecret != null && hmacSecret.length < MIN_HMAC_SECRET_BYTES) {
            throw new IllegalArgumentException(
                    "hmacSecret must contain at least %s bytes".formatted(MIN_HMAC_SECRET_BYTES));
        }
        if (previousHmacSecret != null && hmacSecret == null) {
            throw new IllegalArgumentException("previousHmacSecret requires a configured hmacSecret");
        }
        if (previousHmacSecret != null && previousHmacSecret.length < MIN_HMAC_SECRET_BYTES) {
            throw new IllegalArgumentException(
                    "previousHmacSecret must contain at least %s bytes".formatted(MIN_HMAC_SECRET_BYTES));
        }
        if (previousHmacSecret != null && Arrays.equals(previousHmacSecret, hmacSecret)) {
            throw new IllegalArgumentException(
                    "previousHmacSecret must differ from hmacSecret; a rotation to the same key is a misconfiguration");
        }
        if ((hmacSecret != null || previousHmacSecret != null) && allowUnsignedFrames) {
            throw new IllegalArgumentException(
                    "allowUnsignedFrames contradicts a configured hmacSecret; remove the acknowledgement");
        }
        if (hmacSecret == null && productionMode && !allowUnsignedFrames) {
            throw new IllegalArgumentException(
                    "production mode requires an hmacSecret or an explicit allowUnsignedFrames acknowledgement");
        }
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
        final int authenticatedMaxPayloadBytes = MAX_PAYLOAD_BYTES -
                (hmacSecret == null ? 0 : AeronClusteredCacheMessageCodec.HMAC_LENGTH);
        if (maxPayloadBytes < 1 || maxPayloadBytes > authenticatedMaxPayloadBytes) {
            throw new IllegalArgumentException(
                    "maxPayloadBytes must be between 1 and %s".formatted(authenticatedMaxPayloadBytes));
        }
        if (heartbeatIntervalMillis < 1) {
            throw new IllegalArgumentException(
                    "heartbeatIntervalMillis must be positive: %s".formatted(heartbeatIntervalMillis));
        }
        if (heartbeatIntervalMillis > Long.MAX_VALUE / 1_000_000L) {
            throw new IllegalArgumentException(
                    "heartbeatIntervalMillis is too large: %s".formatted(heartbeatIntervalMillis));
        }
        if (freshnessTimeoutMillis <= heartbeatIntervalMillis) {
            throw new IllegalArgumentException(
                    "freshnessTimeoutMillis (%s) must exceed heartbeatIntervalMillis (%s) so one missed beat cannot mark a live receiver stale".formatted(freshnessTimeoutMillis, heartbeatIntervalMillis));
        }
        if (freshnessTimeoutMillis > Long.MAX_VALUE / 1_000_000L) {
            throw new IllegalArgumentException(
                    "freshnessTimeoutMillis is too large: %s".formatted(freshnessTimeoutMillis));
        }
        if (expectedRemoteSenders < 0) {
            throw new IllegalArgumentException(
                    "expectedRemoteSenders must not be negative: %s".formatted(expectedRemoteSenders));
        }
    }

        /// Returns the default single-host configuration.
    ///
    /// @return default configuration
    public static AeronClusteredCacheConfiguration defaults() {
        return new AeronClusteredCacheConfiguration(
                DEFAULT_CHANNEL, DEFAULT_STREAM_ID, null, null, false,
                DEFAULT_DRIVER_TIMEOUT_MILLIS, DEFAULT_OFFER_TIMEOUT_MILLIS, DEFAULT_MAX_PAYLOAD_BYTES,
                DEFAULT_HEARTBEAT_INTERVAL_MILLIS, DEFAULT_FRESHNESS_TIMEOUT_MILLIS,
                DEFAULT_EXPECTED_REMOTE_SENDERS, DEFAULT_CURSOR_DIRECTORY,
                null, false, false, null);
    }

        /// Returns a copy of the HMAC secret, or `null` when frames are unsigned.
    ///
    /// @return copied secret, or `null`
    @Override
    public byte[] hmacSecret() {
        return this.hmacSecret == null ? null : this.hmacSecret.clone();
    }

        /// Returns a copy of the retiring HMAC secret accepted during rotation overlap.
    ///
    /// @return copied previous secret, or `null`
    @Override
    public byte[] previousHmacSecret() {
        return this.previousHmacSecret == null ? null : this.previousHmacSecret.clone();
    }

        /// Returns whether any frame authentication is configured.
    ///
    /// @return `true` when an HMAC secret is configured
    public boolean authenticated() {
        return this.hmacSecret != null;
    }

        /// Returns the publication wait in nanoseconds.
    ///
    /// @return wait in nanoseconds
    public long offerTimeoutNanos() {
        return this.offerTimeoutMillis * 1_000_000L;
    }

        /// Returns the idle-sender heartbeat interval in nanoseconds.
    ///
    /// @return interval in nanoseconds
    public long heartbeatIntervalNanos() {
        return this.heartbeatIntervalMillis * 1_000_000L;
    }

        /// Returns the receiver silence tolerance in nanoseconds.
    ///
    /// @return timeout in nanoseconds
    public long freshnessTimeoutNanos() {
        return this.freshnessTimeoutMillis * 1_000_000L;
    }
}
