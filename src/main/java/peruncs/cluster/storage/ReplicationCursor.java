package peruncs.cluster.storage;

import peruncs.cluster.storage.binary.ReplicationApplier;

import java.util.HexFormat;
import java.util.Locale;
import java.util.UUID;

/// Names a node's durable place in the replication stream.
///
/// `logicalSequence` is the Data Grid ordering value. The opaque
/// `providerPosition` is interpreted only by the transport (for example an
/// Aeron recording id/position pair), stored as lowercase hex so the record
/// is deeply immutable; an empty string carries no position.
///
/// This type lives in the storage contract package because the reader port
/// ([ReplicationApplier]) publishes it; the node layer consumes it.
///
/// @param transport        selected provider id
/// @param storeGeneration  immutable Store image identity, or `null` when the provider has none
/// @param logicalSequence  last fully resolved transaction, or `-1` before the first one
/// @param providerPosition provider-specific position bytes, lowercase hex
public record ReplicationCursor(
        String transport,
        UUID storeGeneration,
        long logicalSequence,
        String providerPosition) {
    /* Shared codec: HexFormat is immutable and thread-safe. A single instance
     * serves every cursor instead of allocating one per format and parse. */
    private static final HexFormat HEX = HexFormat.of();

    /// Cursor for a node with replication disabled.
    public static final ReplicationCursor NONE = new ReplicationCursor("none", null, -1, "");

        /// Validates the cursor fields.
    ///
    /// @param transport        selected provider id
    /// @param storeGeneration  Store generation
    /// @param logicalSequence  last resolved transaction
    /// @param providerPosition provider position bytes, lowercase hex
    public ReplicationCursor {
        if (transport == null || transport.isBlank()) {
            throw new IllegalArgumentException("transport must not be blank");
        }
        if (logicalSequence < -1) {
            throw new IllegalArgumentException("logicalSequence must be >= -1");
        }
        if (providerPosition == null) providerPosition = "";
        /* Allocation-free validity scan. The per-message path builds cursors
         * from freshly encoded bytes whose hex is valid by construction; fully
         * decoding it here just to throw the bytes away would allocate on
         * every applied message. Uppercase input is accepted but normalized
         * to lowercase so identical positions always compare equal as strings. */
        if (!providerPosition.isEmpty() && !isHex(providerPosition)) {
            throw new IllegalArgumentException("providerPosition must be even-length hex");
        }
        providerPosition = providerPosition.toLowerCase(Locale.ROOT);
    }

        /// Creates a cursor from raw provider position bytes.
    ///
    /// @param transport        selected provider id
    /// @param storeGeneration  Store generation
    /// @param logicalSequence  last resolved transaction
    /// @param providerPosition provider position bytes, or `null` for none
    /// @return cursor with a hex-encoded position
    public static ReplicationCursor of(
            final String transport,
            final UUID storeGeneration,
            final long logicalSequence,
            final byte[] providerPosition
    ) {
        return new ReplicationCursor(transport, storeGeneration, logicalSequence, providerPosition == null ? "" : HEX.formatHex(providerPosition));
    }

        /// Reports whether the text is even-length hexadecimal.
    ///
    /// @param value candidate hex text
    /// @return `true` for even-length hex, including uppercase
    private static boolean isHex(final String value) {
        if ((value.length() & 1) != 0) return false;
        for (int index = 0; index < value.length(); index++) {
            final char current = value.charAt(index);
            final boolean digit = current >= '0' && current <= '9';
            final boolean lower = current >= 'a' && current <= 'f';
            final boolean upper = current >= 'A' && current <= 'F';
            if (!digit && !lower && !upper) return false;
        }
        return true;
    }

        /// Reports whether the cursor carries provider state.
    ///
    /// @return `true` when a provider position is present
    public boolean hasProviderPosition() {
        return !this.providerPosition.isEmpty();
    }

        /// Decodes the hex position back to bytes for provider codecs.
    ///
    /// Decoding allocates; callers that decode the same cursor repeatedly
    /// should decode once and reuse the array.
    ///
    /// @return provider position bytes, empty when absent
    public byte[] providerPositionBytes() {
        return this.providerPosition.isEmpty()
                ? new byte[0]
                : HEX.parseHex(this.providerPosition);
    }

        /// Returns the provider position size in bytes without decoding it.
    ///
    /// @return provider position byte length
    public int providerPositionByteLength() {
        return this.providerPosition.length() / 2;
    }
}
