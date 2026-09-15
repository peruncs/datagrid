package peruncs.datagrid.cluster.node.replication;

import java.util.HexFormat;
import java.util.UUID;

/// Durable Aeron replication position.
///
/// `logicalSequence` is the Data Grid ordering value. The opaque
/// `providerPosition` is interpreted only by the Aeron transport (for
/// example an Aeron recording id/position pair), stored as lowercase hex so
/// the record is deeply immutable; an empty string carries no position.
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
        if (!providerPosition.isEmpty()) {
            try {
                HexFormat.of().parseHex(providerPosition);
            } catch (final IllegalArgumentException notHex) {
                throw new IllegalArgumentException("providerPosition must be even-length lowercase hex", notHex);
            }
        }
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
        return new ReplicationCursor(transport, storeGeneration, logicalSequence,
                providerPosition == null ? "" : HexFormat.of().formatHex(providerPosition));
    }

        /// Reports whether the cursor carries provider state.
    ///
    /// @return `true` when a provider position is present
    public boolean hasProviderPosition() {
        return !this.providerPosition.isEmpty();
    }

        /// Decodes the hex position back to bytes for provider codecs.
    ///
    /// @return provider position bytes, empty when absent
    public byte[] providerPositionBytes() {
        return this.providerPosition.isEmpty()
                ? new byte[0]
                : HexFormat.of().parseHex(this.providerPosition);
    }
}
