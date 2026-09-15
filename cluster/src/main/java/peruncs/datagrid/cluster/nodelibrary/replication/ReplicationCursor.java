package peruncs.datagrid.cluster.nodelibrary.replication;

import java.util.Arrays;
import java.util.UUID;

/// Durable Aeron replication position.
///
/// `logicalSequence` is the Data Grid ordering value. The opaque
/// `providerPosition` is interpreted only by the Aeron transport (for
/// example an Aeron recording id/position pair).
///
/// @param transport        selected provider id
/// @param storeGeneration  immutable Store image identity, or `null` when the provider has none
/// @param logicalSequence  last fully resolved transaction, or `-1` before the first one
/// @param providerPosition provider-specific position bytes
public record ReplicationCursor(
        String transport,
        UUID storeGeneration,
        long logicalSequence,
        byte[] providerPosition) {
        /// Validates and copies the provider position.
    ///
    /// @param transport        selected provider id
    /// @param storeGeneration  Store generation
    /// @param logicalSequence  last resolved transaction
    /// @param providerPosition provider position bytes
    public ReplicationCursor {
        if (transport == null || transport.isBlank()) {
            throw new IllegalArgumentException("transport must not be blank");
        }
        if (logicalSequence < -1) {
            throw new IllegalArgumentException("logicalSequence must be >= -1");
        }
        providerPosition = providerPosition == null ? new byte[0] : providerPosition.clone();
    }

        /// Returns a copy of the provider position.
    ///
    /// @return provider position copy
    public byte[] providerPosition() {
        return this.providerPosition.clone();
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ReplicationCursor cursor)) {
            return false;
        }
        return this.logicalSequence == cursor.logicalSequence
               && java.util.Objects.equals(this.transport, cursor.transport)
               && java.util.Objects.equals(this.storeGeneration, cursor.storeGeneration)
               && Arrays.equals(this.providerPosition, cursor.providerPosition);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(this.transport, this.storeGeneration, this.logicalSequence,
                Arrays.hashCode(this.providerPosition));
    }
}
