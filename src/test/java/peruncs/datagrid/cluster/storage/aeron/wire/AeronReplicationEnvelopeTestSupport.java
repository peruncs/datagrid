package peruncs.datagrid.cluster.storage.aeron.wire;

import org.agrona.concurrent.UnsafeBuffer;

import java.util.Objects;
import java.util.UUID;

/// Provides heap-backed envelope fixtures without exposing a heap encoder in production.
public final class AeronReplicationEnvelopeTestSupport {
    private AeronReplicationEnvelopeTestSupport() {
    }

    /// Encodes one envelope frame into a fresh heap array for test fixtures.
    ///
    /// @param clusterId     cluster identity carried by the frame
    /// @param epoch         writer epoch carried by the frame
    /// @param fencingToken  writer fencing token carried by the frame
    /// @param sequence      transaction sequence carried by the frame
    /// @param kind          frame kind (data, dictionary, commit, or abort)
    /// @param payloadLength logical transaction bytes, not the bytes in this frame
    /// @param chunkIndex    index of this chunk within the transaction
    /// @param chunkCount    total chunks of the transaction
    /// @param chunkOffset   logical offset of this chunk within the transaction
    /// @param commitCrc32c  CRC recorded by commit markers, ignored otherwise
    /// @param payload       wire bytes carried by this frame
    /// @return complete encoded frame including the fixed header
    public static byte[] encode(
            final UUID clusterId,
            final long epoch,
            final long fencingToken,
            final long sequence,
            final AeronReplicationEnvelope.Kind kind,
            final int payloadLength,
            final int chunkIndex,
            final int chunkCount,
            final int chunkOffset,
            final int commitCrc32c,
            final byte[] payload
    ) {
        Objects.requireNonNull(payload, "payload");
        if (payload.length > AeronReplicationEnvelope.MAX_TRANSACTION_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("envelope payload exceeds replication message limit");
        }
        final byte[] encoded = new byte[Math.addExact(AeronReplicationEnvelope.HEADER_LENGTH, payload.length)];
        return AeronReplicationEnvelope.withChecksumContext(
                new AeronReplicationEnvelope.ChecksumContext(),
                () -> {
                    AeronReplicationEnvelope.encode(
                            new UnsafeBuffer(encoded), 0, clusterId, epoch, fencingToken, sequence, kind,
                            payloadLength, chunkIndex, chunkCount, chunkOffset, commitCrc32c,
                            new UnsafeBuffer(payload), 0, payload.length);
                    return encoded;
                });
    }
}
