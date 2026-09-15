package peruncs.datagrid.cluster.storage.aeron.wire;

import org.agrona.concurrent.UnsafeBuffer;

import java.util.UUID;

/// Provides heap-backed envelope fixtures without exposing a heap encoder in production.
public final class AeronReplicationEnvelopeTestSupport {
    private AeronReplicationEnvelopeTestSupport() {
    }

    public static byte[] encode(
            final UUID clusterId,
            final long epoch,
            final long sequence,
            final AeronReplicationEnvelope.Kind kind,
            final int payloadLength,
            final int chunkIndex,
            final int chunkCount,
            final int chunkOffset,
            final int commitCrc32c,
            final byte[] payload
    ) {
        if (payload == null) throw new NullPointerException("payload");
        if (payload.length > AeronReplicationEnvelope.MAX_MESSAGE_LENGTH) {
            throw new IllegalArgumentException("envelope payload exceeds replication message limit");
        }
        final byte[] encoded = new byte[Math.addExact(AeronReplicationEnvelope.HEADER_LENGTH, payload.length)];
        return AeronReplicationEnvelope.withChecksumContext(
                new AeronReplicationEnvelope.ChecksumContext(),
                () -> {
                    AeronReplicationEnvelope.encode(
                            new UnsafeBuffer(encoded), 0, clusterId, epoch, sequence, kind,
                            payloadLength, chunkIndex, chunkCount, chunkOffset, commitCrc32c,
                            new UnsafeBuffer(payload), 0, payload.length);
                    return encoded;
                });
    }
}
