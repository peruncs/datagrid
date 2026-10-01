package peruncs.cluster.storage.aeron.writer;

import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;

import java.util.UUID;
import java.util.function.LongUnaryOperator;

/// Builds publishers driven by a test offerer instead of a real Aeron publication.
final class PublisherFixtures {
    private PublisherFixtures() {
    }

    /// Creates a publisher whose recorded-position wait acknowledges every position immediately.
    ///
    /// @param offerer          publication attempt sink
    /// @param maxMessageLength publication message capacity
    /// @param configuration    framing, retry, and timeout limits
    /// @param clusterId        replication cluster identity
    /// @param epoch            writer epoch bound to the Store mark
    /// @param initialSequence  first sequence to publish
    /// @return publisher without an owned publication
    static AeronReplicationPublisher forTests(final AeronOfferRetryer.Offerer offerer, final int maxMessageLength,
                                              final AeronReplicationConfiguration configuration, final UUID clusterId,
                                              final long epoch, final long initialSequence) {
        return forTests(offerer, maxMessageLength, configuration, clusterId, epoch, initialSequence,
                LongUnaryOperator.identity());
    }

    /// Creates a publisher with an explicit recorded-position acknowledgement.
    ///
    /// @param commitPositionAwaiter waits for the Archive to record a position
    /// @return publisher without an owned publication
    static AeronReplicationPublisher forTests(final AeronOfferRetryer.Offerer offerer, final int maxMessageLength,
                                              final AeronReplicationConfiguration configuration, final UUID clusterId,
                                              final long epoch, final long initialSequence,
                                              final LongUnaryOperator commitPositionAwaiter) {
        return new AeronReplicationPublisher(new AeronReplicationPublisher.Configuration(
                offerer, maxMessageLength, configuration, clusterId, epoch, initialSequence, null,
                (position, timeoutNanos) -> commitPositionAwaiter.applyAsLong(position),
                AeronReplicationEnvelope.defaultWireNonce(clusterId)));
    }
}
