package peruncs.cluster.storage.aeron.reader;

import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.cluster.storage.binary.StorageBinaryDataReceiver;

import java.util.Objects;
import java.util.UUID;

/// Builds [TransactionAssembler] fixtures with the canonical constructor.
///
/// Production code must pass an explicit wire nonce. These factories fill in
/// the documented fixture-only derived nonce so reader tests stay close to the
/// production call shape without reintroducing a defaulting production
/// overload.
final class TransactionAssemblerTestSupport {
    private TransactionAssemblerTestSupport() {
    }

    /// Creates an assembler with fixture defaults.
    ///
    /// @param configuration framing and timeout limits
    /// @param clusterId     expected cluster identity
    /// @param epoch         expected writer epoch
    /// @param receiver      Store binary receiver
    /// @return assembler at sequence and position `-1`
    static TransactionAssembler New(final AeronReplicationConfiguration configuration, final UUID clusterId,
                                    final long epoch, final StorageBinaryDataReceiver receiver) {
        return New(configuration, clusterId, epoch, -1L, receiver, () -> {
        });
    }

    /// Creates an assembler at a recovered sequence.
    ///
    /// @param configuration   framing and timeout limits
    /// @param clusterId       expected cluster identity
    /// @param epoch           expected writer epoch
    /// @param initialSequence last resolved sequence, or `-1`
    /// @param receiver        Store binary receiver
    /// @return assembler at the supplied sequence
    static TransactionAssembler New(final AeronReplicationConfiguration configuration, final UUID clusterId,
                                    final long epoch, final long initialSequence,
                                    final StorageBinaryDataReceiver receiver) {
        return New(configuration, clusterId, epoch, initialSequence, receiver, () -> {
        });
    }

    /// Creates an assembler with a resolution callback.
    ///
    /// @param configuration       framing and timeout limits
    /// @param clusterId           expected cluster identity
    /// @param epoch               expected writer epoch
    /// @param initialSequence     last resolved sequence, or `-1`
    /// @param receiver            Store binary receiver
    /// @param transactionResolved callback after a transaction resolves
    /// @return assembler with the supplied callback
    static TransactionAssembler New(final AeronReplicationConfiguration configuration, final UUID clusterId,
                                    final long epoch, final long initialSequence,
                                    final StorageBinaryDataReceiver receiver,
                                    final Runnable transactionResolved) {
        return New(configuration, clusterId, epoch, initialSequence, receiver, transactionResolved, null);
    }

    /// Creates an assembler with resolution and delivery callbacks.
    ///
    /// @param configuration       framing and timeout limits
    /// @param clusterId           expected cluster identity
    /// @param epoch               expected writer epoch
    /// @param initialSequence     last resolved sequence, or `-1`
    /// @param receiver            Store binary receiver
    /// @param transactionResolved callback after a transaction resolves
    /// @param deliveryListener    callback around Store materialisation, or `null`
    /// @return assembler with the supplied callbacks
    static TransactionAssembler New(final AeronReplicationConfiguration configuration, final UUID clusterId,
                                    final long epoch, final long initialSequence,
                                    final StorageBinaryDataReceiver receiver,
                                    final Runnable transactionResolved,
                                    final ReaderDeliveryListener deliveryListener) {
        return New(configuration, clusterId, epoch, initialSequence, -1L, receiver, transactionResolved,
                deliveryListener);
    }

    /// Creates an assembler at a recovered sequence and position.
    ///
    /// @param configuration       framing and timeout limits
    /// @param clusterId           expected cluster identity
    /// @param epoch               expected writer epoch
    /// @param initialSequence     last resolved sequence, or `-1`
    /// @param initialPosition     last resolved Archive position, or `-1`
    /// @param receiver            Store binary receiver
    /// @param transactionResolved callback after a transaction resolves
    /// @param deliveryListener    callback around Store materialisation, or `null`
    /// @return assembler at the supplied cursor
    static TransactionAssembler New(final AeronReplicationConfiguration configuration, final UUID clusterId,
                                    final long epoch, final long initialSequence, final long initialPosition,
                                    final StorageBinaryDataReceiver receiver,
                                    final Runnable transactionResolved,
                                    final ReaderDeliveryListener deliveryListener) {
        Objects.requireNonNull(clusterId, "clusterId");
        return new TransactionAssembler(configuration, clusterId, epoch, initialSequence, initialPosition,
                receiver, ignored -> transactionResolved.run(), deliveryListener,
                AeronReplicationEnvelope.defaultWireNonce(clusterId));
    }
}
