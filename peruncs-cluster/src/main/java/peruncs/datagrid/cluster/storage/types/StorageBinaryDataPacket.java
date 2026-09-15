package peruncs.datagrid.cluster.storage.types;



import peruncs.datagrid.cluster.storage.types.StorageBinaryDataMessage.MessageType;

import java.nio.ByteBuffer;

import static org.eclipse.serializer.math.XMath.notNegative;
import static org.eclipse.serializer.math.XMath.positive;
import static org.eclipse.serializer.util.X.notNull;

/// A packet carries one fragment of a storage message and its optional
/// transport message index.
///
/// The packet index identifies a fragment inside one message. The message
/// index identifies the complete message in the cluster stream, so readers can
/// resume after a backup without confusing fragments from different messages.
public sealed interface StorageBinaryDataPacket
        permits StorageBinaryDataPacketDefault {
        /// Creates a packet without a transport message index.
    ///
    /// @param messageType   message kind
    /// @param messageLength complete message length
    /// @param packetIndex   packet index
    /// @param packetCount   packet count
    /// @param messageIndex  complete message index
    /// @param buffer        packet payload
    /// @return new packet
        /// Creates a packet with a transport message index.
    ///
    /// @param messageType   message kind
    /// @param messageLength complete message length
    /// @param packetIndex   packet index
    /// @param packetCount   packet count
    /// @param messageIndex  complete message index
    /// @param buffer        packet payload
    /// @return new packet
    static StorageBinaryDataPacket New(
            final MessageType messageType,
            final int messageLength,
            final int packetIndex,
            final int packetCount,
            final ByteBuffer buffer
    ) {
        return New(messageType, messageLength, packetIndex, packetCount, -1L, buffer);
    }

    static StorageBinaryDataPacket New(
            final MessageType messageType,
            final int messageLength,
            final int packetIndex,
            final int packetCount,
            final long messageIndex,
            final ByteBuffer buffer
    ) {
        if (messageIndex < -1L || messageIndex == Long.MAX_VALUE) {
            throw new StorageBinaryDataException("message index must be in [-1, Long.MAX_VALUE)");
        }
        final int validatedPacketIndex = notNegative(packetIndex);
        final int validatedPacketCount = positive(packetCount);
        if (validatedPacketIndex >= validatedPacketCount) {
            throw new IllegalArgumentException("packetIndex must be less than packetCount");
        }
        return new StorageBinaryDataPacketDefault(
                notNull(messageType),
                notNegative(messageLength),
                validatedPacketIndex,
                validatedPacketCount,
                messageIndex,
                validatedBuffer(buffer)
        );
    }

    private static ByteBuffer validatedBuffer(final ByteBuffer buffer) {
        final ByteBuffer checked = notNull(buffer);
        if (!checked.hasRemaining()) {
            throw new StorageBinaryDataException("packet payload must not be empty");
        }
        return checked;
    }

        /// Returns the message kind.
    MessageType messageType();

        /// Returns the complete message length.
    int messageLength();

        /// Returns the packet index within the message.
    int packetIndex();

        /// Returns the packet count in the message.
    int packetCount();

        /// Returns the borrowed packet payload.
    ByteBuffer buffer();

        /// Returns the complete message index.
    ///
    /// @return message index
    long messageIndex();

}

/// Package-private immutable implementation of a cluster packet.
record StorageBinaryDataPacketDefault(
        MessageType messageType,
        int messageLength,
        int packetIndex,
        int packetCount,
        long messageIndex,
        ByteBuffer buffer
) implements StorageBinaryDataPacket {
}
