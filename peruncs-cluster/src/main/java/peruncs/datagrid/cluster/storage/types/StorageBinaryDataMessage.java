package peruncs.datagrid.cluster.storage.types;


import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.typing.Disposable;

import java.nio.ByteBuffer;

import static org.eclipse.serializer.util.X.notNull;

/// This message assembles ordered packets into one type dictionary or binary.
///
/// The message owns its direct buffer after the first packet arrives. A
/// complete message can be consumed and then disposed; callers must not retain
/// its buffer after disposal.
public interface StorageBinaryDataMessage extends Disposable, AutoCloseable {
        /// Defensive upper bound for a single network message.
    int MAX_MESSAGE_LENGTH = ReplicationLimits.MAX_MESSAGE_BYTES;
        /// Defensive upper bound for packet metadata in one message.
    int MAX_PACKET_COUNT = ReplicationLimits.MAX_PACKET_COUNT;

        /// Creates a message from its first packet.
    ///
    /// @param initialPacket first packet
    /// @return new message
    static StorageBinaryDataMessage New(final StorageBinaryDataPacket initialPacket) {
        return new StorageBinaryDataMessageDefault(
                notNull(initialPacket)
        );
    }

        /// Returns the message kind.
    ///
    /// @return message kind
    MessageType type();

        /// Returns the declared message length.
    ///
    /// @return message length in bytes
    int length();

        /// Returns the declared packet count.
    ///
    /// @return packet count
    int packetCount();

        /// Adds the next packet and returns this message.
    ///
    /// @param packet next packet in order
    /// @return this message
    StorageBinaryDataMessage addPacket(StorageBinaryDataPacket packet);

        /// Reports whether all declared packets have arrived.
    ///
    /// @return `true` when the message is complete
    boolean isComplete();

        /// Returns the completed message bytes.
    ///
    /// @return read-only-positioned message buffer
    ByteBuffer data();

        /// Identifies whether the assembled bytes describe types or Store data.
    enum MessageType {
                /// The bytes describe a type dictionary.
        TYPE_DICTIONARY,
                /// The bytes describe Store data.
        DATA
    }

}

/// Package-private implementation that owns the direct buffer.
final class StorageBinaryDataMessageDefault implements StorageBinaryDataMessage {
    private final MessageType type;
    private final int length;
    private final int packetCount;
    private int receivedPackets = 0;
    private ByteBuffer buffer;

    StorageBinaryDataMessageDefault(final StorageBinaryDataPacket initialPacket) {
        super();
        this.type = initialPacket.messageType();
        this.length = initialPacket.messageLength();
        this.packetCount = initialPacket.packetCount();
        if (this.length > MAX_MESSAGE_LENGTH) {
            throw new StorageBinaryDataException("Data message exceeds maximum length %s".formatted(MAX_MESSAGE_LENGTH));
        }
        if (this.length < 0 || this.packetCount <= 0 || this.packetCount > MAX_PACKET_COUNT) {
            throw new StorageBinaryDataException("invalid data message dimensions");
        }
        /* Do not trust the declared length as an allocation request. A malformed
         * first packet may claim the full limit while carrying only a few bytes. */
        final ByteBuffer firstBuffer = initialPacket.buffer();
        if (firstBuffer == null) {
            throw new StorageBinaryDataException("Initial packet has no payload buffer");
        }
        this.buffer = XMemory.allocateDirectNative(Math.max(1,
                Math.min(this.length, firstBuffer.remaining())));
        try {
            this.addPacket(initialPacket);
        } catch (final RuntimeException | Error failure) {
            this.dispose();
            throw failure;
        }
    }

    private void validateForAddition(final StorageBinaryDataPacket packet) {
        if (this.buffer == null) {
            throw new StorageBinaryDataException("Data message already disposed");
        }
        if (this.isComplete()) {
            throw new StorageBinaryDataException("Data message already complete");
        }

        final MessageType expectedMessageType = this.type;
        if (packet.messageType() != expectedMessageType) {
            throw new StorageBinaryDataException(
                    "Invalid packet type, received %s, expected %s".formatted(packet.messageType(), expectedMessageType)
            );
        }

        final int expectedPacketIndex = this.receivedPackets;
        if (packet.packetIndex() != expectedPacketIndex) {
            throw new StorageBinaryDataException(
                    "Invalid packet index, received %s, expected %s".formatted(packet.packetIndex(), expectedPacketIndex)
            );
        }
        if (packet.messageLength() != this.length || packet.packetCount() != this.packetCount) {
            throw new StorageBinaryDataException("packet dimensions changed within a message");
        }
        final ByteBuffer packetBuffer = packet.buffer();
        if (packetBuffer == null || packetBuffer.remaining() > this.length - this.buffer.position()) {
            throw new StorageBinaryDataException("packet payload exceeds declared message length");
        }
    }

    private void ensureCapacity(final int required) {
        if (required <= this.buffer.capacity()) return;
        int newCapacity = Math.max(1, this.buffer.capacity());
        while (newCapacity < required) {
            final int doubled = newCapacity << 1;
            newCapacity = doubled > 0 ? Math.min(this.length, doubled) : required;
            if (newCapacity == this.length) break;
        }
        final ByteBuffer replacement = XMemory.allocateDirectNative(newCapacity);
        this.buffer.flip();
        replacement.put(this.buffer);
        XMemory.deallocateDirectByteBuffer(this.buffer);
        this.buffer = replacement;
    }

    private void internalAddPacket(final StorageBinaryDataPacket packet) {
        final ByteBuffer source = packet.buffer().duplicate();
        this.ensureCapacity(this.buffer.position() + source.remaining());
        this.buffer.put(source);

        this.receivedPackets++;

        if (this.isComplete()) {
            if (this.buffer.position() != this.length) {
                throw new StorageBinaryDataException("packet payload does not fill declared message length");
            }
            this.buffer.flip();
        }
    }

    @Override
    public MessageType type() {
        return this.type;
    }

    @Override
    public int length() {
        return this.length;
    }

    @Override
    public int packetCount() {
        return this.packetCount;
    }

    @Override
    public StorageBinaryDataMessage addPacket(final StorageBinaryDataPacket packet) {
        this.validateForAddition(packet);
        this.internalAddPacket(packet);

        return this;
    }

    @Override
    public boolean isComplete() {
        return this.receivedPackets == this.packetCount;
    }

    @Override
    public ByteBuffer data() {
        if (!this.isComplete()) {
            throw new StorageBinaryDataException("Data message not complete yet");
        }

        if (this.buffer == null) {
            throw new StorageBinaryDataException("Data message already disposed");
        }

        return this.buffer.asReadOnlyBuffer();
    }

    @Override
    public void dispose() {
        final ByteBuffer value = this.buffer;
        this.buffer = null;
        if (value != null) XMemory.deallocateDirectByteBuffer(value);
    }

    @Override
    public void close() {
        this.dispose();
    }

}
