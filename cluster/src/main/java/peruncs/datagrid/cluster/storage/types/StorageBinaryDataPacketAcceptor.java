package peruncs.datagrid.cluster.storage.types;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

import static org.eclipse.serializer.util.X.notNull;

/** Reassembles ordered packets and forwards complete messages to a receiver. */
public interface StorageBinaryDataPacketAcceptor extends Consumer<List<StorageBinaryDataPacket>> {
    /**
     * Creates an acceptor for one receiver.
     *
     * @param receiver destination for complete messages
     * @return packet acceptor
     */
    static StorageBinaryDataPacketAcceptor New(final StorageBinaryDataReceiver receiver) {
        return new StorageBinaryDataPacketAcceptor.Default(
                notNull(receiver)
        );
    }

    /**
     * Reports whether no partial message is currently retained.
     *
     * <p>Consumers use this boundary to commit transport offsets only after a
     * complete message has been accepted. Implementations that do not retain
     * state may keep the default.</p>
     *
     * @return {@code true} when the next packet starts a new message
     */
    default boolean isAtMessageBoundary() {
        return true;
    }

    /**
     * Forwards a packet batch to the reassembler.
     *
     * @param packet packet batch
     */
    @Override
    void accept(final List<StorageBinaryDataPacket> packet);

    /**
     * Releases a retained partial message, if any.
     *
     * <p>The default implementation retains nothing.</p>
     */
    default void dispose() {
    }

    /** Reassembles packets and forwards complete messages to a receiver. */
    class Default implements StorageBinaryDataPacketAcceptor {
        private final StorageBinaryDataReceiver receiver;
        private StorageBinaryDataMessage message;

        /**
         * Creates an acceptor for one receiver.
         *
         * @param receiver destination for complete messages
         */
        protected Default(final StorageBinaryDataReceiver receiver) {
            super();
            this.receiver = receiver;
        }

        @Override
        public void accept(final List<StorageBinaryDataPacket> packets) {
            final StorageBinaryDataPacketAssembler.Result result;
            synchronized (this) {
                try {
                    result = StorageBinaryDataPacketAssembler.collect(this.message, packets);
                    this.message = result.pending();
                } catch (final RuntimeException | Error failure) {
                    this.message = null;
                    throw failure;
                }
            }
            if (!result.completed().isEmpty()) {
                this.handleCompleteMessages(result.completed());
            }
        }

        @Override
        public synchronized boolean isAtMessageBoundary() {
            return this.message == null;
        }

        @Override
        public synchronized void dispose() {
            final StorageBinaryDataMessage pending = this.message;
            this.message = null;
            if (pending != null) {
                pending.dispose();
            }
        }

        private void handleCompleteMessages(final List<StorageBinaryDataMessage> messages) {
            // Join similar messages and hand over to receiver
            try {
                StorageBinaryDataPacketAssembler.dispatch(messages, this::send);
            } finally {
                messages.forEach(StorageBinaryDataMessage::dispose);
            }
        }

        private void send(final StorageBinaryDataMessage last, final List<ByteBuffer> buffers) {
            switch (last.type()) {
                case DATA: {
                    // join all buffers of previous data messages
                    this.receiver.receiveData(
                            StorageBinaryDataChunker.wrap(buffers)
                    );
                }
                break;

                case TYPE_DICTIONARY: {
                    // type dictionary is always sent completely, so only the last one is relevant
                    this.receiver.receiveTypeDictionary(StorageBinaryDataPacketAssembler.decodeTypeDictionary(last.data()));
                }
                break;
            }
        }

    }

}
