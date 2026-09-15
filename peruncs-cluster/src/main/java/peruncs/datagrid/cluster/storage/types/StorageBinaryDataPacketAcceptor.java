package peruncs.datagrid.cluster.storage.types;


import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.typing.Disposable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

import static org.eclipse.serializer.util.X.notNull;

/// Reassembles ordered packets and forwards complete messages to a receiver.
public interface StorageBinaryDataPacketAcceptor extends Consumer<List<StorageBinaryDataPacket>>, Disposable {
        /// Creates an acceptor for one receiver.
    ///
    /// @param receiver destination for complete messages
    /// @return packet acceptor
    static StorageBinaryDataPacketAcceptor New(final StorageBinaryDataReceiver receiver) {
        return new StorageBinaryDataPacketAcceptor.Default(
                notNull(receiver)
        );
    }

        /// Creates an acceptor that also owns the merger lifecycle.
    static StorageBinaryDataPacketAcceptor New(final StorageBinaryDataMerger merger) {
        return new StorageBinaryDataPacketAcceptor.Default(notNull(merger));
    }

        /// Returns a terminal receiver failure, or `null` while healthy.
    default RuntimeException failure() {
        return null;
    }

        /// Waits until deferred receiver work has completed.
    default void awaitApplied() {
    }

        /// Accepts a complete binary without packet reassembly.
    default void acceptData(final Binary data) {
        throw new UnsupportedOperationException("complete-binary delivery is not supported");
    }

        /// Accepts a complete binary and may take ownership of its buffers.
    default boolean acceptDataOwned(final Binary data) {
        this.acceptData(data);
        return false;
    }

        /// Reports whether complete-binary delivery transfers ownership.
    default boolean canAcceptDataOwned() {
        return false;
    }

        /// Accepts a decoded type dictionary without packet reassembly.
    default void acceptTypeDictionary(final String dictionary) {
        throw new UnsupportedOperationException("decoded dictionary delivery is not supported");
    }

        /// Reports whether no partial message is currently retained.
    ///
    /// Consumers use this boundary to commit transport offsets only after a
    /// complete message has been accepted. Implementations that do not retain
    /// state may keep the default.
    ///
    /// @return `true` when the next packet starts a new message
    default boolean isAtMessageBoundary() {
        return true;
    }

        /// Forwards a packet batch to the reassembler.
    ///
    /// @param packet packet batch
    @Override
    void accept(final List<StorageBinaryDataPacket> packet);

        /// Releases a retained partial message, if any.
    ///
    /// The default implementation retains nothing.
    default void dispose() {
    }

        /// Reassembles packets and forwards complete messages to a receiver.
    class Default implements StorageBinaryDataPacketAcceptor {
        private final StorageBinaryDataReceiver receiver;
        private StorageBinaryDataMessage message;

                /// Creates an acceptor for one receiver.
        ///
        /// @param receiver destination for complete messages
        protected Default(final StorageBinaryDataReceiver receiver) {
            super();
            this.receiver = receiver;
        }

        @Override
        public synchronized void accept(final List<StorageBinaryDataPacket> packets) {
            final StorageBinaryDataPacketAssembler.Result result;
            try {
                result = StorageBinaryDataPacketAssembler.collect(this.message, packets);
                this.message = result.pending();
            } catch (final RuntimeException | Error failure) {
                this.message = null;
                throw failure;
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
            if (this.receiver instanceof Disposable disposable) {
                disposable.dispose();
            }
        }

        @Override
        public RuntimeException failure() {
            return this.receiver.failure();
        }

        @Override
        public void awaitApplied() {
            this.receiver.awaitApplied();
        }

        @Override
        public void acceptData(final Binary data) {
            this.receiver.receiveData(data);
        }

        @Override
        public boolean acceptDataOwned(final Binary data) {
            return this.receiver.receiveDataOwned(data);
        }

        @Override
        public boolean canAcceptDataOwned() {
            return this.receiver.canReceiveDataOwned();
        }

        @Override
        public void acceptTypeDictionary(final String dictionary) {
            this.receiver.receiveTypeDictionary(dictionary);
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
                case DATA -> {
                    // join all buffers of previous data messages
                    this.receiver.receiveData(
                            StorageBinaryDataChunker.wrap(buffers)
                    );
                }

                case TYPE_DICTIONARY -> {
                    // type dictionary is always sent completely, so only the last one is relevant
                    this.receiver.receiveTypeDictionary(StorageBinaryDataPacketAssembler.decodeTypeDictionary(last.data()));
                }
            }
        }

    }

}
