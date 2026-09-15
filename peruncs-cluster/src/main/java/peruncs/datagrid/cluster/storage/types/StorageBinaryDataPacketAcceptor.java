package peruncs.datagrid.cluster.storage.types;


import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.typing.Disposable;

import static org.eclipse.serializer.util.X.notNull;

/// Forwards complete Store binaries to a receiver without packet reassembly.
///
/// Transports deliver complete binaries, so this acceptor is a thin decorator:
/// it forwards delivery calls to one receiver and owns the receiver lifecycle
/// on dispose.
public interface StorageBinaryDataPacketAcceptor extends Disposable {
        /// Creates an acceptor for one receiver.
    ///
    /// @param receiver destination for complete binaries
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

        /// Forwards complete binaries to a receiver and owns its disposal.
    class Default implements StorageBinaryDataPacketAcceptor {
        private final StorageBinaryDataReceiver receiver;

                /// Creates an acceptor for one receiver.
        ///
        /// @param receiver destination for complete binaries
        protected Default(final StorageBinaryDataReceiver receiver) {
            super();
            this.receiver = receiver;
        }

        @Override
        public void dispose() {
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
    }
}
