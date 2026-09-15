package peruncs.datagrid.cluster.nodelibrary.replication;


import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.typing.Disposable;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataPacket;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataPacketAcceptor;

import java.util.List;

import static org.eclipse.serializer.util.X.notNull;

/**
 * A {@link StorageBinaryDataPacketAcceptor} that forwards completed messages to
 * the merger and then disposes their packet-owned buffers. The merger copies
 * data it needs for deferred materialization, so ownership remains local to
 * this acceptor. Packet delivery is borrowed and synchronous: implementations
 * must consume or copy packet-owned buffers before returning and must not retain
 * them. The direct Aeron path may use the separate ownership-aware callback
 * when the merger advertises it. This class will also call
 * {@link #dispose()} on the {@link ClusterStorageBinaryDataMerger}
 */
public interface ClusterStorageBinaryDataPacketAcceptor extends StorageBinaryDataPacketAcceptor, Disposable {
    /**
     * Creates an acceptor for one merger.
     *
     * @param merger destination merger
     * @return packet acceptor
     */
    static ClusterStorageBinaryDataPacketAcceptor New(final ClusterStorageBinaryDataMerger merger) {
        return new Default(notNull(merger));
    }

    /**
     * Returns a failure reported by the asynchronous merger, or {@code null}.
     *
     * @return merger failure, or {@code null}
     */
    default RuntimeException failure() {
        return null;
    }

    /**
     * Waits until deferred data has been applied.
     *
     * <p>The default implementation has no deferred work.</p>
     */
    default void awaitApplied() {
    }

    /**
     * Releases a retained partial message and the merger.
     */
    @Override
    void dispose();

    /**
     * Accepts a complete binary without rebuilding packets. Aeron readers use
     * this boundary because their assembler has already validated and reassembled
     * the transaction. The binary is borrowed for the duration of this call;
     * packet transports continue to use {@link #accept(List)}.
     *
     * @param data complete binary
     */
    default void acceptData(final Binary data) {
        throw new UnsupportedOperationException("complete-binary delivery is not supported");
    }

    /**
     * Accepts a complete binary and may take ownership of its direct buffers.
     * Returning {@code true} transfers release responsibility to the acceptor.
     *
     * @param data complete binary
     * @return whether ownership was transferred
     */
    default boolean acceptDataOwned(final Binary data) {
        this.acceptData(data);
        return false;
    }

    /**
     * Reports whether complete-binary delivery transfers ownership before the
     * callback starts.
     *
     * @return whether the acceptor owns the binary on callback entry
     */
    default boolean canAcceptDataOwned() {
        return false;
    }

    /**
     * Accepts a type dictionary already decoded by the transport.
     *
     * @param dictionary decoded type dictionary
     */
    default void acceptTypeDictionary(final String dictionary) {
        throw new UnsupportedOperationException("decoded dictionary delivery is not supported");
    }

    /** Reassembles packets with the shared acceptor and forwards merger callbacks. */
    class Default implements ClusterStorageBinaryDataPacketAcceptor {
        private final ClusterStorageBinaryDataMerger merger;
        private final StorageBinaryDataPacketAcceptor delegate;

        /**
         * Creates the packet acceptor implementation.
         *
         * @param merger destination merger
         */
        protected Default(final ClusterStorageBinaryDataMerger merger) {
            super();
            this.merger = merger;
            this.delegate =
                    StorageBinaryDataPacketAcceptor.New(merger);
        }

        @Override
        public void accept(final List<StorageBinaryDataPacket> packets) {
            this.delegate.accept(packets);
        }

        @Override
        public RuntimeException failure() {
            return this.merger.failure();
        }

        @Override
        public void acceptData(final Binary data) {
            this.merger.receiveData(data);
        }

        @Override
        public boolean acceptDataOwned(final Binary data) {
            return this.merger.receiveDataOwned(data);
        }

        @Override
        public boolean canAcceptDataOwned() {
            return this.merger.canReceiveDataOwned();
        }

        @Override
        public void acceptTypeDictionary(final String dictionary) {
            this.merger.receiveTypeDictionary(dictionary);
        }

        @Override
        public synchronized void dispose() {
            this.delegate.dispose();
            this.merger.dispose();
        }

        @Override
        public void awaitApplied() {
            this.merger.awaitApplied();
        }
    }
}
