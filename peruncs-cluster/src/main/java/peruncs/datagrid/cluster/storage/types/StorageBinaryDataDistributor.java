package peruncs.datagrid.cluster.storage.types;


import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.typing.Disposable;

import java.util.concurrent.atomic.AtomicReference;

import static org.eclipse.serializer.util.X.notNull;

/// Cluster-aware extension of the binary distributor.
///
/// The message index and ignore flag are lifecycle controls, not transport
/// details. [Caching] preserves a type dictionary until the next data
/// message and is used by the Aeron provider.
public interface StorageBinaryDataDistributor extends Disposable {
        /// Publishes one complete Store binary.
    void distributeData(Binary data);

        /// Publishes a type dictionary needed by later Store data.
    void distributeTypeDictionary(String typeDictionaryData);

        /// Returns and clears a dictionary staged for the next binary transaction.
    default String consumeTypeDictionary() {
        return null;
    }

        /// Creates a distributor that ignores all transport work.
    ///
    /// @return neutral distributor
    static StorageBinaryDataDistributor NoOp() {
        return new StorageBinaryDataDistributor() {
            private long index = -1;
            private boolean ignored;

            public void messageIndex(final long value) {
                this.index = value;
            }

            public long messageIndex() {
                return this.index;
            }

            public void ignoreDistribution(final boolean value) {
                this.ignored = value;
            }

            public boolean ignoreDistribution() {
                return this.ignored;
            }

            public void distributeTypeDictionary(final String value) {
            }

            public void distributeData(final Binary value) {
            }

            public void dispose() {
            }
        };
    }

        /// Creates a distributor that keeps dictionary data beside its next binary.
    ///
    /// @param delegate destination distributor
    /// @return caching distributor
    static StorageBinaryDataDistributor Caching(final StorageBinaryDataDistributor delegate) {
        return new Caching(notNull(delegate));
    }

        /// Sets the next message index when the transport supports indexed distribution.
    ///
    /// @param index message index
    default void messageIndex(final long index) {
    }

        /// Returns the current message index.
    ///
    /// @return message index
    default long messageIndex() {
        return -1L;
    }

        /// Sets whether distribution is ignored during lifecycle bootstrap.
    ///
    /// @param ignore whether to ignore distribution
    default void ignoreDistribution(final boolean ignore) {
    }

        /// Reports whether distribution is ignored.
    ///
    /// @return `true` when ignored
    default boolean ignoreDistribution() {
        return false;
    }

        /// Returns a terminal distribution failure, or `null` while healthy.
    ///
    /// @return terminal failure, or `null`
    default RuntimeException failure() {
        return null;
    }

        /// Queues a complete dictionary for the next data transaction, regardless of
    /// which Store thread performs that transaction. This is used after writer
    /// restart to re-establish the reader schema before new binaries arrive.
    ///
    /// @param typeDictionaryData assembled type dictionary
    default void queueTypeDictionaryForNextTransaction(final String typeDictionaryData) {
        this.distributeTypeDictionary(typeDictionaryData);
    }

        /// Keeps dictionary data adjacent to the transaction that needs it.
    final class Caching implements StorageBinaryDataDistributor {
        private final StorageBinaryDataDistributor delegate;
        /* Cluster storage has one writer. Keep the pending dictionary in an explicit
         * atomic slot instead of attaching it to a platform or virtual thread. */
        private final AtomicReference<String> typeDictionaryData = new AtomicReference<>();
        private final AtomicReference<String> queuedTypeDictionary = new AtomicReference<>();

        private Caching(final StorageBinaryDataDistributor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void distributeData(final Binary data) {
            final String dictionary = this.typeDictionaryData.getAndSet(null);
            if (dictionary != null) {
                this.delegate.distributeTypeDictionary(dictionary);
            }
            this.delegate.distributeData(data);
        }

        @Override
        public void distributeTypeDictionary(final String typeDictionaryData) {
            if (typeDictionaryData == null) {
                this.typeDictionaryData.set(null);
            } else {
                this.typeDictionaryData.set(typeDictionaryData);
            }
        }

        @Override
        public void queueTypeDictionaryForNextTransaction(final String typeDictionaryData) {
            /* A node may have accumulated an incremental dictionary while startup
             * distribution was disabled.  The restart snapshot is authoritative and
             * must replace that stale thread-bound value, otherwise consumeTypeDictionary
             * would return the incremental fragment and the queued full dictionary would
             * never reach the next replicated transaction. */
            this.typeDictionaryData.set(null);
            this.queuedTypeDictionary.set(typeDictionaryData);
        }

        @Override
        public String consumeTypeDictionary() {
            final String queued = this.queuedTypeDictionary.getAndSet(null);
            if (queued != null) {
                /* A full restart snapshot supersedes any incremental dictionary staged
                 * on the calling thread while startup distribution was disabled. */
                this.typeDictionaryData.set(null);
                return queued;
            }
            return this.typeDictionaryData.getAndSet(null);
        }

        @Override
        public void messageIndex(final long index) {
            this.delegate.messageIndex(index);
        }

        @Override
        public long messageIndex() {
            return this.delegate.messageIndex();
        }

        @Override
        public boolean ignoreDistribution() {
            return this.delegate.ignoreDistribution();
        }

        @Override
        public RuntimeException failure() {
            return this.delegate.failure();
        }

        @Override
        public void ignoreDistribution(final boolean ignore) {
            this.delegate.ignoreDistribution(ignore);
        }

        @Override
        public void dispose() {
            this.typeDictionaryData.set(null);
            this.queuedTypeDictionary.set(null);
            this.delegate.dispose();
        }
    }
}
