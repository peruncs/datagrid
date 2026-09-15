package peruncs.datagrid.cluster.storage.types;


import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.typing.Disposable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

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
        return new Caching(Objects.requireNonNull(delegate, "delegate"));
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

        /// Queues a complete dictionary for the next data transaction. This is used
    /// after writer restart to re-establish the reader schema before new binaries
    /// arrive. The queued snapshot is authoritative: it replaces any staged
    /// incremental dictionary, and later incrementals are dropped until the
    /// snapshot is consumed.
    ///
    /// @param typeDictionaryData assembled type dictionary
    default void queueTypeDictionaryForNextTransaction(final String typeDictionaryData) {
        this.distributeTypeDictionary(typeDictionaryData);
    }

        /// Keeps dictionary data adjacent to the transaction that needs it.
    /// One staged slot is enough: cluster storage has one writer, so a queued
    /// restart snapshot and later incremental exports never need separate slots.
    final class Caching implements StorageBinaryDataDistributor {
        private final StorageBinaryDataDistributor delegate;
        /* The single staged dictionary plus whether it is an authoritative restart
         * snapshot. Both are held in one immutable value so the staged string and
         * its provenance can never be observed out of sync. A `null` reference
         * means nothing is staged. */
        private final AtomicReference<Pending> pending = new AtomicReference<>();

        private Caching(final StorageBinaryDataDistributor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void distributeData(final Binary data) {
            final Pending staged = this.pending.getAndSet(null);
            if (staged != null) {
                this.delegate.distributeTypeDictionary(staged.dictionary());
            }
            this.delegate.distributeData(data);
        }

        @Override
        public void distributeTypeDictionary(final String typeDictionaryData) {
            if (typeDictionaryData == null) {
                this.pending.set(null);
                return;
            }
            /* Do not let a later incremental export overwrite an authoritative
             * restart snapshot that is still waiting for the next transaction. */
            this.pending.updateAndGet(current ->
                    current != null && current.snapshot() ? current : new Pending(typeDictionaryData, false));
        }

        @Override
        public void queueTypeDictionaryForNextTransaction(final String typeDictionaryData) {
            /* A node may have accumulated an incremental dictionary while startup
             * distribution was disabled. The restart snapshot is authoritative and
             * replaces that stale value. */
            this.pending.set(typeDictionaryData == null ? null : new Pending(typeDictionaryData, true));
        }

        @Override
        public String consumeTypeDictionary() {
            final Pending staged = this.pending.getAndSet(null);
            return staged == null ? null : staged.dictionary();
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
            this.pending.set(null);
            this.delegate.dispose();
        }

        /// One staged dictionary and whether it is an authoritative snapshot.
        private record Pending(String dictionary, boolean snapshot) {
        }
    }
}
