package peruncs.datagrid.cluster.storage.types;


import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.typing.Disposable;

import static org.eclipse.serializer.util.X.notNull;

/**
 * Sink for Store binary data and optional type dictionaries. The Aeron
 * implementation transports the bytes; callers only require that dictionary
 * data precede the matching binary transaction.
 */
public interface StorageBinaryDataDistributor extends Disposable {
    /**
     * Creates a distributor that stages the latest type dictionary per thread.
     *
     * @param delegate destination distributor
     * @return caching decorator
     */
    static StorageBinaryDataDistributor Caching(final StorageBinaryDataDistributor delegate) {
        return new StorageBinaryDataDistributor.Caching(
                notNull(delegate)
        );
    }

    /**
     * Publishes one complete Store binary.
     *
     * @param data binary to publish
     */
    void distributeData(Binary data);

    /**
     * Publishes a type dictionary needed by later Store data.
     *
     * @param typeDictionaryData assembled type dictionary
     */
    void distributeTypeDictionary(String typeDictionaryData);

    /**
     * Returns and clears a dictionary staged for the next binary transaction.
     *
     * @return staged dictionary, or {@code null}
     */
    default String consumeTypeDictionary() {
        return null;
    }

    /*
     * Only distribute optional new type dictionary before actual data to minimize
     * traffic.
     */

    /** Stages a type dictionary until the matching binary is published. */
    class Caching implements StorageBinaryDataDistributor {
        private final StorageBinaryDataDistributor delegate;
        /*
         * BinaryStorer exports its dictionary and writes the matching Binary on the
         * committing thread. Keeping the snapshot per thread prevents concurrent
         * commits from attaching one transaction's dictionary to another.
         */
        private final ThreadLocal<String> typeDictionaryData = new ThreadLocal<>();

        Caching(final StorageBinaryDataDistributor delegate) {
            super();
            this.delegate = delegate;
        }

        @Override
        public void distributeData(final Binary data) {
            final String dictionary = this.typeDictionaryData.get();
            this.typeDictionaryData.remove();
            if (dictionary != null) {
                this.delegate.distributeTypeDictionary(dictionary);
            }
            this.delegate.distributeData(data);
        }

        @Override
        public String consumeTypeDictionary() {
            final String value = this.typeDictionaryData.get();
            this.typeDictionaryData.remove();
            return value;
        }

        @Override
        public void distributeTypeDictionary(final String typeDictionaryData) {
            if (typeDictionaryData == null) this.typeDictionaryData.remove();
            else this.typeDictionaryData.set(typeDictionaryData);
        }

        @Override
        public void dispose() {
            this.typeDictionaryData.remove();
            this.delegate.dispose();
        }

    }

}
