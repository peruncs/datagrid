package peruncs.cluster.storage.aeron.writer;

import java.nio.ByteBuffer;

/// Test-only bridge for crash fixtures that need raw Archive frames.
public final class RawArchivePublisher {
    private RawArchivePublisher() {
    }

        /// Publishes a fixture transaction without exposing the raw path in production APIs.
    ///
    /// @param publisher target writer publisher
    /// @param dictionary optional type dictionary bytes, or `null`
    /// @param data Store binary payload buffers
    public static void publish(
            final AeronArchiveReplicationPublisher publisher,
            final byte[] dictionary,
            final ByteBuffer[] data
    ) {
        publisher.publishTransaction(dictionary, data);
    }
}
