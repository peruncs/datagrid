package peruncs.cluster.storage.aeron.reader;

/// Hooks around materializing one committed Store transaction.
///
/// The callback runs on the reader polling thread and gives the owner a place
/// to record an uncertain reader state before import. The state is cleared only
/// after import and cursor handling finish. A state left by a crash is
/// therefore evidence that recovery needs a deliberate decision, not evidence
/// that the import succeeded or failed.
///
/// The callback runs inside the reader's delivery boundary. It must not call
/// back into the owning transport or close the reader; request lifecycle
/// changes after the callback returns. Re-entry would compete with shutdown and
/// can deadlock a caller that is waiting for the polling thread.
@FunctionalInterface
public interface ReaderDeliveryListener {
        /// Invoked immediately before the assembled bytes enter the Store.
    ///
    /// @param sequence       terminal replication sequence
    /// @param position       terminal Archive position
    /// @param dataLength     assembled Store binary length
    /// @param dataChunkCount assembled Store chunk count
    /// @param crc32c         checksum from the commit marker
    void beforeStoreImport(long sequence, long position, int dataLength, int dataChunkCount, int crc32c);

        /// Invoked synchronously on the same polling thread after Store import and
    /// the durable cursor callback succeed. An implementation may clear the
    /// uncertainty marker for the transaction whose [#beforeStoreImport]
    /// callback just ran; no transaction arguments are repeated because the
    /// assembler processes one transaction at a time.
    default void afterStoreImport() {
    }
}
