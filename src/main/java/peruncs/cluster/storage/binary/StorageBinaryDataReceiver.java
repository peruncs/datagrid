package peruncs.cluster.storage.binary;

import org.eclipse.serializer.persistence.binary.types.Binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/// Receives complete Store binaries and type dictionaries from a provider.
///
/// The ordinary callback is borrowed and synchronous. A transport that has
/// already assembled a native binary may override [#receiveDataOwned(Binary)]
/// to take buffer ownership, followed by [#awaitApplied()] when the
/// receiver performs deferred materialization. The owned callback must return
/// `true` only after the receiver has taken responsibility for releasing
/// every direct buffer in the supplied binary.
public interface StorageBinaryDataReceiver {
    /// Allocates native storage for a transaction being assembled for this receiver.
    ///
    /// The receiver owns the allocation policy (pooling, limits, accounting) because it is the one that
    /// finally releases the buffers; there is deliberately no default.
    ///
    /// @param minimumCapacity required capacity in bytes
    /// @return writable native buffer with at least the requested capacity
    ByteBuffer allocateNativeBuffer(int minimumCapacity);

    /// Releases native storage acquired for a transaction for this receiver.
    ///
    /// @param buffer native buffer to release
    void releaseNativeBuffer(ByteBuffer buffer);

    /// Returns a terminal receiver failure, or `null` while healthy.
    ///
    /// @return terminal failure, or `null`
    default RuntimeException failure() {
        return null;
    }

    /// Reports whether [#receiveDataOwned(Binary)] takes ownership before
    /// invoking the implementation.
    ///
    /// `true` is reserved for receivers that release the supplied direct
    /// buffers on every success and failure path. The default is `false`,
    /// so the caller remains the owner until the method returns `true`.
    ///
    /// @return whether ownership transfers before the callback starts
    default boolean canReceiveDataOwned() {
        return false;
    }

    /// Reports whether owned data can be admitted without waiting for backpressure.
    ///
    /// A `false` result leaves the binary with the caller, which should stop polling,
    /// then retry delivery outside the transport callback. The default keeps simple
    /// synchronous receivers on their existing path.
    ///
    /// @param payloadBytes size of the Store binary
    /// @return whether delivery can proceed without waiting
    default boolean canAcceptOwnedData(final long payloadBytes) {
        return true;
    }

    /// Receives a complete binary. The callback must consume the supplied binary
    /// synchronously; transports may release its native buffers immediately after
    /// this method returns to avoid retaining off-heap memory.
    ///
    /// @param data complete binary to receive
    void receiveData(Binary data);

    /// Delivers a complete binary while allowing an implementation to take
    /// ownership of its direct buffers. The default uses the borrowed callback
    /// contract and therefore returns `false`; the caller then
    /// releases its buffers after this method returns. An override takes ownership
    /// before processing and must release the buffers itself if processing fails.
    ///
    /// @param data complete binary whose direct buffers may be transferred
    /// @return `true` when the receiver owns the buffers after return
    default boolean receiveDataOwned(final Binary data) {
        this.receiveData(data);
        return false;
    }

    /// Completes the durable application of the most recently accepted binary.
    /// Implementations that only consume the binary synchronously may leave this
    /// method as a no-op.  A transport adapter uses it to wait for deferred Store
    /// materialization after ownership has already transferred, which prevents a
    /// failure during the wait from causing the caller to free the same buffers a
    /// second time.
    default void awaitApplied() {
    }

    /// Receives a type dictionary before data that depends on it.
    ///
    /// Replicated imports never execute the local Store operation that would
    /// normally flush the exporting dictionary manager, so implementations
    /// flush newly registered definitions explicitly — otherwise a restart
    /// could no longer resolve the imported type ids.
    ///
    /// @param typeDictionaryData assembled type dictionary
    void receiveTypeDictionary(String typeDictionaryData);
}
