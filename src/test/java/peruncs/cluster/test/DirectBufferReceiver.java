package peruncs.cluster.test;

import peruncs.cluster.storage.binary.StorageBinaryDataReceiver;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/// Test receiver that allocates plain direct buffers and leaves them to the garbage collector.
///
/// Production receivers own and release their buffers through a pool; this untracked policy is only
/// for short tests whose receiver is not under test, so it lives in the test tree.
public interface DirectBufferReceiver extends StorageBinaryDataReceiver {
    @Override
    default ByteBuffer allocateNativeBuffer(final int minimumCapacity) {
        return ByteBuffer.allocateDirect(minimumCapacity).order(ByteOrder.nativeOrder());
    }

    @Override
    default void releaseNativeBuffer(final ByteBuffer buffer) {
    }
}
