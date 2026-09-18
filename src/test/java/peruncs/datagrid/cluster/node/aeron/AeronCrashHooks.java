package peruncs.datagrid.cluster.node.aeron;

import peruncs.datagrid.cluster.storage.aeron.writer.WriterCrashHooks;
import peruncs.datagrid.cluster.storage.types.FileStoreCrashHooks;

import java.util.function.BiConsumer;

/// Test-only bridge for the forked Aeron crash harness.
public final class AeronCrashHooks {
    private AeronCrashHooks() {
    }

        /// Runs an action with writer and provider hooks bound to its scope.
    ///
    /// @param hook callback that receives the crash seam name and sequence
    public static void runWithHook(final BiConsumer<String, Long> hook, final Runnable action) {
        WriterCrashHooks.runWithHook(hook, () ->
                AeronClusterReplicationTransportProvider.runWithCrashHook(hook, action));
    }

        /// Calls an operation with writer and provider hooks bound to its scope.
    public static <T, X extends Throwable> T callWithHook(
            final BiConsumer<String, Long> hook,
            final ScopedValue.CallableOp<? extends T, X> operation
    ) throws X {
        return WriterCrashHooks.callWithHook(hook,
                () -> AeronClusterReplicationTransportProvider.callWithCrashHook(hook, operation));
    }

    /// Captures all crash-test bindings for an explicitly created worker thread.
    public static Runnable inheritCurrent(final Runnable action) {
        return WriterCrashHooks.inheritCurrent(
                AeronClusterReplicationTransportProvider.inheritCurrentCrashHook(
                        FileStoreCrashHooks.inheritCurrent(action)));
    }

        /// Returns the checkpoint sequence associated with the current write callback.
    ///
    /// @return current checkpoint sequence, or `-1` when none is active
    public static long currentCheckpointSequence() {
        return AeronClusterReplicationTransportProvider.currentCheckpointSequence();
    }

}
