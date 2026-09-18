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
    /// @param action guarded write operation
    public static void runWithHook(final BiConsumer<String, Long> hook, final Runnable action) {
        WriterCrashHooks.runWithHook(hook, () ->
                AeronClusterReplicationTransportProvider.runWithCrashHook(hook, action));
    }

        /// Calls an operation with writer and provider hooks bound to its scope.
    ///
    /// @param <T> operation result type
    /// @param <X> operation failure type
    /// @param hook callback that receives the crash seam name and sequence
    /// @param operation guarded write operation
    /// @return operation result
    /// @throws X when the operation fails
    public static <T, X extends Throwable> T callWithHook(
            final BiConsumer<String, Long> hook,
            final ScopedValue.CallableOp<? extends T, X> operation
    ) throws X {
        return WriterCrashHooks.callWithHook(hook,
                () -> AeronClusterReplicationTransportProvider.callWithCrashHook(hook, operation));
    }

    /// Captures all crash-test bindings for an explicitly created worker thread.
    ///
    /// @param action worker body
    /// @return wrapped action carrying the current bindings
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
