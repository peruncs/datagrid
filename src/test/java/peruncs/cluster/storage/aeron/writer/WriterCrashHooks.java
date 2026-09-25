package peruncs.cluster.storage.aeron.writer;

import java.util.function.BiConsumer;

/// Test-only bridge arming writer crash hooks from other test packages.
///
/// The production seam is package-confined; this bridge lives in test sources
/// so it can never ship on a production classpath.
public final class WriterCrashHooks {
    private WriterCrashHooks() {
    }

    /// Runs an action with a writer crash hook bound to its dynamic scope.
    ///
    /// @param hook callback that receives the crash seam name and sequence
    /// @param action guarded write operation
    public static void runWithHook(final BiConsumer<String, Long> hook, final Runnable action) {
        CrashHook.runWithHook(hook, action);
    }

    /// Calls an operation with a writer crash hook bound to its dynamic scope.
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
        return CrashHook.callWithHook(hook, operation);
    }

    /// Captures the current writer hook for an explicitly created worker thread.
    ///
    /// @param action worker body
    /// @return wrapped action carrying the current hook
    public static Runnable inheritCurrent(final Runnable action) {
        return CrashHook.inheritCurrent(action);
    }
}
