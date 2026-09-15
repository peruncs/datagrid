package peruncs.datagrid.cluster.storage.types;

import java.nio.file.Path;
import java.util.function.BiConsumer;

/// Explicit, reflection-free bridge used by forked metadata crash tests.
public final class AtomicFileStoreCrashHook {
    private AtomicFileStoreCrashHook() {
    }

        /// Runs an action with a hook bound to its dynamic scope.
    ///
    /// @param hook callback for crash-test phases
    public static void runWithHook(final BiConsumer<String, Path> hook, final Runnable action) {
        AtomicFileStore.runWithTestHook(hook, action);
    }

        /// Calls an operation with a hook bound to its dynamic scope.
    public static <T, X extends Throwable> T callWithHook(
            final BiConsumer<String, Path> hook,
            final java.lang.ScopedValue.CallableOp<? extends T, X> operation
    ) throws X {
        return AtomicFileStore.callWithTestHook(hook, operation);
    }

    /// Captures the current hook for an explicitly created unstructured thread.
    public static Runnable inheritCurrent(final Runnable action) {
        return AtomicFileStore.inheritCurrentTestHook(action);
    }
}
