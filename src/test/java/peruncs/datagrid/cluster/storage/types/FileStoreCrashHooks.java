package peruncs.datagrid.cluster.storage.types;

import java.nio.file.Path;
import java.util.function.BiConsumer;

/// Test-only bridge arming file-store crash hooks from other test packages.
///
/// The production seam is package-confined; this bridge lives in test sources
/// so it can never ship on a production classpath.
public final class FileStoreCrashHooks {
    private FileStoreCrashHooks() {
    }

    /// Runs an action with a file-store crash hook bound to its dynamic scope.
    public static void runWithHook(final BiConsumer<String, Path> hook, final Runnable action) {
        AtomicFileStore.runWithTestHook(hook, action);
    }

    /// Calls an operation with a file-store crash hook bound to its dynamic scope.
    public static <T, X extends Throwable> T callWithHook(
            final BiConsumer<String, Path> hook,
            final ScopedValue.CallableOp<? extends T, X> operation
    ) throws X {
        return AtomicFileStore.callWithTestHook(hook, operation);
    }

    /// Captures the current file-store hook for an explicitly created worker thread.
    public static Runnable inheritCurrent(final Runnable action) {
        return AtomicFileStore.inheritCurrentTestHook(action);
    }
}
