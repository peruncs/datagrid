package peruncs.cluster.storage.io;

import java.nio.file.Path;
import java.util.Objects;

/// Scoped fault hook shared by transport, backup, and atomic-file crash tests.
///
/// Production calls do one bound check and allocate nothing. Structured tasks
/// inherit the binding; explicitly created threads use [#inheritCurrent].
public final class FaultInjection {
    private static final ScopedValue<Hook> CURRENT = ScopedValue.newInstance();

    private FaultInjection() {
    }

    /// One fault point, with either a transaction sequence or filesystem path.
    @FunctionalInterface
    public interface Hook {
        /// Handles the named point; unused context is `-1` or `null`.
        void at(String point, long sequence, Path path);
    }

    /// Runs an operation with a hook bound to its dynamic scope.
    public static void runWithHook(final Hook hook, final Runnable action) {
        Objects.requireNonNull(hook, "hook");
        Objects.requireNonNull(action, "action");
        ScopedValue.where(CURRENT, hook).run(action);
    }

    /// Calls an operation with a hook bound to its dynamic scope.
    public static <T, X extends Throwable> T callWithHook(
            final Hook hook,
            final ScopedValue.CallableOp<? extends T, X> operation
    ) throws X {
        Objects.requireNonNull(hook, "hook");
        Objects.requireNonNull(operation, "operation");
        return ScopedValue.where(CURRENT, hook).call(operation);
    }

    /// Captures the current hook for an explicitly started unstructured thread.
    public static Runnable inheritCurrent(final Runnable action) {
        Objects.requireNonNull(action, "action");
        final Hook hook = CURRENT.isBound() ? CURRENT.get() : null;
        return hook == null ? action : () -> ScopedValue.where(CURRENT, hook).run(action);
    }

    /// Invokes a transaction fault point.
    public static void invoke(final String point, final long sequence) {
        final Hook hook = CURRENT.isBound() ? CURRENT.get() : null;
        if (hook != null) hook.at(point, sequence, null);
    }

    /// Invokes a filesystem fault point.
    public static void invoke(final String point, final Path path) {
        final Hook hook = CURRENT.isBound() ? CURRENT.get() : null;
        if (hook != null) hook.at(point, -1L, path);
    }
}
