package peruncs.datagrid.cluster.storage.aeron.writer;

import java.util.function.BiConsumer;

/// Scoped seam used by deterministic crash tests.
///
/// This type is deliberately package-confined: only the writer package may arm
/// hooks, so production classpaths can never install throwing or blocking
/// callbacks into commit paths. Tests in other packages arm writer hooks
/// through the test-only bridge in this package's test sources.
///
/// A test binds a hook around the exact write operation it owns. Structured
/// subtasks inherit the binding, while unrelated work cannot observe it.
/// The seam is explicit rather than reflective so forked children can arm the
/// exact write boundary they validate; applications must never install a hook.
///
/// Throwing hooks are safe in unit tests. A blocking hook must be used only
/// by a forked child that the parent can terminate; blocking while holding a
/// publisher or coordinator monitor can otherwise deadlock the test.
final class CrashHook {
    private static final ScopedValue<BiConsumer<String, Long>> CURRENT = ScopedValue.newInstance();

    private CrashHook() {
    }

        /// Runs an action with a crash hook bound to its dynamic scope.
    ///
    /// @param hook callback invoked at an armed crash point, or `null`
    static void runWithHook(final BiConsumer<String, Long> hook, final Runnable action) {
        if (hook == null) throw new NullPointerException("hook");
        if (action == null) throw new NullPointerException("action");
        ScopedValue.where(CURRENT, hook).run(action);
    }

        /// Calls an operation with a crash hook bound to its dynamic scope.
    static <T, X extends Throwable> T callWithHook(
            final BiConsumer<String, Long> hook,
            final ScopedValue.CallableOp<? extends T, X> operation
    ) throws X {
        if (hook == null) throw new NullPointerException("hook");
        if (operation == null) throw new NullPointerException("operation");
        return ScopedValue.where(CURRENT, hook).call(operation);
    }

    /// Captures the current hook for an explicitly created unstructured thread.
    /// Structured task scopes inherit scoped values automatically; an independently
    /// started worker must opt in explicitly.
    static Runnable inheritCurrent(final Runnable action) {
        if (action == null) throw new NullPointerException("action");
        final BiConsumer<String, Long> hook = CURRENT.isBound() ? CURRENT.get() : null;
        return hook == null ? action : () -> ScopedValue.where(CURRENT, hook).run(action);
    }

    static void invoke(final String name, final long sequence) {
        final BiConsumer<String, Long> hook = CURRENT.isBound() ? CURRENT.get() : null;
        if (hook != null) hook.accept(name, sequence);
    }
}
