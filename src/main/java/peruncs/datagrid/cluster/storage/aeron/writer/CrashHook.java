package peruncs.datagrid.cluster.storage.aeron.writer;

import java.util.Objects;
import java.util.function.BiConsumer;

/// Scoped seam used by deterministic crash tests across the Aeron replication
/// packages.
///
/// This type is the single crash-hook seam for writer and transport code. A
/// test binds a hook around the exact write operation it owns; structured
/// subtasks inherit the binding, while unrelated work cannot observe it. The
/// seam is explicit rather than reflective so forked children can arm the
/// exact write boundary they validate; applications must never install a
/// hook.
///
/// Throwing hooks are safe in unit tests. A blocking hook must be used only
/// by a forked child that the parent can terminate; blocking while holding a
/// publisher or coordinator monitor can otherwise deadlock the test.
public final class CrashHook {
    private static final ScopedValue<BiConsumer<String, Long>> CURRENT = ScopedValue.newInstance();

    private CrashHook() {
    }

    /// Runs an action with a crash hook bound to its dynamic scope.
    ///
    /// @param hook   callback invoked at an armed crash point
    /// @param action guarded write operation
    public static void runWithHook(final BiConsumer<String, Long> hook, final Runnable action) {
        Objects.requireNonNull(hook, "hook");
        Objects.requireNonNull(action, "action");
        ScopedValue.where(CURRENT, hook).run(action);
    }

    /// Calls an operation with a crash hook bound to its dynamic scope.
    ///
    /// @param <T>       operation result type
    /// @param <X>       operation failure type
    /// @param hook      callback invoked at an armed crash point
    /// @param operation guarded write operation
    /// @return operation result
    /// @throws X when the operation fails
    public static <T, X extends Throwable> T callWithHook(
            final BiConsumer<String, Long> hook,
            final ScopedValue.CallableOp<? extends T, X> operation
    ) throws X {
        Objects.requireNonNull(hook, "hook");
        Objects.requireNonNull(operation, "operation");
        return ScopedValue.where(CURRENT, hook).call(operation);
    }

    /// Captures the current hook for an explicitly created unstructured thread.
    /// Structured task scopes inherit scoped values automatically; an
    /// independently started worker must opt in explicitly.
    ///
    /// @param action worker body
    /// @return wrapped action carrying the current hook
    public static Runnable inheritCurrent(final Runnable action) {
        Objects.requireNonNull(action, "action");
        final BiConsumer<String, Long> hook = CURRENT.isBound() ? CURRENT.get() : null;
        return hook == null ? action : () -> ScopedValue.where(CURRENT, hook).run(action);
    }

    /// Invokes the bound crash hook when one is present.
    ///
    /// Hot paths call this unconditionally; an unbound scope costs one scoped
    /// value check and no allocation.
    ///
    /// @param name     crash point name
    /// @param sequence transaction sequence observed at the crash point
    public static void invoke(final String name, final long sequence) {
        final BiConsumer<String, Long> hook = CURRENT.isBound() ? CURRENT.get() : null;
        if (hook != null) hook.accept(name, sequence);
    }
}
