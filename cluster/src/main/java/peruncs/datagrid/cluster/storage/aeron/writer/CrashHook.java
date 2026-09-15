package peruncs.datagrid.cluster.storage.aeron.writer;

import java.util.function.BiConsumer;

/**
 * Thread-local seam used by deterministic crash tests.
 *
 * <p>Normal writes pay only for a thread-local lookup. A test installs a hook
 * on the thread that owns the write, and must clear it when the test ends.
 * Hooks are not inherited by polling or worker threads. The seam is explicit
 * rather than reflective so forked children can arm the exact write boundary
 * they validate; applications must never install a hook in production.</p>
 *
 * <p>Throwing hooks are safe in unit tests. A blocking hook must be used only
 * by a forked child that the parent can terminate; blocking while holding a
 * publisher or coordinator monitor can otherwise deadlock the test.</p>
 */
public final class CrashHook {
    private static final ThreadLocal<BiConsumer<String, Long>> CURRENT = new ThreadLocal<>();

    private CrashHook() {
    }

    /**
     * Installs a hook for the current thread. Passing {@code null} removes the
     * current thread's hook.
     *
     * @param hook callback invoked at an armed crash point, or {@code null}
     */
    public static void install(final BiConsumer<String, Long> hook) {
        if (hook == null) CURRENT.remove();
        else CURRENT.set(hook);
    }

    /** Removes the crash hook installed for the current thread, if any. */
    public static void clear() {
        CURRENT.remove();
    }

    static void invoke(final String name, final long sequence) {
        final BiConsumer<String, Long> hook = CURRENT.get();
        if (hook != null) hook.accept(name, sequence);
    }
}
