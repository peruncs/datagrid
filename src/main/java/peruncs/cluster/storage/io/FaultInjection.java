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

    /// Every named point at which a test may inject a fault.
    public enum Point {
        AFTER_ABORT_OFFERED,
        AFTER_COMMIT_OFFER,
        AFTER_COMMIT_RECORDED,
        AFTER_COMMIT_RECORDED_BEFORE_BOUNDARY_UPDATE,
        AFTER_DATA_CHUNKS,
        AFTER_DICTIONARY_CHUNKS,
        AFTER_EXISTING_PUBLICATION_INSPECTION,
        AFTER_LOCAL_WRITE_BEFORE_COMMIT,
        AFTER_PREPARE,
        AFTER_PREPARE_BEFORE_LOCAL_WRITE,
        AFTER_PREPARE_FAILURE_ABORT_OFFERED,
        AFTER_PREVIOUS_STORAGE_MOVED,
        AFTER_RECOVERY_PUBLISHER_CREATED,
        AFTER_REGULAR_DELETE,
        AFTER_RENAME_BEFORE_DIRECTORY_SYNC,
        AFTER_STORAGE_RENAME_BEFORE_DIRECTORY_SYNC,
        AFTER_STORE_BACKUP_BEFORE_READY,
        AFTER_TEMP_WRITE_BEFORE_RENAME,
        BEFORE_COMMIT_GATE,
        BEFORE_COMMIT_OFFER,
        BEFORE_PREPARE,
        BEFORE_PUBLICATION_CONNECTED,
        BEFORE_PUBLISH_RENAME,
        BEFORE_TEMP_WRITE,
        DATA_CHUNK,
        DURING_FILE_WRITE
    }

    /// One fault point, with either a transaction sequence or filesystem path.
    @FunctionalInterface
    public interface Hook {
        /// Handles the named point; unused context is `-1` or `null`.
        void at(Point point, long sequence, Path path);
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
    public static void invoke(final Point point, final long sequence) {
        final Hook hook = CURRENT.isBound() ? CURRENT.get() : null;
        if (hook != null) hook.at(point, sequence, null);
    }

    /// Invokes a filesystem fault point.
    public static void invoke(final Point point, final Path path) {
        final Hook hook = CURRENT.isBound() ? CURRENT.get() : null;
        if (hook != null) hook.at(point, -1L, path);
    }
}
