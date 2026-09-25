package peruncs.cluster.node.store;

/// Reports whether the configured storage limit has been reached.
///
/// Construction policy: wired by the node assembly, never application API.
/// Public solely because the lifecycle lives in a sibling package.
public interface StorageSizeValidation {
    /// Reports whether another Store write must be rejected.
    ///
    /// @return `true` when the limit is reached
    boolean isStorageLimitReached();

    /// Returns a validation that never rejects, for read-only managers.
    ///
    /// @return validation that never reports the limit as reached
    static StorageSizeValidation notReached() {
        return () -> false;
    }
}
