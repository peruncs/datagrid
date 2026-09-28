package peruncs.cluster.api;

/// Selects the retention slot for a backup request.
///
/// @since 1.0
public enum BackupSlot {
    /// The scheduled slot, subject to normal retention.
    SCHEDULED,
    /// The manual slot, retained beyond scheduled backups.
    MANUAL
}
