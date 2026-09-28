package peruncs.cluster.api;

import java.util.Objects;

/// Reports the last backup outcome independently of replication health.
///
/// A durable archive can coexist with a failed pruning or retention step, so
/// post-publication maintenance is reported separately from backup failure.
///
/// @param lastFailed whether the last completed backup failed
/// @param lastFailureMessage failure detail, or empty when none
/// @param lastSuccessEpochMillis last successful backup time, or -1 if none
/// @param maintenanceFailed whether post-publication maintenance failed
/// @param maintenanceFailureMessage maintenance failure detail, or empty when none
///
/// @since 1.0
public record BackupStatus(
        boolean lastFailed,
        String lastFailureMessage,
        long lastSuccessEpochMillis,
        boolean maintenanceFailed,
        String maintenanceFailureMessage
) {
    public BackupStatus {
        Objects.requireNonNull(lastFailureMessage, "lastFailureMessage");
        if (lastSuccessEpochMillis < -1L) throw new IllegalArgumentException("lastSuccessEpochMillis must be -1 when unknown");
        Objects.requireNonNull(maintenanceFailureMessage, "maintenanceFailureMessage");
    }

    /// Empty backup history.
    public static BackupStatus empty() {
        return new BackupStatus(false, "", -1L, false, "");
    }

    /// Whether any backup has succeeded.
    public boolean hasLastSuccess() { return this.lastSuccessEpochMillis >= 0L; }

    /// Whether the last backup failed.
    public boolean hasLastFailure() { return this.lastFailed; }

    /// Whether post-publication maintenance failed.
    public boolean hasMaintenanceFailure() { return this.maintenanceFailed; }
}
