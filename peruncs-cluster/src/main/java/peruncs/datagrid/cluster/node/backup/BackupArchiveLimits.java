package peruncs.datagrid.cluster.node.backup;

/// Budgets bounding backup restore and manifest reads.
///
/// The extraction budget is derived from an archive's declared entry sizes
/// when all are known and falls back to the absolute ceiling otherwise, so
/// legitimate large stores restore while decompression bombs still fail fast.
///
/// Public because it is a parameter of the [FilesystemVolumeBackupBackend]
/// factory that embedding applications call.
///
/// @param maxExtractedBytes absolute extraction ceiling in bytes
public record BackupArchiveLimits(long maxExtractedBytes) {
    /// Creates default limits with a generous absolute ceiling.
    ///
    /// @return default limits
    public static BackupArchiveLimits defaults() {
        return new BackupArchiveLimits(1L << 40);
    }

    /// Creates extraction budgets.
    ///
    /// @param maxExtractedBytes absolute ceiling for one restore or manifest read
    public BackupArchiveLimits {
        if (maxExtractedBytes <= 0L) {
            throw new IllegalArgumentException("maxExtractedBytes must be positive");
        }
    }
}
