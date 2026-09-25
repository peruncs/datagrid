package peruncs.cluster.node.backup;

/// Budgets bounding backup restore, digest, and manifest reads.
///
/// The extraction byte budget is the absolute ceiling for one archive when
/// its entries do not declare trustworthy sizes; the entry budget bounds how
/// many files an archive may declare, so a hostile archive cannot exhaust
/// inodes while staying inside a large byte budget. Both bounds are
/// configurable by operators through the explicit-budget factory on [FilesystemVolumeBackupBackend],
/// while [defaults()] keeps the previous behavior for callers that do not
/// choose explicit budgets.
///
/// @param maxExtractedBytes absolute extraction byte ceiling
/// @param maxArchiveEntries maximum number of entries an archive may declare
record BackupArchiveLimits(long maxExtractedBytes, int maxArchiveEntries) {
    /// Default absolute extraction ceiling: one tebibyte.
    static final long DEFAULT_MAX_EXTRACTED_BYTES = 1L << 40;
    /// Budgeted bytes per archive entry when deriving the entry budget.
    static final long BYTES_PER_ENTRY = 64L * 1024L;
    /// Hard upper bound for derived entry budgets, independent of byte budget.
    static final int MAX_ENTRY_BUDGET = 1 << 24;

    /// Creates default limits: a generous byte ceiling and an entry budget
    /// proportional to it, capped independently.
    ///
    /// @return default limits
    static BackupArchiveLimits defaults() {
        return of(DEFAULT_MAX_EXTRACTED_BYTES);
    }

    /// Creates limits whose entry budget is proportional to the byte budget.
    ///
    /// At most one entry is allowed per [BYTES_PER_ENTRY] of byte budget,
    /// with a minimum of one entry and the [MAX_ENTRY_BUDGET] cap. A small
    /// byte budget therefore receives a small entry budget instead of the
    /// former fixed million-entry allowance.
    ///
    /// @param maxExtractedBytes absolute extraction byte ceiling
    /// @return limits with a proportional entry budget
    static BackupArchiveLimits of(final long maxExtractedBytes) {
        return new BackupArchiveLimits(maxExtractedBytes, entriesFor(maxExtractedBytes));
    }

    /// Creates limits with explicit budgets.
    ///
    /// @param maxExtractedBytes absolute extraction byte ceiling
    /// @param maxArchiveEntries maximum number of archive entries
    /// @return limits
    static BackupArchiveLimits of(final long maxExtractedBytes, final int maxArchiveEntries) {
        return new BackupArchiveLimits(maxExtractedBytes, maxArchiveEntries);
    }

    /// Derives the entry budget proportional to a byte budget.
    ///
    /// @param maxExtractedBytes absolute extraction byte ceiling
    /// @return entry budget between one and [MAX_ENTRY_BUDGET]
    private static int entriesFor(final long maxExtractedBytes) {
        if (maxExtractedBytes <= 0L) {
            return 1;
        }
        return (int) Math.clamp(maxExtractedBytes / BYTES_PER_ENTRY, 1L, MAX_ENTRY_BUDGET);
    }

    /// Validates the budgets.
    BackupArchiveLimits {
        if (maxExtractedBytes <= 0L) {
            throw new IllegalArgumentException("maxExtractedBytes must be positive");
        }
        if (maxArchiveEntries <= 0) {
            throw new IllegalArgumentException("maxArchiveEntries must be positive");
        }
    }
}
