package peruncs.cluster.api;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/// Describes a successfully published backup.
///
/// @param id unique backup identifier
/// @param createdAt publication time
/// @param sequence replication sequence at the backup boundary, or -1 if unknown
/// @param manual whether the backup uses the retained manual slot
///
/// @since 1.0
public record BackupInfo(UUID id, Instant createdAt, long sequence, boolean manual) {
    public BackupInfo {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(createdAt, "createdAt");
        if (sequence < -1L) throw new IllegalArgumentException("sequence must be -1 when unknown");
    }

    /// Whether the replication sequence is known.
    public boolean hasSequence() {
        return this.sequence >= 0L;
    }
}
