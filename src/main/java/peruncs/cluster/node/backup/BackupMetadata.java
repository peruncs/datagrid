package peruncs.cluster.node.backup;

import peruncs.cluster.storage.ReplicationPosition;

import java.util.Comparator;
import java.util.Objects;
import java.util.UUID;

/// Identifies one backup and the store generation it was taken from.
///
/// The timestamp orders backups and the manual slot separates ad-hoc backups
/// from scheduled ones. The remaining fields form the backup generation: the
/// cluster, store image, writer epoch, recording, and replication sequence the
/// backup belongs to, the node that published it, a random id that keeps
/// concurrent publishers from colliding on one shared volume, and a CRC over
/// the archived content.
///
/// Identity fields are unknown (`null` for UUIDs, `-1` for numbers) when the
/// backup was taken without replication identity, for example on a node with
/// replication disabled. A node with a configured replicated identity rejects
/// an unknown backup dimension because it cannot prove that the archive belongs
/// to the same Store generation.
///
/// @param timestamp       backup creation time
/// @param manualSlot      whether the backup uses the manual slot
/// @param clusterId       replication cluster identity, or `null` when unknown
/// @param storeGeneration Store image identity, or `null` when unknown
/// @param epoch           writer epoch, or `-1` when unknown
/// @param recordingId     replication recording identity, or `-1` when unknown
/// @param logicalSequence replication sequence at the backup boundary, or `-1` when unknown
/// @param nodeId          node that published the backup, or `null` when unknown
/// @param backupId        random id unique to this publication
/// @param digest          CRC over the archived content, or `-1` when unknown
public record BackupMetadata(
        long timestamp,
        boolean manualSlot,
        UUID clusterId,
        UUID storeGeneration,
        long epoch,
        long recordingId,
        long logicalSequence,
        long fencingToken,
        long recordingPosition,
        UUID nodeId,
        UUID backupId,
        long digest) {
        /// Sentinel for an unknown numeric identity, sequence, or digest.
    public static final long UNKNOWN = -1L;

        /// Orders backups oldest first: replication sequence when known, the
        /// creation timestamp as the fallback, and the random backup id to
        /// break exact ties deterministically.
    ///
    /// Backups carrying a known sequence sort after sequence-less backups, so
    /// a mixed volume never interleaves the two ordering domains
    /// non-transitively. Nodes sharing a backup volume must keep their clocks
    /// NTP-disciplined because the timestamp fallback is wall-clock based.
    public static final Comparator<BackupMetadata> OLDEST_FIRST =
            Comparator.comparingInt((BackupMetadata backup) -> backup.logicalSequence() >= 0L ? 1 : 0)
                    .thenComparingLong(backup -> backup.logicalSequence() >= 0L
                            ? backup.logicalSequence()
                            : backup.timestamp())
                    .thenComparing(BackupMetadata::backupId);

        /// Orders backups newest first, the reverse of [OLDEST_FIRST].
    public static final Comparator<BackupMetadata> NEWEST_FIRST = OLDEST_FIRST.reversed();

        /// Validates the backup identity.
    public BackupMetadata {
        if (timestamp < 0L) {
            throw new IllegalArgumentException("timestamp must not be negative");
        }
        if (epoch < UNKNOWN || recordingId < UNKNOWN || logicalSequence < UNKNOWN ||
            fencingToken < UNKNOWN || recordingPosition < UNKNOWN) {
            throw new IllegalArgumentException("replication identity values must be -1 when unknown");
        }
        Objects.requireNonNull(backupId, "backupId");
    }

        /// Creates a backup identity for a new publication.
    ///
    /// The backup id is random, so two nodes publishing in the same
    /// millisecond still produce distinct archives. Generation fields come
    /// from the typed position. The digest stays unknown
    /// until the backend has archived the content.
    ///
    /// @param timestamp  backup creation time
    /// @param manualSlot whether the backup uses the manual slot
    /// @param position   replication boundary stored in the backup identity, or `null`
    /// @return new backup metadata with a random backup id
    public static BackupMetadata create(final long timestamp, final boolean manualSlot, final ReplicationPosition position) {
        final boolean replicated = position != null && position.clusterId() != null;
        return new BackupMetadata(
                timestamp,
                manualSlot,
                replicated ? position.clusterId() : null,
                replicated ? position.storeGeneration() : null,
                replicated ? position.epoch() : UNKNOWN,
                replicated ? position.recordingId() : UNKNOWN,
                replicated && position.sequence() >= 0L ? position.sequence() : UNKNOWN,
                replicated ? position.fencingToken() : UNKNOWN,
                replicated ? position.prepareStartPosition() : UNKNOWN,
                replicated ? position.nodeId() : null,
                UUID.randomUUID(),
                UNKNOWN);
    }

        /// Returns a copy carrying the archived content digest.
    ///
    /// @param digest CRC over the archived content
    /// @return copy with the digest set
    BackupMetadata withDigest(final long digest) {
        return new BackupMetadata(
                this.timestamp, this.manualSlot, this.clusterId, this.storeGeneration,
                this.epoch, this.recordingId, this.logicalSequence, this.fencingToken,
                this.recordingPosition, this.nodeId, this.backupId, digest);
    }

        /// Returns the comparison identity of this backup.
    ///
    /// This is the view [Identity#matches] compares, so compatibility checks
    /// and post-restore validation share exactly one rule.
    ///
    /// @return identity built from the generation fields
    Identity identity() {
        return new Identity(this.clusterId, this.storeGeneration, this.epoch, this.recordingId);
    }

        /// Reports whether this backup may serve a node with the given identity.
    ///
    /// Every configured dimension must be present on the backup and agree. A
    /// backup from another cluster, generation, epoch, or recording — or one
    /// missing any identity required by this node — is rejected. A fully
    /// unconfigured node still accepts identity-free backups.
    ///
    /// @param configured node identity to check against
    /// @return `true` when no known dimension contradicts
    boolean isCompatibleWith(final Identity configured) {
        Objects.requireNonNull(configured, "configured");
        return configured.matches(this.identity());
    }

    /// Returns the stored Archive boundary used by retention, when present.
    ReplicationPosition retentionBoundary() {
        if (this.clusterId == null || this.nodeId == null || this.storeGeneration == null ||
            this.epoch < 0L || this.recordingId < 0L || this.logicalSequence < 0L ||
            this.fencingToken <= 0L || this.recordingPosition < 0L) {
            return null;
        }
        return new ReplicationPosition(this.clusterId, this.storeGeneration, this.epoch,
                this.recordingId, this.logicalSequence, this.recordingPosition, this.fencingToken, this.nodeId);
    }

        /// The node identity a backup is checked against.
    ///
    /// Dimensions are unknown (`null` for UUIDs, `-1` for numbers) only when
    /// the node has no configured replication identity. A configured dimension
    /// must be present and equal on a candidate backup; unknown backup values
    /// are not treated as wildcards.
    ///
    /// @param clusterId       configured cluster identity, or `null`
    /// @param storeGeneration configured Store image identity, or `null`
    /// @param epoch           configured writer epoch, or `-1`
    /// @param recordingId     configured recording identity, or `-1`
    public record Identity(
            UUID clusterId,
            UUID storeGeneration,
            long epoch,
            long recordingId) {
                /// Creates an identity with every dimension unknown.
        ///
        /// @return fully unknown identity, which accepts every backup
        public static Identity unknown() {
            return new Identity(null, null, UNKNOWN, UNKNOWN);
        }

                /// Derives the identity from a replication position.
        ///
        /// The store generation, cluster, epoch, and recording come from the
        /// typed position. Anything unavailable stays unknown.
        ///
        /// @param position position to inspect, or `null`
        /// @return node identity, unknown where the position says nothing
        public static Identity of(final ReplicationPosition position) {
            return position == null ? unknown() : new Identity(
                    position.clusterId(), position.storeGeneration(), position.epoch(), position.recordingId());
        }

                /// Fills unknown dimensions from a fallback identity.
        ///
        /// Known dimensions of this identity win; only unknown ones are taken
        /// from the fallback. Used to merge the replication provider's view
        /// with the durable local position.
        ///
        /// @param fallback fallback identity
        /// @return merged identity
        public Identity fillUnknowns(final Identity fallback) {
            Objects.requireNonNull(fallback, "fallback");
            return new Identity(
                    this.clusterId != null ? this.clusterId : fallback.clusterId,
                    this.storeGeneration != null ? this.storeGeneration : fallback.storeGeneration,
                    this.epoch >= 0L ? this.epoch : fallback.epoch,
                    this.recordingId >= 0L ? this.recordingId : fallback.recordingId);
        }

                /// Reports whether this configured identity accepts a candidate.
        ///
        /// Every known configured dimension must be present and equal on the
        /// candidate. This is the single compatibility rule used both to
        /// select backups and to validate metadata after a restore.
        ///
        /// @param candidate candidate identity
        /// @return `true` when the candidate belongs to this identity
        public boolean matches(final Identity candidate) {
            Objects.requireNonNull(candidate, "candidate");
            return (this.clusterId == null || this.clusterId.equals(candidate.clusterId)) &&
                   (this.storeGeneration == null || this.storeGeneration.equals(candidate.storeGeneration)) &&
                   (this.epoch < 0L || this.epoch == candidate.epoch) &&
                   (this.recordingId < 0L || this.recordingId == candidate.recordingId);
        }
    }
}
