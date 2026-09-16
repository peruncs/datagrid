package peruncs.datagrid.cluster.node.backup;

import peruncs.datagrid.cluster.node.replication.ReplicationCursor;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCursor;

import java.util.Objects;
import java.util.UUID;

/// Identifies one backup and the store generation it was taken from.
///
/// The timestamp orders backups and the manual slot separates ad-hoc backups
/// from scheduled ones. The remaining fields form the backup generation: the
/// cluster, store image, writer epoch, and recording the backup belongs to,
/// the node that published it, a random id that keeps concurrent publishers
/// from colliding on one shared volume, and a CRC over the archived content.
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
        UUID nodeId,
        UUID backupId,
        long digest) {
        /// Sentinel for an unknown numeric identity or digest.
    public static final long UNKNOWN = -1L;

        /// Validates the backup identity.
    public BackupMetadata {
        if (timestamp < 0L) {
            throw new IllegalArgumentException("timestamp must not be negative");
        }
        if (epoch < UNKNOWN || recordingId < UNKNOWN) {
            throw new IllegalArgumentException("epoch and recordingId must be -1 when unknown");
        }
        Objects.requireNonNull(backupId, "backupId");
    }

        /// Creates a backup identity for a new publication.
    ///
    /// The backup id is random, so two nodes publishing in the same
    /// millisecond still produce distinct archives. Generation fields come
    /// from the cursor: the store generation directly, and the cluster,
    /// epoch, recording, and node identities from an Aeron provider position.
    /// An Aeron cursor without a decodable identity is rejected; non-replicated
    /// cursors remain identity-free. The digest stays unknown until the backend
    /// has archived the content.
    ///
    /// @param timestamp  backup creation time
    /// @param manualSlot whether the backup uses the manual slot
    /// @param cursor     replication cursor stored with the backup, or `null`
    /// @return new backup metadata with a random backup id
    public static BackupMetadata New(final long timestamp, final boolean manualSlot, final ReplicationCursor cursor) {
        final AeronReplicationCursor aeron = decodeAeron(cursor);
        if (cursor != null && "aeron".equalsIgnoreCase(cursor.transport()) && aeron == null) {
            throw new IllegalArgumentException(
                    "an Aeron backup cursor must contain a decodable provider identity");
        }
        if (aeron != null && (aeron.clusterId() == null || aeron.storeGeneration() == null)) {
            throw new IllegalArgumentException(
                    "an Aeron backup cursor must contain cluster and Store-generation identity");
        }
        return new BackupMetadata(
                timestamp,
                manualSlot,
                aeron == null ? null : aeron.clusterId(),
                aeron == null ? (cursor == null ? null : cursor.storeGeneration()) : aeron.storeGeneration(),
                aeron == null ? UNKNOWN : aeron.epoch(),
                aeron == null ? UNKNOWN : aeron.recordingId(),
                aeron == null ? null : aeron.nodeId(),
                UUID.randomUUID(),
                UNKNOWN);
    }

        /// Returns a copy carrying the archived content digest.
    ///
    /// @param digest CRC over the archived content
    /// @return copy with the digest set
    public BackupMetadata withDigest(final long digest) {
        return new BackupMetadata(
                this.timestamp, this.manualSlot, this.clusterId, this.storeGeneration,
                this.epoch, this.recordingId, this.nodeId, this.backupId, digest);
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
    public boolean isCompatibleWith(final Identity configured) {
        Objects.requireNonNull(configured, "configured");
        return (configured.clusterId() == null || configured.clusterId().equals(this.clusterId)) &&
               (configured.storeGeneration() == null || configured.storeGeneration().equals(this.storeGeneration)) &&
               (configured.epoch() < 0L || configured.epoch() == this.epoch) &&
               (configured.recordingId() < 0L || configured.recordingId() == this.recordingId);
    }

        /// Verifies that archived metadata and its stored cursor describe one boundary.
    ///
    /// The configured-identity check alone cannot catch a corrupt or mixed
    /// archive: when recording is unconfigured, metadata and manifest can
    /// disagree about recording, and the outer cursor can disagree with its
    /// encoded Aeron position about generation or sequence. All three must
    /// agree before local files are touched.
    ///
    /// @param metadata selected backup metadata
    /// @param cursor   replication cursor archived with that backup
    /// @throws IllegalArgumentException when metadata and cursor disagree
    public static void requireConsistentWithCursor(final BackupMetadata metadata, final ReplicationCursor cursor) {
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(cursor, "cursor");
        if (!"aeron".equalsIgnoreCase(cursor.transport())) {
            final var generation = metadata.storeGeneration();
            if (!Objects.equals(generation, cursor.storeGeneration())) {
                throw new IllegalArgumentException(
                        "backup metadata store generation %s disagrees with archived cursor generation %s"
                                .formatted(generation, cursor.storeGeneration()));
            }
            return;
        }
        final AeronReplicationCursor aeron;
        try {
            if (!cursor.hasProviderPosition()) {
                throw new IllegalArgumentException("Aeron backup cursor carries no provider position");
            }
            aeron = AeronReplicationCursor.decode(cursor.providerPositionBytes());
        } catch (final RuntimeException unreadable) {
            throw new IllegalArgumentException("Aeron backup cursor provider position is undecodable", unreadable);
        }
        if (!Objects.equals(metadata.clusterId(), aeron.clusterId())) {
            throw new IllegalArgumentException(
                    "backup metadata cluster %s disagrees with archived cursor cluster %s"
                            .formatted(metadata.clusterId(), aeron.clusterId()));
        }
        final var generation = metadata.storeGeneration();
        final var aeronGeneration = aeron.storeGeneration();
        final var cursorGeneration = cursor.storeGeneration();
        if (!Objects.equals(generation, aeronGeneration) ||
            !Objects.equals(generation, cursorGeneration) ||
            !Objects.equals(aeronGeneration, cursorGeneration)) {
            throw new IllegalArgumentException(
                    "backup metadata generation %s disagrees with archived cursor generation %s/%s"
                            .formatted(generation, aeronGeneration, cursorGeneration));
        }
        if (metadata.epoch() != aeron.epoch()) {
            throw new IllegalArgumentException(
                    "backup metadata epoch %s disagrees with archived cursor epoch %s"
                            .formatted(metadata.epoch(), aeron.epoch()));
        }
        if (metadata.recordingId() != aeron.recordingId()) {
            throw new IllegalArgumentException(
                    "backup metadata recording %s disagrees with archived cursor recording %s"
                            .formatted(metadata.recordingId(), aeron.recordingId()));
        }
        if (cursor.logicalSequence() != aeron.sequence()) {
            throw new IllegalArgumentException(
                    "archived cursor sequence %s disagrees with encoded Aeron sequence %s"
                            .formatted(cursor.logicalSequence(), aeron.sequence()));
        }
    }

        /// Extracts the Aeron identity from a cursor on a best-effort basis.
    ///
    /// @param cursor cursor to inspect, or `null`
    /// @return decoded Aeron identity, or `null` when the cursor carries none
    private static AeronReplicationCursor decodeAeron(final ReplicationCursor cursor) {
        if (cursor == null || !cursor.hasProviderPosition() ||
            !"aeron".equalsIgnoreCase(cursor.transport())) {
            return null;
        }
        try {
            return AeronReplicationCursor.decode(cursor.providerPositionBytes());
        } catch (final RuntimeException unreadable) {
            return null;
        }
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

                /// Derives the identity from a replication cursor.
        ///
        /// The store generation comes from the cursor directly; the cluster,
        /// epoch, and recording come from an Aeron provider position when the
        /// cursor carries a decodable one. Anything unavailable stays unknown.
        ///
        /// @param cursor cursor to inspect, or `null`
        /// @return node identity, unknown where the cursor says nothing
        public static Identity of(final ReplicationCursor cursor) {
            final AeronReplicationCursor aeron = decodeAeron(cursor);
            return new Identity(
                    aeron == null ? null : aeron.clusterId(),
                    aeron == null ? (cursor == null ? null : cursor.storeGeneration()) : aeron.storeGeneration(),
                    aeron == null ? UNKNOWN : aeron.epoch(),
                    aeron == null ? UNKNOWN : aeron.recordingId());
        }

                /// Fills unknown dimensions from a fallback identity.
        ///
        /// Known dimensions of this identity win; only unknown ones are taken
        /// from the fallback. Used to merge the replication provider's view
        /// with the durable local cursor.
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
        /// candidate. This is used after reading a backup cursor so metadata
        /// and the archived cursor cannot disagree before local files change.
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
