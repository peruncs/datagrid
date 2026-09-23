package peruncs.datagrid.cluster.node.backup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cluster.errors.NodeException;
import peruncs.datagrid.cluster.storage.ReplicationCursor;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCursor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies backup generation identity: derivation from cursors,
/// compatibility filtering, and the archive file-name round trip.
class BackupMetadataTest {
    private static final UUID CLUSTER = UUID.randomUUID();
    private static final UUID GENERATION = UUID.randomUUID();
    private static final UUID NODE = UUID.randomUUID();

    private static ReplicationCursor aeronCursor(final long sequence) {
        return ReplicationCursor.of("aeron", GENERATION, sequence,
                new AeronReplicationCursor(CLUSTER, NODE, GENERATION, 5L, 7L, 42L, 0L, sequence).encode());
    }

    /// Verifies backup identity fields derive from the Aeron cursor while the content digest stays unknown until publication.
    @Test
    void derivesGenerationFromAnAeronCursor() {
        final BackupMetadata backup = BackupMetadata.create(100L, false, aeronCursor(7L));

        assertEquals(100L, backup.timestamp());
        assertFalse(backup.manualSlot());
        assertEquals(CLUSTER, backup.clusterId());
        assertEquals(GENERATION, backup.storeGeneration());
        assertEquals(5L, backup.epoch());
        assertEquals(42L, backup.recordingId());
        assertEquals(NODE, backup.nodeId());
        assertEquals(7L, backup.logicalSequence());
        assertNotNull(backup.backupId());
        assertEquals(BackupMetadata.UNKNOWN, backup.digest());
    }

    /// Verifies missing cursors leave generation identity unknown while a corrupt Aeron position is rejected.
    @Test
    void leavesIdentityUnknownWithoutACursor() {
        final BackupMetadata missing = BackupMetadata.create(100L, true, null);
        assertTrue(missing.manualSlot());
        assertNull(missing.clusterId());
        assertNull(missing.storeGeneration());
        assertEquals(BackupMetadata.UNKNOWN, missing.epoch());
        assertEquals(BackupMetadata.UNKNOWN, missing.recordingId());
        assertNull(missing.nodeId());
        assertNotNull(missing.backupId());

        final BackupMetadata plain =
                BackupMetadata.create(100L, false, new ReplicationCursor("none", null, -1L, ""));
        assertNull(plain.clusterId());
        assertNull(plain.storeGeneration());

        /* A corrupt Aeron provider position cannot identify the Store image;
         * publishing it would create a backup that a configured reader could
         * mistake for a compatible generation. */
        assertThrows(IllegalArgumentException.class, () -> BackupMetadata.create(
                100L, false, new ReplicationCursor("aeron", GENERATION, 7L, "deadbeef")));
    }

    /// Verifies every publication gets a distinct backup id even for the same cursor and timestamp.
    @Test
    void everyPublicationGetsADistinctBackupId() {
        final ReplicationCursor cursor = aeronCursor(7L);

        assertNotEquals(BackupMetadata.create(100L, false, cursor).backupId(),
                BackupMetadata.create(100L, false, cursor).backupId());
    }

    /// Verifies compatibility rejects only known identity contradictions and accepts unknown node identities.
    @Test
    void compatibilityRejectsOnlyKnownContradictions() {
        final BackupMetadata backup = BackupMetadata.create(100L, false, aeronCursor(7L));

        assertTrue(backup.isCompatibleWith(BackupMetadata.Identity.unknown()),
                "unknown node identity must accept every backup");
        assertTrue(backup.isCompatibleWith(new BackupMetadata.Identity(CLUSTER, GENERATION, 5L, 42L)));
        assertFalse(backup.isCompatibleWith(new BackupMetadata.Identity(UUID.randomUUID(), GENERATION, 5L, 42L)));
        assertFalse(backup.isCompatibleWith(new BackupMetadata.Identity(CLUSTER, UUID.randomUUID(), 5L, 42L)));
        assertFalse(backup.isCompatibleWith(new BackupMetadata.Identity(CLUSTER, GENERATION, 6L, 42L)));
        assertFalse(backup.isCompatibleWith(new BackupMetadata.Identity(CLUSTER, GENERATION, 5L, 43L)));

        final BackupMetadata unknown = BackupMetadata.create(100L, false, null);
        assertFalse(unknown.isCompatibleWith(new BackupMetadata.Identity(CLUSTER, GENERATION, 5L, 42L)),
                "a configured replicated node must reject an unidentifiable backup");
        assertTrue(unknown.isCompatibleWith(BackupMetadata.Identity.unknown()),
                "an unreplicated node has no identity constraint");
    }

    /// Verifies provider and local cursor identities merge by filling unknown fields from the local view.
    @Test
    void identityMergesProviderAndLocalViews() {
        final BackupMetadata.Identity provider =
                new BackupMetadata.Identity(CLUSTER, null, BackupMetadata.UNKNOWN, 42L);
        final BackupMetadata.Identity local = BackupMetadata.Identity.of(aeronCursor(7L));

        final BackupMetadata.Identity merged = provider.fillUnknowns(local);
        assertEquals(CLUSTER, merged.clusterId());
        assertEquals(GENERATION, merged.storeGeneration());
        assertEquals(5L, merged.epoch());
        assertEquals(42L, merged.recordingId());
    }

    /// Verifies the archive file name round-trips all backup selection fields through formatting and parsing.
    @Test
    void fileNameRoundTripsTheSelectionFields(@TempDir final Path volume) {
        final BackupMetadata backup = BackupMetadata.create(1700000000000L, true, aeronCursor(7L));
        final String name = BackupArchive.toArchiveFileName(backup);

        assertTrue(BackupArchive.isBackupFileName(name));
        final BackupMetadata parsed = BackupArchive.parseMetadata(name, volume);
        assertEquals(backup.timestamp(), parsed.timestamp());
        assertEquals(backup.manualSlot(), parsed.manualSlot());
        assertEquals(backup.clusterId(), parsed.clusterId());
        assertEquals(backup.storeGeneration(), parsed.storeGeneration());
        assertEquals(backup.epoch(), parsed.epoch());
        assertEquals(backup.recordingId(), parsed.recordingId());
        assertEquals(backup.logicalSequence(), parsed.logicalSequence());
        assertEquals(backup.backupId(), parsed.backupId());
    }

    /// Verifies matching backup metadata and cursor pass the consistency check.
    @Test
    void consistentMetadataAndCursorPass() {
        final ReplicationCursor cursor = aeronCursor(7L);
        final BackupMetadata backup = BackupMetadata.create(100L, false, cursor);
        BackupMetadata.requireConsistentWithCursor(backup, cursor);
    }

    /// Verifies metadata and cursor with mismatched cluster ids are rejected.
    @Test
    void rejectsClusterMismatchBetweenMetadataAndCursor() {
        final ReplicationCursor cursor = aeronCursor(7L);
        final BackupMetadata backup = BackupMetadata.create(100L, false, cursor);
        final ReplicationCursor foreign = ReplicationCursor.of("aeron", GENERATION, 7L,
                new AeronReplicationCursor(UUID.randomUUID(), NODE, GENERATION, 5L, 7L, 42L, 0L, 7L).encode());
        assertThrows(NodeException.class,
                () -> BackupMetadata.requireConsistentWithCursor(backup, foreign));
    }

    /// Verifies metadata and cursor with mismatched Store generations are rejected.
    @Test
    void rejectsGenerationMismatchBetweenMetadataAndCursor() {
        final ReplicationCursor cursor = aeronCursor(7L);
        final BackupMetadata backup = BackupMetadata.create(100L, false, cursor);
        final UUID otherGeneration = UUID.randomUUID();
        final ReplicationCursor foreign = ReplicationCursor.of("aeron", otherGeneration, 7L,
                new AeronReplicationCursor(CLUSTER, NODE, otherGeneration, 5L, 7L, 42L, 0L, 7L).encode());
        assertThrows(NodeException.class,
                () -> BackupMetadata.requireConsistentWithCursor(backup, foreign));
    }

    /// Verifies metadata and cursor with mismatched recording ids are rejected.
    @Test
    void rejectsRecordingMismatchWhenRecordingUnconfigured() {
        final ReplicationCursor cursor = aeronCursor(7L);
        final BackupMetadata backup = BackupMetadata.create(100L, false, cursor);
        final ReplicationCursor foreign = ReplicationCursor.of("aeron", GENERATION, 7L,
                new AeronReplicationCursor(CLUSTER, NODE, GENERATION, 5L, 7L, 43L, 0L, 7L).encode());
        assertThrows(NodeException.class,
                () -> BackupMetadata.requireConsistentWithCursor(backup, foreign));
    }

    /// Verifies metadata and cursor with mismatched epochs are rejected.
    @Test
    void rejectsEpochMismatchBetweenMetadataAndCursor() {
        final ReplicationCursor cursor = aeronCursor(7L);
        final BackupMetadata backup = BackupMetadata.create(100L, false, cursor);
        final ReplicationCursor foreign = ReplicationCursor.of("aeron", GENERATION, 7L,
                new AeronReplicationCursor(CLUSTER, NODE, GENERATION, 6L, 7L, 42L, 0L, 7L).encode());
        assertThrows(NodeException.class,
                () -> BackupMetadata.requireConsistentWithCursor(backup, foreign));
    }

    /// Verifies a cursor whose outer sequence drifts from its encoded Aeron position is rejected.
    @Test
    void rejectsOuterSequenceMismatchWithEncodedPosition() {
        final ReplicationCursor cursor = aeronCursor(7L);
        final BackupMetadata backup = BackupMetadata.create(100L, false, cursor);
        final ReplicationCursor drifted = ReplicationCursor.of("aeron", GENERATION, 8L,
                new AeronReplicationCursor(CLUSTER, NODE, GENERATION, 5L, 7L, 42L, 0L, 7L).encode());
        assertThrows(NodeException.class,
                () -> BackupMetadata.requireConsistentWithCursor(backup, drifted));
    }

    /// Verifies an undecodable Aeron cursor is rejected by the consistency check.
    @Test
    void rejectsUndecodableAeronCursor() {
        final BackupMetadata backup = BackupMetadata.create(100L, false, aeronCursor(7L));
        final ReplicationCursor corrupt =
                new ReplicationCursor("aeron", GENERATION, 7L, "deadbeef");
        assertThrows(NodeException.class,
                () -> BackupMetadata.requireConsistentWithCursor(backup, corrupt));
    }

    /// Verifies legacy millisecond and malformed archive names are rejected as backup files.
    @Test
    void fileNameRejectsLegacyAndMalformedNames(@TempDir final Path volume) {
        assertFalse(BackupArchive.isBackupFileName("1700000000000.zip"));
        assertFalse(BackupArchive.isBackupFileName("1700000000000.manual.zip"));
        assertFalse(BackupArchive.isBackupFileName("123.evil.zip"));
        assertFalse(BackupArchive.isBackupFileName(null));
        assertThrows(NodeException.class,
                () -> BackupArchive.parseMetadata("1700000000000.zip", volume));
    }

    /// Verifies backup ordering prefers the replication sequence, falls back to the
    /// wall-clock timestamp, and breaks exact ties by backup id.
    @Test
    void newestFirstPrefersSequenceThenTimestampThenBackupId() {
        final UUID lowerId = new UUID(0L, 1L);
        final UUID higherId = new UUID(0L, 2L);
        final BackupMetadata sequencedNewer = new BackupMetadata(
                50L, false, CLUSTER, GENERATION, 5L, 42L, 20L, NODE, higherId, 1L);
        final BackupMetadata sequencedOlder = new BackupMetadata(
                500L, false, CLUSTER, GENERATION, 5L, 42L, 10L, NODE, lowerId, 1L);
        final BackupMetadata unsequenced = new BackupMetadata(
                1000L, false, CLUSTER, GENERATION, 5L, 42L, BackupMetadata.UNKNOWN, NODE, UUID.randomUUID(), 1L);

        final List<BackupMetadata> ordered =
                new ArrayList<>(List.of(sequencedOlder, unsequenced, sequencedNewer));
        ordered.sort(BackupMetadata.OLDEST_FIRST);
        assertEquals(List.of(unsequenced, sequencedOlder, sequencedNewer), ordered,
                "a known replication sequence orders after a timestamp-only fallback");
        assertEquals(sequencedNewer,
                Stream.of(sequencedOlder, unsequenced, sequencedNewer).max(BackupMetadata.OLDEST_FIRST).orElseThrow());

        final BackupMetadata firstTie = new BackupMetadata(
                10L, false, CLUSTER, GENERATION, 5L, 42L, BackupMetadata.UNKNOWN, NODE, lowerId, 1L);
        final BackupMetadata secondTie = new BackupMetadata(
                10L, false, CLUSTER, GENERATION, 5L, 42L, BackupMetadata.UNKNOWN, NODE, higherId, 1L);
        assertEquals(secondTie, Stream.of(firstTie, secondTie).max(BackupMetadata.OLDEST_FIRST).orElseThrow(),
                "the random backup id breaks exact ties deterministically");
    }

    /// Verifies backup compatibility is exactly the single identity match rule.
    @Test
    void compatibilityDelegatesToTheIdentityMatchRule() {
        final BackupMetadata backup = BackupMetadata.create(100L, false, aeronCursor(7L));
        final List<BackupMetadata.Identity> identities = List.of(
                BackupMetadata.Identity.unknown(),
                new BackupMetadata.Identity(CLUSTER, GENERATION, 5L, 42L),
                new BackupMetadata.Identity(UUID.randomUUID(), GENERATION, 5L, 42L),
                new BackupMetadata.Identity(CLUSTER, UUID.randomUUID(), 6L, 42L),
                new BackupMetadata.Identity(CLUSTER, GENERATION, BackupMetadata.UNKNOWN, BackupMetadata.UNKNOWN));

        for (final BackupMetadata.Identity identity : identities) {
            assertEquals(identity.matches(backup.identity()), backup.isCompatibleWith(identity),
                    "compatibility and matching must share one comparison rule");
        }
    }

    /// Verifies a cursor sequence is captured in the backup and checked against the archived cursor.
    @Test
    void rejectsMetadataSequenceThatDisagreesWithTheCursor() {
        final ReplicationCursor cursor = aeronCursor(7L);
        final BackupMetadata drifted = new BackupMetadata(
                100L, false, CLUSTER, GENERATION, 5L, 42L, 6L, NODE, UUID.randomUUID(), BackupMetadata.UNKNOWN);

        assertThrows(NodeException.class,
                () -> BackupMetadata.requireConsistentWithCursor(drifted, cursor));
    }
}
