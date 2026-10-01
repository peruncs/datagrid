package peruncs.cluster.storage.index;

import org.eclipse.serializer.collections.EqHashEnum;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.*;
import org.eclipse.store.gigamap.types.GigaMap;
import org.junit.jupiter.api.Test;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.*;

class ClusterIndexCommitPrefilterTest {
    @Test
    void skipsTypesProvenNotToReachIndexes() {
        assertFalse(touchesIndexes(String.class));
    }

    @Test
    void scansManyPlainEntitiesWithoutTreatingThemAsIndexRelevant() {
        final int count = 10_000;
        final ByteBuffer buffer = ByteBuffer.allocateDirect(count * Binary.entityHeaderLength())
                .order(ByteOrder.nativeOrder());
        for (int objectId = 0; objectId < count; objectId++) {
            buffer.putLong(Binary.entityHeaderLength()).putLong(1L).putLong(objectId + 1L);
        }
        final Binary binary = ChunksWrapper.New(buffer);
        final PersistenceTypeDictionary dictionary = dictionary(String.class);

        assertFalse(ClusterIndexValidation.commitTouchesIndexes(binary, dictionary,
                new ClusterIndexValidation.CommitPrefilterScratch()));
    }

    @Test
    void scansTypesThatCanReachIndexes() {
        assertTrue(touchesIndexes(GigaMap.class));
    }

    @Test
    void writerCommitFindsOnlyTheRegisteredReplicationMarkType() {
        final ByteBuffer bytes = ByteBuffer.allocateDirect(48).order(ByteOrder.nativeOrder());
        bytes.putLong(24L).putLong(17L).putLong(31L);
        bytes.putLong(24L).putLong(19L).putLong(32L);
        final Binary binary = ChunksWrapper.New(bytes);
        final PersistenceTypeDictionary dictionary = unknownTypeDictionary();
        registerType(dictionary, 17L, String.class);
        registerType(dictionary, 19L, ReplicationMark.class);

        final int missingMark = ClusterIndexValidation.inspectWriterCommit(binary, dictionary,
                new ClusterIndexValidation.CommitPrefilterScratch(), 31L);
        assertEquals(0, missingMark);

        final int foundMark = ClusterIndexValidation.inspectWriterCommit(binary, dictionary,
                new ClusterIndexValidation.CommitPrefilterScratch(), 32L);
        assertEquals(ClusterStoreIndexes.COMMIT_HAS_REPLICATION_MARK, foundMark,
                "the replication mark must not make ordinary writes index-relevant");
    }

    @Test
    void writerCommitNoticesMarkTypeRegisteredAfterAnEarlierScan() {
        final PersistenceTypeDictionary dictionary = unknownTypeDictionary();
        registerType(dictionary, 17L, String.class);
        final ClusterIndexValidation.CommitPrefilterScratch scratch =
                new ClusterIndexValidation.CommitPrefilterScratch();
        final ByteBuffer bytes = ByteBuffer.allocateDirect(48).order(ByteOrder.nativeOrder());
        bytes.putLong(24L).putLong(17L).putLong(1L);
        bytes.putLong(24L).putLong(19L).putLong(2L);
        final Binary binary = ChunksWrapper.New(bytes);

        assertEquals(ClusterStoreIndexes.COMMIT_HAS_REPLICATION_MARK,
                ClusterIndexValidation.inspectWriterCommit(binary, dictionary, scratch, 1L) &
                ClusterStoreIndexes.COMMIT_HAS_REPLICATION_MARK);

        registerType(dictionary, 19L, ReplicationMark.class);

        assertEquals(0,
                ClusterIndexValidation.inspectWriterCommit(binary, dictionary, scratch, 1L) &
                        ClusterStoreIndexes.COMMIT_HAS_REPLICATION_MARK);
    }

    @Test
    void retriesAnUnknownTypeAfterItsDefinitionArrives() {
        final PersistenceTypeDictionary dictionary = unknownTypeDictionary();
        registerType(dictionary, 19L, ReplicationMark.class);
        final ClusterIndexValidation.CommitPrefilterScratch scratch =
                new ClusterIndexValidation.CommitPrefilterScratch();
        final Binary binary = binary(17L);

        assertEquals(ClusterStoreIndexes.COMMIT_TOUCHES_INDEXES,
                ClusterIndexValidation.inspectWriterCommit(binary, dictionary, scratch, -1L));

        registerType(dictionary, 17L, String.class);

        assertEquals(0, ClusterIndexValidation.inspectWriterCommit(binary, dictionary, scratch, -1L),
                "an unknown type must not stay cached as index-relevant after it becomes known");
    }

    @Test
    void scansUnknownTypesConservatively() {
        assertTrue(ClusterIndexValidation.commitTouchesIndexes(binary(41), unknownTypeDictionary(),
                new ClusterIndexValidation.CommitPrefilterScratch()));
    }

    @Test
    void boundFailureNamesTheOperatorSetting() {
        final ClusterIndexValidation.ValidationScratch scratch =
                new ClusterIndexValidation.ValidationScratch(ClusterIndexTestSupport.typeHandlers());
        ClusterIndexValidation.countScanWork(scratch, 1);

        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> ClusterIndexValidation.countScanWork(scratch, 1));
        assertTrue(failure.getMessage().contains("PERUNCS_INDEX_VALIDATION_MAX_OBJECTS"));
    }

    /// Mutual recursion must not cache a false "index-free" answer, whichever type is asked first.
    @Test
    void cyclicTypesReachingAnIndexAreRelevantInEitherQueryOrder() {
        for (final boolean askOwnerFirst : new boolean[]{true, false}) {
            final PersistenceTypeDictionary dictionary = unknownTypeDictionary();
            registerType(dictionary, 1L, CycleOwner.class, member("b", CycleMember.class), member("map", GigaMap.class));
            registerType(dictionary, 2L, CycleMember.class, member("owner", CycleOwner.class));
            final ClusterIndexValidation.CommitPrefilterScratch scratch =
                    new ClusterIndexValidation.CommitPrefilterScratch();
            if (askOwnerFirst) {
                assertTrue(ClusterIndexValidation.commitTouchesIndexes(binary(1), dictionary, scratch));
            }
            assertTrue(ClusterIndexValidation.commitTouchesIndexes(binary(2), dictionary, scratch),
                    "the member type reaches an index through the cycle (owner first: " + askOwnerFirst + ")");
            assertTrue(ClusterIndexValidation.commitTouchesIndexes(binary(1), dictionary, scratch));
        }
    }

    /// A self-recursive plain type stays index-free, so ordinary linked data is not scanned.
    @Test
    void selfRecursivePlainTypesStayIndexFree() {
        final PersistenceTypeDictionary dictionary = unknownTypeDictionary();
        registerType(dictionary, 1L, PlainNode.class, member("next", PlainNode.class), member("value", String.class));
        assertFalse(ClusterIndexValidation.commitTouchesIndexes(binary(1), dictionary,
                new ClusterIndexValidation.CommitPrefilterScratch()));
    }

    static final class CycleOwner {
    }

    static final class CycleMember {
    }

    static final class PlainNode {
    }

    private static PersistenceTypeDefinitionMember member(final String name, final Class<?> type) {
        return PersistenceTypeDefinitionMemberFieldGenericSimple.New(type.getName(), null, name, type, true, 8L, 8L);
    }

    private static void registerType(final PersistenceTypeDictionary dictionary, final long typeId,
                                     final Class<?> type, final PersistenceTypeDefinitionMember... fields) {
        final var members = EqHashEnum.<PersistenceTypeDefinitionMember>New(
                PersistenceTypeDescriptionMember.identityHashEqualator());
        for (final PersistenceTypeDefinitionMember field : fields) members.add(field);
        dictionary.registerTypeDefinition(PersistenceTypeDefinition.New(
                typeId, type.getName(), type.getName(), type, members, members));
    }

    private static boolean touchesIndexes(final Class<?> type) {
        return ClusterIndexValidation.commitTouchesIndexes(binary(1), dictionary(type),
                new ClusterIndexValidation.CommitPrefilterScratch());
    }

    private static PersistenceTypeDictionary unknownTypeDictionary() {
        return PersistenceTypeDictionary.New(PersistenceTypeLineageCreator.New());
    }

    private static PersistenceTypeDictionary dictionary(final Class<?> type) {
        final PersistenceTypeDictionary dictionary = unknownTypeDictionary();
        registerType(dictionary, 1L, type);
        return dictionary;
    }

    private static void registerType(final PersistenceTypeDictionary dictionary, final long typeId,
                                     final Class<?> type) {
        final var members = EqHashEnum.<PersistenceTypeDefinitionMember>New(
                PersistenceTypeDescriptionMember.identityHashEqualator());
        dictionary.registerTypeDefinition(PersistenceTypeDefinition.New(
                typeId, type.getName(), type.getName(), type, members, members));
    }

    private static Binary binary(final long typeId) {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(Binary.entityHeaderLength()).order(ByteOrder.nativeOrder());
        buffer.putLong(Binary.entityHeaderLength()).putLong(typeId).putLong(1L);
        return ChunksWrapper.New(buffer);
    }

}
