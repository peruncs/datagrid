package peruncs.cluster.storage.index;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.collections.EqHashEnum;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinition;
import org.eclipse.serializer.persistence.types.PersistenceTypeDefinitionMember;
import org.eclipse.serializer.persistence.types.PersistenceTypeDescriptionMember;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.persistence.types.PersistenceTypeLineageCreator;
import org.eclipse.store.gigamap.types.GigaMap;
import org.junit.jupiter.api.Test;

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
    void scansUnknownTypesConservatively() {
        assertTrue(ClusterIndexValidation.commitTouchesIndexes(binary(41), unknownTypeDictionary(),
                new ClusterIndexValidation.CommitPrefilterScratch()));
    }

    @Test
    void boundFailureNamesTheOperatorSetting() {
        final ClusterIndexValidation.ValidationScratch scratch = new ClusterIndexValidation.ValidationScratch();
        ClusterIndexValidation.countScanWork(scratch, 1);

        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> ClusterIndexValidation.countScanWork(scratch, 1));
        assertTrue(failure.getMessage().contains("ECLIPSE_DATAGRID_INDEX_VALIDATION_MAX_OBJECTS"));
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
        final var members = EqHashEnum.<PersistenceTypeDefinitionMember>New(
                PersistenceTypeDescriptionMember.identityHashEqualator());
        dictionary.registerTypeDefinition(PersistenceTypeDefinition.New(
                1L, type.getName(), type.getName(), type, members, members));
        return dictionary;
    }

    private static Binary binary(final long typeId) {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(Binary.entityHeaderLength()).order(ByteOrder.nativeOrder());
        buffer.putLong(Binary.entityHeaderLength()).putLong(typeId).putLong(1L);
        return ChunksWrapper.New(buffer);
    }

}
