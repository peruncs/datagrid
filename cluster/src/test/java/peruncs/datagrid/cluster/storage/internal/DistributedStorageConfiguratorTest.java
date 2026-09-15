package peruncs.datagrid.cluster.storage.internal;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;
import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.storage.internal.DistributedStorageConfigurator;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Verifies that one Store object implementing both extension SPIs keeps both contracts. */
class DistributedStorageConfiguratorTest {
    @Test
    void preservesTargetAndDictionaryExporterContracts() {
        final DistributedStorageConfigurator configurator = new DistributedStorageConfigurator(new NoOpDistributor());
        final Object decorated = configurator.apply(new BothContracts());

        assertInstanceOf(PersistenceTarget.class, decorated);
        assertInstanceOf(PersistenceTypeDictionaryExporter.class, decorated);
    }

    private static final class BothContracts
            implements PersistenceTarget<Binary>, PersistenceTypeDictionaryExporter {
        @Override
        public void write(final Binary data) {
        }

        @Override
        public boolean isWritable() {
            return true;
        }

        @Override
        public void exportTypeDictionary(final PersistenceTypeDictionary typeDictionary) {
        }
    }

    private static final class NoOpDistributor implements StorageBinaryDataDistributor {
        @Override
        public void distributeData(final Binary data) {
        }

        @Override
        public void distributeTypeDictionary(final String typeDictionaryData) {
        }

        @Override
        public void dispose() {
        }
    }
}
