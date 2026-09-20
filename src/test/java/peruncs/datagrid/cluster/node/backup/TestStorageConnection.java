package peruncs.datagrid.cluster.node.backup;

import org.eclipse.serializer.afs.types.AFile;
import org.eclipse.serializer.collections.types.XGettingEnum;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceManager;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;
import org.eclipse.store.storage.types.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;

import static org.eclipse.serializer.util.X.notNull;

/// Minimal typed Store connection for backup orchestration tests.
///
/// Non-final so the forked backup crash child can subclass it and park inside
/// [StorageConnection#issueFullBackup] after writing a partial export.
class TestStorageConnection extends org.eclipse.serializer.reference.UsageMarkable.Default
        implements StorageConnection {
    @Override
    public boolean issueGarbageCollection(final long nanoTimeBudget) {
        throw unsupported();
    }

    @Override
    public boolean issueFileCheck(final long nanoTimeBudget) {
        throw unsupported();
    }

    @Override
    public StorageIntegrityCheckResult issueIntegrityCheck(final long nanoTimeBudget) {
        throw unsupported();
    }

    @Override
    public boolean issueCacheCheck(final long nanoTimeBudget, final StorageEntityCacheEvaluator entityEvaluator) {
        throw unsupported();
    }

    @Override
    public void issueFullBackup(
            final StorageLiveFileProvider targetFileProvider,
            final PersistenceTypeDictionaryExporter typeDictionaryExporter
    ) {
        notNull(targetFileProvider);
        notNull(typeDictionaryExporter);
    }

    @Override
    public void issueTransactionsLogCleanup() {
        throw unsupported();
    }

    @Override
    public boolean issueStorageFlush() {
        throw unsupported();
    }

    @Override
    public List<StorageAdjacencyDataExporter.AdjacencyFiles> exportAdjacencyData(final Path workingDir) {
        throw unsupported();
    }

    @Override
    public StorageRawFileStatistics createStorageStatistics() {
        throw unsupported();
    }

    @Override
    public void exportChannels(final StorageLiveFileProvider fileProvider, final boolean performGarbageCollection) {
        throw unsupported();
    }

    @Override
    public StorageEntityTypeExportStatistics exportTypes(
            final StorageEntityTypeExportFileProvider exportFileProvider,
            final Predicate<? super org.eclipse.store.storage.types.StorageEntityTypeHandler> isExportType
    ) {
        throw unsupported();
    }

    @Override
    public void importFiles(final XGettingEnum<AFile> files) {
        throw unsupported();
    }

    @Override
    public void importData(final XGettingEnum<ByteBuffer> data) {
        throw unsupported();
    }

    @Override
    public PersistenceManager<Binary> persistenceManager() {
        throw unsupported();
    }

    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("unsupported in backup orchestration test");
    }
}
