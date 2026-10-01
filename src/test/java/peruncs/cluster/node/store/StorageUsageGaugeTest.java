package peruncs.cluster.node.store;

import org.eclipse.store.storage.types.Storage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.eclipse.serializer.afs.types.ADirectory;
import org.eclipse.serializer.afs.types.AFile;

import java.io.UncheckedIOException;
import java.lang.reflect.Proxy;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.function.Consumer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StorageUsageGaugeTest {
    @Test
    void readsTheScheduledSnapshotWithoutStartingAnotherMeasurement(@TempDir final Path directory)
            throws Exception {
        final StorageUsageGauge gauge = StorageUsageGauge.create(Storage.FileProvider(directory).baseDirectory());
        assertEquals(-1L, gauge.readUsedDiskSpaceBytes());

        final Path file = directory.resolve("store.data");
        Files.writeString(file, "first");
        assertEquals(5L, gauge.measureNow());
        assertEquals(5L, gauge.readUsedDiskSpaceBytes());

        Files.writeString(file, "updated");
        assertEquals(5L, gauge.readUsedDiskSpaceBytes());
        assertEquals(7L, gauge.measureNow());
        assertEquals(7L, gauge.readUsedDiskSpaceBytes());
    }

    @Test
    void anUnreadableFileMakesTheMeasurementUnknownButARemovedFileIsSkipped() {
        assertEquals(-1L, StorageUsageGauge.create(directoryWithFile(
                new UncheckedIOException(new AccessDeniedException("denied")))).measureNow());
        assertEquals(0L, StorageUsageGauge.create(directoryWithFile(
                new UncheckedIOException(new NoSuchFileException("gone")))).measureNow());
    }

    private static ADirectory directoryWithFile(final RuntimeException sizeFailure) {
        final AFile file = (AFile) Proxy.newProxyInstance(AFile.class.getClassLoader(), new Class<?>[]{AFile.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("size")) throw sizeFailure;
                    if (method.getName().equals("toString")) return "test-file";
                    throw new UnsupportedOperationException(method.getName());
                });
        return (ADirectory) Proxy.newProxyInstance(ADirectory.class.getClassLoader(), new Class<?>[]{ADirectory.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("iterateFiles")) {
                        @SuppressWarnings("unchecked") final Consumer<AFile> consumer = (Consumer<AFile>) args[0];
                        consumer.accept(file);
                        return null;
                    }
                    if (method.getName().equals("iterateDirectories")) return null;
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
