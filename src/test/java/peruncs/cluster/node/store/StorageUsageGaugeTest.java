package peruncs.cluster.node.store;

import org.eclipse.store.storage.types.Storage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
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
}
