package peruncs.cluster.node;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.errors.NodeException;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriterProcessLockTest {
    @Test
    void excludesAnotherWriterUntilTheStoreLockIsReleased(@TempDir final Path storagePath) {
        final WriterProcessLock first = WriterProcessLock.acquire(storagePath);
        try {
            assertTrue(Files.exists(storagePath.resolve("writer.lock")));
            assertThrows(NodeException.class, () -> WriterProcessLock.acquire(storagePath));
        } finally {
            first.close();
        }

        try (WriterProcessLock reopened = WriterProcessLock.acquire(storagePath)) {
            assertTrue(Files.exists(storagePath.resolve("writer.lock")));
        }
    }
}
