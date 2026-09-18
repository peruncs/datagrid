package peruncs.datagrid.cluster.node.aeron;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies that an Aeron driver directory itself cannot be a symlink.
class AeronRuntimeTest {
    /// Verifies a symlinked driver directory itself is rejected instead of being trusted.
    @Test
    void rejectsSymlinkedDirectory(@TempDir final Path root) throws Exception {
        final Path real = root.resolve("real");
        Files.createDirectory(real);
        final Path link = root.resolve("link");
        try {
            Files.createSymbolicLink(link, real);
        } catch (final UnsupportedOperationException | FileSystemException unsupported) {
            return;
        }
        assertThrows(IllegalStateException.class, () -> AeronRuntime.ensurePrivateDirectory(link));
    }

    /// Verifies a driver path beneath a symlinked parent component is rejected instead of being trusted.
    @Test
    void rejectsSymlinkedParentComponent(@TempDir final Path root) throws Exception {
        final Path real = root.resolve("real-parent");
        Files.createDirectory(real);
        final Path link = root.resolve("linked-parent");
        try {
            Files.createSymbolicLink(link, real);
        } catch (final UnsupportedOperationException | FileSystemException unsupported) {
            return;
        }
        assertThrows(IllegalStateException.class,
                () -> AeronRuntime.ensurePrivateDirectory(link.resolve("driver")));
    }
}
