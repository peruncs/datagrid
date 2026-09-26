package peruncs.cluster.node.aeron;

import io.aeron.archive.ArchiveMarkFile;
import io.aeron.archive.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.archive.codecs.mark.MarkFileHeaderEncoder;
import io.aeron.archive.codecs.mark.MessageHeaderDecoder;
import io.aeron.archive.codecs.mark.MessageHeaderEncoder;
import io.aeron.driver.MediaDriver;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies that an Aeron driver directory itself cannot be a symlink and that a
/// launch recovers crash leftovers in the archive mark file instead of bricking.
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

    /// Verifies a never-signaled archive mark file left by a writer killed during
    /// Archive startup is removed before the first launch attempt, so upstream's
    /// permanent version rejection never happens.
    @Test
    void removesNeverSignaledArchiveMarkFileBeforeLaunch(@TempDir final Path root) throws Exception {
        final Path markFile = archiveMarkFile(root, 0);
        final AtomicInteger launches = new AtomicInteger();
        final AutoCloseable launched = AeronRuntime.launchDriver(new MediaDriver.Context(),
                markFile,
                () -> {
                    launches.incrementAndGet();
                    return (AutoCloseable) () -> { };
                });
        assertNotNull(launched);
        assertEquals(1, launches.get());
        assertFalse(Files.exists(markFile));
    }

    /// Verifies the same repair still applies when upstream rejects a mark file that
    /// appeared after the pre-launch scan: the launch is retried instead of failing.
    @Test
    void removesNeverSignaledArchiveMarkFileRejectedDuringLaunch(@TempDir final Path root) throws Exception {
        final Path markFile = root.resolve("archive").resolve(ArchiveMarkFile.FILENAME);
        final AtomicInteger launches = new AtomicInteger();
        final AutoCloseable launched = AeronRuntime.launchDriver(new MediaDriver.Context(),
                markFile,
                () -> {
                    if (launches.incrementAndGet() == 1) {
                        try {
                            writeArchiveMarkFile(markFile, 0);
                        } catch (final IOException failure) {
                            throw new IllegalStateException(failure);
                        }
                        throw rejectedVersion(markFile, 0);
                    }
                    return (AutoCloseable) () -> { };
                });
        assertNotNull(launched);
        assertEquals(2, launches.get());
        assertFalse(Files.exists(markFile));
    }

    /// Verifies a version mismatch other than "never signaled" is a real format
    /// mismatch and fails closed without touching the mark file.
    @Test
    void keepsMarkFileWhenMajorVersionIsNotNeverSignaled(@TempDir final Path root) throws Exception {
        final Path markFile = archiveMarkFile(root, SemanticVersion.compose(2, 0, 0));
        final AtomicInteger launches = new AtomicInteger();
        assertThrows(IllegalArgumentException.class, () -> AeronRuntime.<AutoCloseable>launchDriver(
                new MediaDriver.Context(), markFile, () -> {
                    launches.incrementAndGet();
                    throw rejectedVersion(markFile, 2);
                }));
        assertEquals(1, launches.get());
        assertTrue(Files.exists(markFile));
    }

    /// Verifies a rejection that names some other file never triggers a deletion here.
    @Test
    void keepsForeignMarkFile(@TempDir final Path root) throws Exception {
        final Path markFile = archiveMarkFile(root, ArchiveMarkFile.SEMANTIC_VERSION);
        final Path foreign = root.resolve("foreign").resolve(ArchiveMarkFile.FILENAME);
        Files.createDirectories(foreign.getParent());
        Files.write(foreign, new byte[1024]);
        final AtomicInteger launches = new AtomicInteger();
        assertThrows(IllegalArgumentException.class, () -> AeronRuntime.<AutoCloseable>launchDriver(
                new MediaDriver.Context(), markFile, () -> {
                    launches.incrementAndGet();
                    throw rejectedVersion(foreign, 0);
                }));
        assertEquals(1, launches.get());
        assertTrue(Files.exists(foreign));
        assertTrue(Files.exists(markFile));
    }

    /// Verifies a mark file that reads a signaled version when the rejection arrives
    /// is kept: only a positively never-signaled file may be removed.
    @Test
    void keepsMarkFileThatWasSignaledInTheMeantime(@TempDir final Path root) throws Exception {
        final Path markFile = archiveMarkFile(root, ArchiveMarkFile.SEMANTIC_VERSION);
        final AtomicInteger launches = new AtomicInteger();
        assertThrows(IllegalArgumentException.class, () -> AeronRuntime.<AutoCloseable>launchDriver(
                new MediaDriver.Context(), markFile, () -> {
                    launches.incrementAndGet();
                    throw rejectedVersion(markFile, 0);
                }));
        assertEquals(1, launches.get());
        assertTrue(Files.exists(markFile));
    }

    /// Verifies a stale archive mark file whose owner recently died is waited out
    /// and the launch retried, without removing the file.
    @Test
    void waitsForOwnActiveArchiveMarkFile(@TempDir final Path root) throws Exception {
        final Path markFile = archiveMarkFile(root, ArchiveMarkFile.SEMANTIC_VERSION);
        final AtomicInteger launches = new AtomicInteger();
        final AutoCloseable launched = AeronRuntime.launchDriver(new MediaDriver.Context(),
                markFile,
                () -> {
                    if (launches.incrementAndGet() == 1) {
                        throw new IllegalStateException(
                                "active mark file detected: %s".formatted(markFile.toFile().getAbsolutePath()));
                    }
                    return (AutoCloseable) () -> { };
                });
        assertNotNull(launched);
        assertEquals(2, launches.get());
        assertTrue(Files.exists(markFile));
    }

    /// Verifies an active-mark rejection that names some other file fails closed
    /// instead of being waited out here.
    @Test
    void failsClosedOnForeignActiveMarkFile(@TempDir final Path root) throws Exception {
        final Path markFile = archiveMarkFile(root, ArchiveMarkFile.SEMANTIC_VERSION);
        final Path foreign = root.resolve("foreign").resolve(ArchiveMarkFile.FILENAME);
        Files.createDirectories(foreign.getParent());
        Files.write(foreign, new byte[1024]);
        final AtomicInteger launches = new AtomicInteger();
        assertThrows(IllegalStateException.class, () -> AeronRuntime.<AutoCloseable>launchDriver(
                new MediaDriver.Context(), markFile, () -> {
                    launches.incrementAndGet();
                    throw new IllegalStateException(
                            "active mark file detected: %s".formatted(foreign.toFile().getAbsolutePath()));
                }));
        assertEquals(1, launches.get());
        assertTrue(Files.exists(foreign));
    }

    /// Verifies an unrelated illegal state propagates instead of being retried.
    @Test
    void failsClosedOnUnrelatedIllegalState() {
        final AtomicInteger launches = new AtomicInteger();
        assertThrows(IllegalStateException.class, () -> AeronRuntime.<AutoCloseable>launchDriver(
                new MediaDriver.Context(), null, () -> {
                    launches.incrementAndGet();
                    throw new IllegalStateException("configurations are broken");
                }));
        assertEquals(1, launches.get());
    }

    /// Writes an archive mark file shaped like upstream's own output: a valid SBE
    /// header whose version field is either left never-signaled or written like
    /// `ArchiveMarkFile.signalReady` would.
    private static Path archiveMarkFile(final Path root, final int version) throws IOException {
        final Path markFile = root.resolve("archive").resolve(ArchiveMarkFile.FILENAME);
        writeArchiveMarkFile(markFile, version);
        return markFile;
    }

    /// Writes a mark file of at least a kilobyte so the version field at the upstream
    /// offset is addressable, mirroring the layout [ArchiveMarkFile] creates.
    private static void writeArchiveMarkFile(final Path markFile, final int version) throws IOException {
        Files.createDirectories(markFile.getParent());
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
        new MarkFileHeaderEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        if (version != 0) {
            buffer.putInt(MessageHeaderDecoder.ENCODED_LENGTH + MarkFileHeaderDecoder.versionEncodingOffset(),
                    version);
        }
        Files.write(markFile, buffer.byteArray());
    }

    /// Builds the exact rejection upstream [io.aeron.archive.ArchiveMarkFile] throws
    /// for a mark file whose semantic version major does not match.
    private static IllegalArgumentException rejectedVersion(final Path markFile, final int major) {
        return new IllegalArgumentException("mark file (%s) major version %d does not match software: %d"
                .formatted(markFile.toFile().getAbsolutePath(), major, ArchiveMarkFile.MAJOR_VERSION));
    }
}
