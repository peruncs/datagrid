package peruncs.cluster.node;

import peruncs.cluster.errors.NodeException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/// Holds the one-writer lock for a local Store directory.
final class WriterProcessLock implements AutoCloseable {
    private final Path path;
    private final FileChannel channel;
    private final FileLock lock;

    private WriterProcessLock(final Path path, final FileChannel channel, final FileLock lock) {
        this.path = path;
        this.channel = channel;
        this.lock = lock;
    }

    /// Acquires `<storagePath>/writer.lock` without waiting.
    static WriterProcessLock acquire(final Path storagePath) {
        final Path path = storagePath.resolve("writer.lock");
        try {
            Files.createDirectories(storagePath);
            if (Files.isSymbolicLink(path)) {
                throw new NodeException("writer lock must not be a symbolic link: " + path);
            }
            FileChannel channel;
            try {
                channel = FileChannel.open(path,
                        Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS),
                        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            } catch (final UnsupportedOperationException unsupported) {
                channel = FileChannel.open(path,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            }
            try {
                try {
                    Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
                } catch (final UnsupportedOperationException ignored) {
                    // The file-system has no POSIX permission model.
                }
                final FileLock lock = channel.tryLock();
                if (lock == null) throw held(path);
                return new WriterProcessLock(path, channel, lock);
            } catch (final IOException | RuntimeException failure) {
                try {
                    channel.close();
                } catch (final IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                if (failure instanceof OverlappingFileLockException) throw held(path);
                throw failure;
            }
        } catch (final IOException failure) {
            throw new NodeException("cannot acquire writer lock " + path, failure);
        }
    }

    private static NodeException held(final Path path) {
        return new NodeException("another writer process holds " + path);
    }

    @Override
    public void close() {
        IOException failure = null;
        try {
            this.lock.release();
        } catch (final IOException releaseFailure) {
            failure = releaseFailure;
        }
        try {
            this.channel.close();
        } catch (final IOException closeFailure) {
            if (failure == null) failure = closeFailure;
            else failure.addSuppressed(closeFailure);
        }
        if (failure != null) throw new NodeException("cannot release writer lock " + this.path, failure);
    }
}
