package peruncs.datagrid.cache.aeron;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

/// Persists one receiver's per-sender invalidation cursors in a single file.
///
/// A volatile broadcast cannot replay what a restarted process missed. The
/// receiver therefore records, for every sender identity it has seen, the
/// last sequence it applied, and validates the first sequence after a
/// restart against that record: the next frame must continue the persisted
/// cursor, or the receiver invalidates everything and requires
/// re-synchronization instead of silently accepting an arbitrary sequence.
///
/// The file is written atomically (temporary file plus atomic move) so a
/// crash mid-write leaves either the previous or the new cursors behind,
/// never a half-written file. A half-written file cannot occur, but a stale
/// one can: cursors are flushed periodically and on disposal, so a crash may
/// lose the tail. That only ever causes a conservative extra invalidation,
/// never a silent gap, because an older cursor still rejects a skipped
/// sequence.
///
/// Cursor files are bounded before parsing, and the file identity is checked
/// again after the descriptor read. A corrupt, replaced, or oversized cursor
/// file therefore fails startup instead of becoming an allocation or parsing
/// attack.
///
/// The store is thread-safe; all methods are synchronized.
final class AeronClusteredCacheCursorStore {
    private static final System.Logger LOGGER =
            System.getLogger(AeronClusteredCacheCursorStore.class.getName());
    /* 1,024 sender keys plus Properties' formatting overhead fit comfortably
     * below this bound. Rejecting an oversized persisted file before parsing
     * keeps an operator-controlled cursor volume from becoming a heap-growth
     * vector during receiver startup. */
    private static final int MAX_CURSOR_FILE_BYTES = 128 * 1024;

    private final Path file;

    /// Creates the store for one subscription namespace.
    ///
    /// @param directory directory holding the cursor file; created on demand
    /// @param namespace stable per-subscription name, already sanitized for file use
    AeronClusteredCacheCursorStore(final Path directory, final String namespace) {
        Objects.requireNonNull(directory, "directory");
        Objects.requireNonNull(namespace, "namespace");
        if (namespace.isBlank()) {
            throw new IllegalArgumentException("namespace must not be blank");
        }
        this.file = directory.resolve("cursors-" + namespace + ".properties");
    }

    /// Creates the cursor file's directory when needed.
    ///
    /// @throws IllegalStateException when the directory cannot be created
    void ensureWritable() {
        try {
            Files.createDirectories(this.file.getParent());
            if (Files.isSymbolicLink(this.file.getParent())) {
                throw new IOException("cursor directory must not be a symbolic link");
            }
            if (Files.exists(this.file, LinkOption.NOFOLLOW_LINKS) &&
                !Files.isRegularFile(this.file, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("cursor path must be a regular file");
            }
        } catch (final IOException failure) {
            throw new IllegalStateException(
                    "cannot create Aeron clustered-cache cursor directory %s".formatted(this.file.getParent()),
                    failure);
        }
    }

    /// Loads the persisted cursors.
    ///
    /// @return sender identity to last applied sequence; empty when no cursors were stored
    /// @throws IllegalStateException when the file is missing its required
    ///                              regular-file shape, changed while read,
    ///                              oversized, or cannot be parsed
    synchronized Map<AeronClusteredCacheMessageCodec.SenderId, Long> load() {
        final Map<AeronClusteredCacheMessageCodec.SenderId, Long> cursors = new HashMap<>();
        if (Files.isSymbolicLink(this.file)) {
            throw new IllegalStateException("refusing to read symbolic-link cursor file %s".formatted(this.file));
        }
        if (Files.notExists(this.file, LinkOption.NOFOLLOW_LINKS)) {
            return cursors;
        }
        final BasicFileAttributes before;
        try {
            before = Files.readAttributes(this.file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (final IOException failure) {
            throw new IllegalStateException(
                    "cannot inspect Aeron clustered-cache cursors at %s".formatted(this.file), failure);
        }
        if (!before.isRegularFile() || before.size() > MAX_CURSOR_FILE_BYTES) {
            throw new IllegalStateException(
                    "Aeron clustered-cache cursor file must be a regular file of at most %s bytes: %s"
                            .formatted(MAX_CURSOR_FILE_BYTES, this.file));
        }
        final byte[] encoded = new byte[MAX_CURSOR_FILE_BYTES + 1];
        final int length;
        try (final FileChannel channel = FileChannel.open(
                this.file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            if (channel.size() > MAX_CURSOR_FILE_BYTES) {
                throw new IOException("cursor file exceeds %s bytes".formatted(MAX_CURSOR_FILE_BYTES));
            }
            final ByteBuffer destination = ByteBuffer.wrap(encoded);
            while (destination.hasRemaining()) {
                final int read = channel.read(destination);
                if (read < 0) break;
                if (read == 0) throw new IOException("cursor file read made no progress");
            }
            if (destination.position() > MAX_CURSOR_FILE_BYTES) {
                throw new IOException("cursor file exceeds %s bytes".formatted(MAX_CURSOR_FILE_BYTES));
            }
            length = destination.position();
            final BasicFileAttributes after = Files.readAttributes(
                    this.file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (before.fileKey() == null || after.fileKey() == null ||
                    !Objects.equals(before.fileKey(), after.fileKey()) || !after.isRegularFile()) {
                throw new IOException("cursor file changed while it was being read");
            }
        } catch (final IOException failure) {
            throw new IllegalStateException(
                    "cannot read Aeron clustered-cache cursors from %s".formatted(this.file), failure);
        }
        final Properties stored = new Properties();
        try {
            stored.load(new ByteArrayInputStream(encoded, 0, length));
        } catch (final IOException failure) {
            throw new IllegalStateException(
                    "cannot parse Aeron clustered-cache cursors from %s".formatted(this.file), failure);
        } finally {
            Arrays.fill(encoded, (byte) 0);
        }
        for (final String key : stored.stringPropertyNames()) {
            cursors.put(parseSender(key, stored.getProperty(key)), parseSequence(key, stored.getProperty(key)));
        }
        return cursors;
    }

    private AeronClusteredCacheMessageCodec.SenderId parseSender(final String key, final String value) {
        final int separator = key.indexOf('-');
        try {
            if (separator <= 0) {
                throw new NumberFormatException("missing separator");
            }
            final long most = Long.parseUnsignedLong(key.substring(0, separator), 16);
            final long least = Long.parseUnsignedLong(key.substring(separator + 1), 16);
            return new AeronClusteredCacheMessageCodec.SenderId(most, least);
        } catch (final RuntimeException failure) {
            throw new IllegalStateException(
                    "cannot parse Aeron clustered-cache cursor %s=%s in %s".formatted(key, value, this.file),
                    failure);
        }
    }

    private long parseSequence(final String key, final String value) {
        try {
            final long sequence = Long.parseLong(value.trim());
            if (sequence < 0 || sequence == Long.MAX_VALUE) {
                throw new NumberFormatException("sequence out of range");
            }
            return sequence;
        } catch (final RuntimeException failure) {
            throw new IllegalStateException(
                    "cannot parse Aeron clustered-cache cursor %s=%s in %s".formatted(key, value, this.file),
                    failure);
        }
    }

    /// Stores the cursors atomically.
    ///
    /// @param cursors sender identity to last applied sequence
    /// @throws IllegalStateException when the cursors cannot be stored
    synchronized void store(final Map<AeronClusteredCacheMessageCodec.SenderId, Long> cursors) {
        Objects.requireNonNull(cursors, "cursors");
        this.ensureWritable();
        final Properties stored = new Properties();
        for (final Map.Entry<AeronClusteredCacheMessageCodec.SenderId, Long> cursor : cursors.entrySet()) {
            final AeronClusteredCacheMessageCodec.SenderId sender = cursor.getKey();
            stored.setProperty("%016x-%016x".formatted(sender.mostSignificantBits(), sender.leastSignificantBits()),
                    Long.toString(cursor.getValue()));
        }
        try {
            if (Files.isSymbolicLink(this.file)) {
                throw new IOException("refusing to replace symbolic-link cursor file");
            }
            final Path temporary = Files.createTempFile(
                    this.file.getParent(), this.file.getFileName().toString(), ".tmp");
            try (final OutputStream output =
                         Files.newOutputStream(temporary, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                stored.store(output, null);
            }
            try {
                Files.move(temporary, this.file,
                        StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (final AtomicMoveNotSupportedException unsupported) {
                LOGGER.log(System.Logger.Level.DEBUG,
                        "Atomic cursor move is not supported; storing %s non-atomically".formatted(this.file));
                Files.move(temporary, this.file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (final IOException failure) {
            throw new IllegalStateException(
                    "cannot store Aeron clustered-cache cursors in %s".formatted(this.file), failure);
        }
    }
}
