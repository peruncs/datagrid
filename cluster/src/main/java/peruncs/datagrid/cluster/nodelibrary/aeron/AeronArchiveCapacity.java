package peruncs.datagrid.cluster.nodelibrary.aeron;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.LongSupplier;

/// Cached Archive free-space view and writer admission policy.
final class AeronArchiveCapacity {
    private static final long CACHE_NANOS = 250_000_000L;

    private final boolean externalArchive;
    private final long minimumFreeBytes;
    private final int segmentFileLength;
    private final LongSupplier usableSpace;
    private volatile CapacitySnapshot capacity = new CapacitySnapshot(0L, Long.MIN_VALUE);

    AeronArchiveCapacity(final AeronSettings settings) {
        this(settings.externalArchive(), settings.minimumArchiveFreeBytes(),
                settings.archiveSegmentFileLength(), () -> queryUsableSpace(settings.archiveDirectory()));
    }

    AeronArchiveCapacity(final boolean externalArchive, final long minimumFreeBytes,
                         final int segmentFileLength, final LongSupplier usableSpace) {
        if (minimumFreeBytes < 0 || segmentFileLength <= 0)
            throw new IllegalArgumentException("invalid Aeron Archive capacity policy");
        this.externalArchive = externalArchive;
        this.minimumFreeBytes = minimumFreeBytes;
        this.segmentFileLength = segmentFileLength;
        this.usableSpace = usableSpace;
    }

    private static long queryUsableSpace(final Path archiveDirectory) {
        try {
            return Files.getFileStore(archiveDirectory).getUsableSpace();
        } catch (final IOException | RuntimeException failure) {
            /* Unknown capacity must not admit a write when a threshold is configured. */
            return -1L;
        }
    }

    boolean available() {
        return this.available(0L);
    }

    boolean available(final long transactionBytes) {
        if (transactionBytes < 0) return false;
        if (this.minimumFreeBytes == 0 || this.externalArchive) return true;
        final long reserve = Math.max(transactionBytes, this.segmentFileLength);
        final long required;
        try {
            required = Math.addExact(this.minimumFreeBytes, reserve);
        } catch (final ArithmeticException ignored) {
            return false;
        }
        return this.usableSpace() >= required;
    }

    long usableSpaceBytes() {
        return this.externalArchive ? -1L : this.usableSpace();
    }

    void invalidate() {
        final CapacitySnapshot current = this.capacity;
        this.capacity = new CapacitySnapshot(0L, current.usableSpace());
    }

    private long usableSpace() {
        final long now = System.nanoTime();
        final CapacitySnapshot current = this.capacity;
        if (current.checkedNanos() != 0L && now - current.checkedNanos() < CACHE_NANOS)
            return current.usableSpace();
        final long usable = this.usableSpace.getAsLong();
        this.capacity = new CapacitySnapshot(now, usable);
        return usable;
    }

    private record CapacitySnapshot(long checkedNanos, long usableSpace) {
    }
}
