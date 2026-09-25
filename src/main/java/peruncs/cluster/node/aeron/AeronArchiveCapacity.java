package peruncs.cluster.node.aeron;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.LongSupplier;

/// Cached Archive free-space view and writer admission policy.
///
/// Capacity has two consumers: write admission, which must refuse a write when
/// the embedded Archive volume cannot hold it, and health, which reports the
/// usable space. Filesystem probes are cached for a short interval so neither
/// path turns every write into a stat call; [#invalidate] drops the cache when
/// the Archive directory or policy changes.
final class AeronArchiveCapacity {
    private static final long CACHE_NANOS = 250_000_000L;

    private final boolean externalArchive;
    private final long minimumFreeBytes;
    private final int segmentFileLength;
    private final LongSupplier usableSpace;
    private volatile CapacitySnapshot capacity = new CapacitySnapshot(0L, Long.MIN_VALUE);

    AeronArchiveCapacity(final AeronSettings settings) {
        this(settings.archivePolicy().externalArchive(), settings.archivePolicy().minimumFreeBytes(),
                settings.archivePolicy().segmentFileLength(),
                () -> queryUsableSpace(settings.topology().directories().archiveDirectory()));
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

    /// Reports whether the Archive can accept a write of unknown size.
    ///
    /// @return `true` when the configured policy admits a write
    boolean available() {
        return this.available(0L);
    }

        /// Reports whether the Archive can accept one transaction of the given size.
    ///
    /// External Archives and a zero minimum reserve admit unconditionally.
    /// Otherwise the required free space is the configured reserve plus the
    /// larger of the transaction size and one Archive segment; an unknown
    /// usable space fails closed and refuses the write.
    ///
    /// @param transactionBytes encoded transaction size
    /// @return `true` when the configured policy admits the write
    boolean available(final long transactionBytes) {
        if (transactionBytes < 0) return false;
        if (this.minimumFreeBytes == 0 || this.externalArchive) return true;
        final long reserve = Math.max(transactionBytes, this.segmentFileLength);
        final long required;
        try {
            required = Math.addExact(this.minimumFreeBytes, reserve);
        } catch (final ArithmeticException overflow) {
            /* Deliberate sentinel arithmetic: -1 means "no usable figure",
             * never silently saturated. The overflow reports unavailable. */
            return false;
        }
        return this.usableSpace() >= required;
    }

        /// Returns the last known usable Archive bytes.
    ///
    /// An external Archive has no local volume to measure; `-1` is the
    /// explicit "unknown" value rather than a fake zero.
    ///
    /// @return usable bytes, or `-1` when the Archive is external
    long usableSpaceBytes() {
        return this.externalArchive ? -1L : this.usableSpace();
    }

        /// Drops the cached filesystem probe without touching policy.
    ///
    /// Called when the runtime (re)starts so the first admission check after
    /// startup observes a fresh volume reading instead of a pre-startup value.
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
