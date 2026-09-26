package peruncs.cluster.node.store;

import org.eclipse.serializer.afs.types.ADirectory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.Logger.Level.*;
import static org.eclipse.serializer.util.X.notNull;

/// Measures the bytes currently used by a storage directory.
///
/// A running Store may remove a file while the directory is being visited.
/// Implementations therefore treat that individual file as unavailable and
/// continue the measurement.
public interface StorageUsageGauge {
    /// Default cache lifetime for one directory measurement.
    Duration DEFAULT_CACHE_TTL = Duration.ofSeconds(5);

        /// Creates a disk-space reader with the default cache lifetime.
    ///
    /// @param storageDir storage directory
    /// @return disk-space reader
    static StorageUsageGauge create(final ADirectory storageDir) {
        return create(storageDir, DEFAULT_CACHE_TTL);
    }

        /// Creates a disk-space reader with an explicit cache lifetime.
    ///
    /// @param storageDir storage directory
    /// @param cacheTtl   how long one measurement is reused
    /// @return disk-space reader
    static StorageUsageGauge create(final ADirectory storageDir, final Duration cacheTtl) {
        return new Default(notNull(storageDir), notNull(cacheTtl));
    }

        /// Reads used bytes in the storage directory.
    ///
    /// @return used bytes
    long readUsedDiskSpaceBytes();

        /// Recursively measures the configured Store directory.
    class Default implements StorageUsageGauge {
        private static final System.Logger LOGGER = System.getLogger(StorageUsageGauge.class.getName());
        private final ADirectory storageDir;
        private final long cacheNanos;
        private final AtomicLong lastLog = new AtomicLong(System.currentTimeMillis());
        private final AtomicBoolean refreshRunning = new AtomicBoolean();
        private volatile long cachedBytes;
        private volatile long measuredAtNanos;

        private Default(final ADirectory storageDir, final Duration cacheTtl) {
            if (cacheTtl.isNegative()) {
                throw new IllegalArgumentException("cacheTtl must not be negative");
            }
            this.storageDir = storageDir;
            this.cacheNanos = cacheTtl.toNanos();
        }

        @Override
        public long readUsedDiskSpaceBytes() {
            final long now = System.nanoTime();
            final long measuredAt = this.measuredAtNanos;
            if (measuredAt != 0L && now - measuredAt >= 0L && now - measuredAt < this.cacheNanos) {
                return this.cachedBytes;
            }
            /* Single-flight refresh for the stale and the cold case alike: the
             * first measurement never walks the directory on the caller's
             * thread, so a slow volume cannot block health or limit checks.
             * A cold gauge reports zero until the first refresh lands. */
            if (this.refreshRunning.compareAndSet(false, true)) {
                Thread.startVirtualThread(() -> {
                    try {
                        this.measure();
                    } finally {
                        this.refreshRunning.set(false);
                    }
                });
            }
            return this.cachedBytes;
        }

        private void measure() {
            final long sizeBytes = this.totalSize(this.storageDir);
            this.cachedBytes = sizeBytes;
            this.measuredAtNanos = System.nanoTime();
            final long nowMillis = System.currentTimeMillis();
            final long previousLog = this.lastLog.get();
            if (LOGGER.isLoggable(TRACE) && nowMillis - previousLog > 600_000L &&
                this.lastLog.compareAndSet(previousLog, nowMillis)) {
                LOGGER.log(TRACE, "Read current storage disk space (%s)".formatted(sizeBytes));
            }
        }

        private long totalSize(final ADirectory dir) {
            final long[] total = {
                    0L
            };
            /* Overflow is latched, not repeatedly logged: once usage
             * saturates the long space, every file of every refresh would
             * otherwise warn again. */
            final boolean[] overflowLogged = {
                    false
            };
            dir.iterateFiles(f ->
            {
                try {
                    try {
                        total[0] = Math.addExact(total[0], f.size());
                    } catch (final ArithmeticException overflow) {
                        total[0] = Long.MAX_VALUE;
                        if (!overflowLogged[0]) {
                            overflowLogged[0] = true;
                            LOGGER.log(WARNING, "Storage size overflow while measuring %s".formatted(f));
                        }
                    }
                } catch (final RuntimeException e) {
                    LOGGER.log(DEBUG, "Could not measure storage file %s; it may have been removed".formatted(f), e);
                }
            });
            try {
                dir.iterateDirectories(d ->
                {
                    final long child = this.totalSize(d);
                    try {
                        total[0] = Math.addExact(total[0], child);
                    } catch (final ArithmeticException overflow) {
                        total[0] = Long.MAX_VALUE;
                    }
                });
            } catch (final RuntimeException failure) {
                LOGGER.log(DEBUG, "Could not measure a storage directory; it may have been removed", failure);
            }
            return total[0];
        }
    }
}
