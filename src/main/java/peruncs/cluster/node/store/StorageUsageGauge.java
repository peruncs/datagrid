package peruncs.cluster.node.store;

import org.eclipse.serializer.afs.types.ADirectory;

import java.io.FileNotFoundException;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.Logger.Level.*;
import static org.eclipse.serializer.util.X.notNull;

/// Measures the bytes currently used by a storage directory.
///
/// A running Store may remove a file while the directory is being visited.
/// That file is skipped and the measurement continues. Any other failure
/// makes the whole measurement unknown (`-1`): an understated total must
/// never reopen write admission.
public final class StorageUsageGauge {
    private static final System.Logger LOGGER = System.getLogger(StorageUsageGauge.class.getName());
    /// Refresh cadence for the cached directory measurement.
    public static final Duration REFRESH_INTERVAL = Duration.ofSeconds(5);

    private final ADirectory storageDir;
    private final AtomicLong lastLog = new AtomicLong(System.currentTimeMillis());
    private volatile long cachedBytes = -1L;

    private StorageUsageGauge(final ADirectory storageDir) {
        this.storageDir = storageDir;
    }

    /// Creates a disk-space gauge for one storage directory.
    ///
    /// @param storageDir storage directory
    /// @return disk-space reader
    public static StorageUsageGauge create(final ADirectory storageDir) {
        return new StorageUsageGauge(notNull(storageDir));
    }

    /// Reads the latest scheduled storage-directory measurement.
    ///
    /// @return used bytes, or `-1` until the first measurement finishes
    public long readUsedDiskSpaceBytes() {
        return this.cachedBytes;
    }

    /// Measures used bytes synchronously, updating the snapshot returned to readers.
    ///
    /// @return measured bytes, or `-1` when part of the directory could not be measured
    public long measureNow() {
        final long sizeBytes = this.totalSize(this.storageDir);
        this.cachedBytes = sizeBytes;
        final long nowMillis = System.currentTimeMillis();
        final long previousLog = this.lastLog.get();
        if (LOGGER.isLoggable(TRACE) && nowMillis - previousLog > 600_000L &&
            this.lastLog.compareAndSet(previousLog, nowMillis)) {
            LOGGER.log(TRACE, "Read current storage disk space (%s)".formatted(sizeBytes));
        }
        return sizeBytes;
    }

    private long totalSize(final ADirectory dir) {
        final long[] total = { 0L };
        /* Overflow is latched, not repeatedly logged: once usage saturates
         * the long space, every file of every refresh would otherwise warn. */
        final boolean[] overflowLogged = { false };
        final boolean[] incomplete = { false };
        this.measureDirectory(dir, total, overflowLogged, incomplete);
        return incomplete[0] ? -1L : total[0];
    }

    private void measureDirectory(
            final ADirectory dir, final long[] total, final boolean[] overflowLogged, final boolean[] incomplete) {
        dir.iterateFiles(file -> {
            try {
                total[0] = Math.addExact(total[0], file.size());
            } catch (final ArithmeticException overflow) {
                total[0] = Long.MAX_VALUE;
                if (!overflowLogged[0]) {
                    overflowLogged[0] = true;
                    LOGGER.log(WARNING, "Storage size overflow while measuring %s".formatted(file));
                }
            } catch (final RuntimeException failure) {
                this.noteFailure("file " + file, failure, incomplete);
            }
        });
        try {
            dir.iterateDirectories(child -> this.measureDirectory(child, total, overflowLogged, incomplete));
        } catch (final RuntimeException failure) {
            this.noteFailure("a storage directory", failure, incomplete);
        }
    }

    private void noteFailure(final String what, final RuntimeException failure, final boolean[] incomplete) {
        if (disappeared(failure)) {
            LOGGER.log(DEBUG, "Skipping %s: it was removed while measuring".formatted(what), failure);
            return;
        }
        incomplete[0] = true;
        LOGGER.log(WARNING, "Could not measure %s; storage usage is unknown".formatted(what), failure);
    }

    /// Reports whether the failure is a concurrent removal rather than an I/O or permission error.
    private static boolean disappeared(final Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof NoSuchFileException || cause instanceof FileNotFoundException) return true;
            if (cause.getCause() == cause) return false;
        }
        return false;
    }
}
