package peruncs.cluster.node.store;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.System.Logger.Level.*;

/// Records whether storage measurements reached the configured limit.
///
/// The maintenance scheduler updates the gate from its measurement thread while
/// request threads read it to decide whether writes are still accepted. Once
/// the limit is reached, a configurable hysteresis band (ten percent by
/// default) prevents usage near the boundary from oscillating between
/// writable and read-only.
public final class StorageLimitGate {
    private static final System.Logger LOGGER = System.getLogger(StorageLimitGate.class.getName());
    /// Wall-clock spacing between "still full" reminders while the limit stays reached.
    private static final long FULL_REMINDER_INTERVAL_MILLIS = 600_000L;
    private long lastFullLogMillis;
    private static final long BYTES_PER_GIGABYTE = 1_000_000_000L;
    private static final int DEFAULT_RELEASE_PERMILLE = 100;

    private final AtomicBoolean limitReached = new AtomicBoolean(false);
    private final int limitGb;
    private final long limitBytes;
    private final long releaseBytes;

    private StorageLimitGate(final int limitGb, final int releasePermille) {
        this.limitGb = limitGb;
        this.limitBytes = Math.multiplyExact(limitGb, BYTES_PER_GIGABYTE);
        if (releasePermille < 0 || releasePermille > 1_000) {
            throw new IllegalArgumentException("releasePermille must be between 0 and 1000");
        }
        this.releaseBytes = this.limitBytes - this.limitBytes * releasePermille / 1_000L;
    }

        /// Creates a gate for the given limit with the default hysteresis.
    ///
    /// @param limitGb the limit in decimal gigabytes
    /// @return a gate that has not reached its limit yet
    public static StorageLimitGate create(final int limitGb) {
        return create(limitGb, DEFAULT_RELEASE_PERMILLE);
    }

        /// Creates a gate with an explicit release hysteresis.
    ///
    /// @param limitGb        the limit in decimal gigabytes
    /// @param releasePermille hysteresis below the limit, in tenths of a
    ///                        percent (100 = release ten percent below)
    /// @return a gate that has not reached its limit yet
    public static StorageLimitGate create(final int limitGb, final int releasePermille) {
        if (limitGb <= 0) {
            throw new IllegalArgumentException("Storage limit must be a positive number of gigabytes");
        }
        return new StorageLimitGate(limitGb, releasePermille);
    }

        /// Records one storage measurement.
    ///
    /// @param usedBytes measured used bytes
    public void updateUsage(final long usedBytes) {
        if (this.limitReached.get()) {
            if (usedBytes <= this.releaseBytes) {
                this.limitReached.set(false);
            }
        } else if (usedBytes >= this.limitBytes) {
            this.limitReached.set(true);
        }
    }

        /// Reports whether a measurement has reached the configured limit.
    ///
    /// @return `true` after a measurement reaches the limit
    public boolean limitReached() {
        return this.limitReached.get();
    }

        /// Returns the limit in decimal gigabytes.
    ///
    /// @return limit in gigabytes
    public int limitGb() {
        return this.limitGb;
    }

        /// Returns the limit in bytes.
    ///
    /// @return limit in bytes
    public long limitBytes() {
        return this.limitBytes;
    }

        /// Creates the periodic storage-limit check task.
    ///
    /// The task measures used disk space and records it in this gate, which
    /// request threads read to decide whether writes are still accepted.
    ///
    /// @param diskSpaceReader storage measurement source
    /// @return limit-check task for maintenance scheduling
    public Runnable createScheduledWork(final StorageUsageGauge diskSpaceReader) {
        Objects.requireNonNull(diskSpaceReader, "diskSpaceReader");
        return () ->
        {
            if (LOGGER.isLoggable(TRACE)) {
                LOGGER.log(TRACE, "Executing storage limit checker task");
            }
            final long nowMillis = System.currentTimeMillis();
            final long usedBytes = diskSpaceReader.readUsedDiskSpaceBytes();
            final long usedGb = usedBytes / BYTES_PER_GIGABYTE;
            if (LOGGER.isLoggable(DEBUG)) {
                LOGGER.log(DEBUG,
                        "Storage Size: %sgb/%sgb (%s bytes)".formatted(usedGb, this.limitGb(), usedBytes));
            }
            final boolean wasLimited = this.limitReached();
            this.updateUsage(usedBytes);
            if (!wasLimited && this.limitReached()) {
                this.lastFullLogMillis = nowMillis;
                LOGGER.log(WARNING, "Storage limit reached! No more data will be stored!");
            } else if (wasLimited && !this.limitReached()) {
                LOGGER.log(System.Logger.Level.INFO, "Storage usage fell back below the %s GB limit; storing resumes".formatted(this.limitGb()));
            } else if (this.limitReached() && nowMillis - this.lastFullLogMillis >= FULL_REMINDER_INTERVAL_MILLIS) {
                /* Operators get a throttled reminder while the node stays
                 * full instead of one warning followed by silence. */
                this.lastFullLogMillis = nowMillis;
                LOGGER.log(WARNING,
                        "Storage limit still reached! No more data will be stored!");
            }
        };
    }
}
