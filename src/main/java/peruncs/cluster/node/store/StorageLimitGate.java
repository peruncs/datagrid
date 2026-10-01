package peruncs.cluster.node.store;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

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
    private static final int MEASUREMENT_KNOWN = 1;
    private static final int LIMIT_REACHED = 1 << 1;
    /// Wall-clock spacing between "still full" reminders while the limit stays reached.
    private static final long FULL_REMINDER_INTERVAL_MILLIS = 600_000L;
    private long lastFullLogMillis;
    private static final long BYTES_PER_GIGABYTE = 1_000_000_000L;
    private static final int DEFAULT_RELEASE_PERMILLE = 100;

    private final AtomicInteger state = new AtomicInteger();
    private final AtomicBoolean unknownWarningLogged = new AtomicBoolean();
    private final long limitBytes;
    private final long releaseBytes;

    private StorageLimitGate(final long limitBytes, final int releasePermille) {
        this.limitBytes = limitBytes;
        if (releasePermille < 0 || releasePermille > 1_000) {
            throw new IllegalArgumentException("releasePermille must be between 0 and 1000");
        }
        final long releaseBytes = this.limitBytes / 1_000L * releasePermille +
                this.limitBytes % 1_000L * releasePermille / 1_000L;
        this.releaseBytes = this.limitBytes - releaseBytes;
    }

    /// Creates a gate for the given byte limit with the default hysteresis.
    ///
    /// @param limitBytes the limit in bytes
    /// @return a gate that fails closed until its first measurement
    public static StorageLimitGate create(final long limitBytes) {
        return create(limitBytes, DEFAULT_RELEASE_PERMILLE);
    }

    /// Creates a gate with an explicit release hysteresis.
    ///
    /// @param limitBytes     the limit in bytes
    /// @param releasePermille hysteresis below the limit, in tenths of a
    ///                        percent (100 = release ten percent below)
    /// @return a gate that has not reached its limit yet
    public static StorageLimitGate create(final long limitBytes, final int releasePermille) {
        if (limitBytes <= 0L) {
            throw new IllegalArgumentException("Storage limit must be positive");
        }
        return new StorageLimitGate(limitBytes, releasePermille);
    }

    /// Records one storage measurement.
    ///
    /// @param usedBytes measured used bytes
    public void updateUsage(final long usedBytes) {
        if (usedBytes < 0L) {
            this.markUnknown();
            return;
        }
        this.unknownWarningLogged.set(false);
        int current;
        int updated;
        do {
            current = this.state.get();
            final boolean wasLimited = (current & LIMIT_REACHED) != 0;
            final boolean limited = wasLimited
                    ? usedBytes > this.releaseBytes
                    : usedBytes >= this.limitBytes;
            updated = MEASUREMENT_KNOWN | (limited ? LIMIT_REACHED : 0);
            if (current == updated) return;
        } while (!this.state.compareAndSet(current, updated));
    }

    /// Marks the usage as unknown, so writes are refused until a measurement succeeds.
    ///
    /// An already reached limit stays reached.
    public void markUnknown() {
        this.state.updateAndGet(current -> current & ~MEASUREMENT_KNOWN);
    }

    /// Reports whether a measurement has reached the configured limit.
    ///
    /// @return `true` after a measurement reaches the limit
    public boolean limitReached() {
        final int current = this.state.get();
        return (current & MEASUREMENT_KNOWN) == 0 || (current & LIMIT_REACHED) != 0;
    }

    /// Returns the limit in decimal gigabytes.
    ///
    /// @return limit in gigabytes
    public long limitGb() {
        return this.limitBytes / BYTES_PER_GIGABYTE;
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
    public Runnable createScheduledWork(final LongSupplier diskSpaceReader) {
        Objects.requireNonNull(diskSpaceReader, "diskSpaceReader");
        return () ->
        {
            if (LOGGER.isLoggable(TRACE)) {
                LOGGER.log(TRACE, "Executing storage limit checker task");
            }
            final long nowMillis = System.currentTimeMillis();
            final long usedBytes = diskSpaceReader.getAsLong();
            if (usedBytes < 0L) {
                this.markUnknown();
                if (this.unknownWarningLogged.compareAndSet(false, true)) {
                    LOGGER.log(WARNING, "Storage usage is unknown; writes are disabled until a measurement succeeds");
                }
                /* Surface the failure to the maintenance scheduler so repeated
                 * unknown measurements degrade the node instead of staying silent. */
                throw new IllegalStateException("storage usage is unknown");
            }
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
