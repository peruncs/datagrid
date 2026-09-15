package peruncs.datagrid.cluster.nodelibrary.store;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Records whether storage measurements reached the configured limit.
 *
 * <p>The housekeeper updates the gate from its measurement thread while
 * request threads read it to decide whether writes are still accepted. Once
 * the limit is reached, a ten-percent hysteresis band prevents usage near the
 * boundary from oscillating between writable and read-only.</p>
 */
public final class StorageLimitGate {
    private static final long BYTES_PER_GIGABYTE = 1_000_000_000L;

    private final AtomicBoolean limitReached = new AtomicBoolean(false);
    private final int limitGb;
    private final long limitBytes;
    private final long releaseBytes;

    private StorageLimitGate(final int limitGb) {
        this.limitGb = limitGb;
        this.limitBytes = Math.multiplyExact(limitGb, BYTES_PER_GIGABYTE);
        this.releaseBytes = this.limitBytes - this.limitBytes / 10L;
    }

    /**
     * Creates a gate for the given limit.
     *
     * @param limitGb the limit in decimal gigabytes
     * @return a gate that has not reached its limit yet
     */
    public static StorageLimitGate New(final int limitGb) {
        if (limitGb <= 0) {
            throw new IllegalArgumentException("Storage limit must be a positive number of gigabytes");
        }
        return new StorageLimitGate(limitGb);
    }

    /**
     * Records one storage measurement.
     *
     * @param usedBytes measured used bytes
     */
    public void updateUsage(final long usedBytes) {
        if (this.limitReached.get()) {
            if (usedBytes <= this.releaseBytes) {
                this.limitReached.set(false);
            }
        } else if (usedBytes >= this.limitBytes) {
            this.limitReached.set(true);
        }
    }

    /**
     * Reports whether a measurement has reached the configured limit.
     *
     * @return {@code true} after a measurement reaches the limit
     */
    public boolean limitReached() {
        return this.limitReached.get();
    }

    /**
     * Returns the limit in decimal gigabytes.
     *
     * @return limit in gigabytes
     */
    public int limitGb() {
        return this.limitGb;
    }

    /**
     * Returns the limit in bytes.
     *
     * @return limit in bytes
     */
    public long limitBytes() {
        return this.limitBytes;
    }
}
