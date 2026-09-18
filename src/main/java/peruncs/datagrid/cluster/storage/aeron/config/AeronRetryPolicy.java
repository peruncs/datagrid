package peruncs.datagrid.cluster.storage.aeron.config;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/// Idle pacing and probe spacing for bounded Aeron retry loops.
///
/// All values are nanoseconds. The defaults preserve the historical behavior
/// of the writer and Archive await loops; override them only to trade CPU
/// burn against reaction time on slow or distant Archives.
///
/// @param idleMaxSpins                   maximum spin iterations before yielding in one idle step
/// @param idleMaxYields                  maximum yield iterations before parking in one idle step
/// @param idleMinParkNanos               minimum park duration in one idle step
/// @param idleMaxParkNanos               maximum park duration in one idle step
/// @param jitterBaseNanos                first-attempt delay for full-jitter offer spacing
/// @param jitterCapNanos                 maximum delay for full-jitter offer spacing
/// @param archiveProbeDelayNanos         spacing between Archive progress probes while awaiting a position
/// @param catalogProbeInitialDelayNanos  initial spacing between Archive catalog probes while awaiting start
/// @param catalogProbeMaxDelayNanos      maximum spacing between Archive catalog probes while awaiting start
public record AeronRetryPolicy(
        int idleMaxSpins,
        int idleMaxYields,
        long idleMinParkNanos,
        long idleMaxParkNanos,
        long jitterBaseNanos,
        long jitterCapNanos,
        long archiveProbeDelayNanos,
        long catalogProbeInitialDelayNanos,
        long catalogProbeMaxDelayNanos
) {
    /// Creates the historical retry pacing.
    ///
    /// @return default retry policy
    public static AeronRetryPolicy Default() {
        return new AeronRetryPolicy(
                1, 10, 1L, 1_000_000L,
                1_000L, 1_000_000L,
                10_000_000L,
                1_000_000L, 100_000_000L);
    }

    /// Creates a retry policy.
    public AeronRetryPolicy {
        if (idleMaxSpins < 0 || idleMaxYields < 0) {
            throw new IllegalArgumentException("idle spins and yields must not be negative");
        }
        if (idleMinParkNanos <= 0L || idleMaxParkNanos <= 0L || idleMinParkNanos > idleMaxParkNanos) {
            throw new IllegalArgumentException("idle park bounds must be positive with min <= max");
        }
        if (jitterBaseNanos <= 0L || jitterCapNanos <= 0L) {
            throw new IllegalArgumentException("jitter bounds must be positive");
        }
        if (jitterBaseNanos > jitterCapNanos) {
            throw new IllegalArgumentException("jitter delays must grow with base <= cap");
        }
        if (archiveProbeDelayNanos <= 0L || catalogProbeInitialDelayNanos <= 0L || catalogProbeMaxDelayNanos <= 0L) {
            throw new IllegalArgumentException("probe delays must be positive");
        }
        if (catalogProbeInitialDelayNanos > catalogProbeMaxDelayNanos) {
            throw new IllegalArgumentException("catalog probe delays must grow with min <= max");
        }
    }

        /// Creates an idle strategy paced by this policy's idle bounds.
    ///
    /// Callers that poll a subscription or offer loop should build their
    /// strategy from here so the configured pacing is actually applied instead
    /// of Agrona's hard-coded defaults.
    ///
    /// @return idle strategy using this policy's spin, yield, and park bounds
    public IdleStrategy idleStrategy() {
        return new BackoffIdleStrategy(this.idleMaxSpins, this.idleMaxYields, this.idleMinParkNanos, this.idleMaxParkNanos);
    }
}
