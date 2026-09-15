package peruncs.datagrid.cluster.storage.aeron.config;

/// Idle pacing and probe spacing for bounded Aeron retry loops.
///
/// All values are nanoseconds. The defaults preserve the historical behavior
/// of the writer and Archive await loops; override them only to trade CPU
/// burn against reaction time on slow or distant Archives.
public record AeronRetryPolicy(
        /// Maximum spin iterations before yielding in one idle step.
        int idleMaxSpins,
        /// Maximum yield iterations before parking in one idle step.
        int idleMaxYields,
        /// Minimum park duration in one idle step.
        long idleMinParkNanos,
        /// Maximum park duration in one idle step.
        long idleMaxParkNanos,
        /// First-attempt delay for full-jitter offer spacing.
        long jitterBaseNanos,
        /// Maximum delay for full-jitter offer spacing.
        long jitterCapNanos,
        /// Spacing between Archive progress probes while awaiting a position.
        long archiveProbeDelayNanos,
        /// Initial spacing between Archive catalog probes while awaiting start.
        long catalogProbeInitialDelayNanos,
        /// Maximum spacing between Archive catalog probes while awaiting start.
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
}
