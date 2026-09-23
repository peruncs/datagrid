package peruncs.datagrid.cluster.storage.binary;

/// The lifecycle seam the queue and the worker see of the merger.
///
/// The facade ([StorageBinaryDataMerger]) is the lifecycle owner:
/// it holds the executors, the terminal failure latch, the disposal flag,
/// and the configured limits, and it composes [ApplyQueue] and
/// [ApplyWorker]. A separate lifecycle class would own no state beyond
/// those references alone, so the split keeps lifecycle in the facade and
/// narrows the collaborators' view to this interface instead.
interface MergerLifecycle {
    /* One slow batch must not brick the reader: a GC pause or a slow disk
     * can exceed the apply timeout while the worker is still progressing.
     * The bounded wait is retried this many times before latching a
     * terminal failure; a genuinely wedged worker still fails after the
     * budget, so shutdown stays bounded. */
    int APPLY_TIMEOUT_RETRIES = 2;

    /// Returns the latched terminal failure, or `null` while healthy.
    ///
    /// @return terminal failure, or `null`
    RuntimeException failure();

    /// Returns whether disposal has begun.
    ///
    /// @return `true` once the merger is shutting down
    boolean isDisposed();

    /// Records the first failure without returning it.
    ///
    /// Later failures are dropped so concurrent paths cannot overwrite the
    /// root cause.
    ///
    /// @param failure failure to latch
    void latchFailure(RuntimeException failure);

    /// Records the first failure for paths that rethrow the original cause.
    ///
    /// @param message failure description
    /// @param cause   original cause, rethrown by the caller to preserve its type
    void noteFailure(String message, Throwable cause);

    /// Records the first wait/interrupt/timeout failure and returns the
    /// latched one.
    ///
    /// Lifecycle outcomes carry [peruncs.datagrid.cluster.errors.ReplicationUnavailableException]
    /// so callers can distinguish an unusable-but-healthy transport message
    /// from a corrupt/unusable assembled one. Later failures are dropped.
    ///
    /// @param message failure description
    /// @param cause   original cause
    /// @return the latched terminal failure
    RuntimeException recordLifecycleFailure(String message, Throwable cause);

    /// Fails the merger when one batch overruns its materialization budget.
    ///
    /// Invoked by the watchdog; a no-op unless `startedNanos` still matches
    /// the batch the worker is applying.
    ///
    /// @param startedNanos nanoTime stamp of the batch being applied
    void onMaterializationBudgetExpired(long startedNanos);
}
