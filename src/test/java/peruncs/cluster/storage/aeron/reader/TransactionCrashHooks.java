package peruncs.cluster.storage.aeron.reader;

import java.util.concurrent.Callable;

/// Test bridge for the reader chunk-assembly observation hook.
///
/// Crash children live outside the reader package while
/// [TransactionAssembler] is package-private, so this bridge is the only
/// sanctioned way for a forked child to bind the chunk observer around a
/// reader. It is test-scoped and never ships in the artifact's API surface.
public final class TransactionCrashHooks {
    private TransactionCrashHooks() {
    }

        /// Observation callback for crash tests: fires after one data chunk of
    /// a multi-chunk transaction has been buffered.
    @FunctionalInterface
    public interface ChunkObserver {
        /// Reports one buffered chunk.
        ///
        /// @param sequence   transaction sequence
        /// @param chunkIndex buffered chunk index
        /// @param chunkCount transaction chunk count
        void afterChunkBuffered(long sequence, int chunkIndex, int chunkCount);
    }

        /// Runs an action with the chunk observer bound to its dynamic scope.
    ///
    /// The reader must be CONSTRUCTED inside this scope: scoped-value
    /// bindings are not inherited by the reader's virtual-thread poller, so
    /// the assembler captures the binding at construction time.
    ///
    /// @param observer invoked after each buffered multi-chunk chunk
    /// @param action  action to run with the hook bound
    /// @return the action's result
    /// @param <T>    action result type
    public static <T> T runWithChunkObserver(final ChunkObserver observer, final Callable<T> action) {
        return TransactionAssembler.runWithChunkObserver(
                observer::afterChunkBuffered,
                action);
    }
}
