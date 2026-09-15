package peruncs.datagrid.cluster.nodelibrary.aeron;

import peruncs.datagrid.cluster.storage.aeron.writer.CrashHook;

import java.util.function.BiConsumer;

/// Test-only bridge for the forked Aeron crash harness.
public final class AeronCrashHooks {
    private AeronCrashHooks() {
    }

        /// Installs the writer and provider hooks on the calling test thread.
    ///
    /// @param hook callback that receives the crash seam name and sequence
    public static void install(final BiConsumer<String, Long> hook) {
        CrashHook.install(hook);
        AeronClusterReplicationTransportProvider.setCrashHook(hook);
    }

        /// Clears all writer and provider hooks on the calling test thread.
    public static void clear() {
        CrashHook.clear();
        AeronClusterReplicationTransportProvider.clearCrashHook();
    }

        /// Returns the checkpoint sequence associated with the current write callback.
    ///
    /// @return current checkpoint sequence, or `-1` when none is active
    public static long currentCheckpointSequence() {
        return AeronClusterReplicationTransportProvider.currentCheckpointSequence();
    }

}
