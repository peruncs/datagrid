package peruncs.cluster.storage.aeron.checkpoint;

import java.nio.file.Path;
import java.util.function.BiConsumer;

/// Lets forked crash tests pause a journal write without exporting the hook.
public final class CheckpointJournalCrashHooks {
    private CheckpointJournalCrashHooks() {
    }

    public static void runWithHook(final BiConsumer<String, Path> hook, final Runnable action) {
        AeronReplicationCheckpointStore.runWithTestHook(hook, action);
    }
}
