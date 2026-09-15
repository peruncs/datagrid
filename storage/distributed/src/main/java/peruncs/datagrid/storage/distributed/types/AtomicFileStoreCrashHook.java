package peruncs.datagrid.storage.distributed.types;

import java.nio.file.Path;
import java.util.function.BiConsumer;

/** Explicit, reflection-free bridge used by forked metadata crash tests. */
public final class AtomicFileStoreCrashHook
{
	private AtomicFileStoreCrashHook() { }

	/** Installs a hook on the calling thread.
	 *
	 * @param hook callback for crash-test phases
	 */
	public static void install(final BiConsumer<String, Path> hook)
	{
		AtomicFileStore.setTestHook(hook);
	}

	/** Clears the calling thread's hook. */
	public static void clear()
	{
		AtomicFileStore.clearTestHook();
	}
}
