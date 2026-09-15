package peruncs.datagrid.cluster.nodelibrary.aeron.crashtest;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Isolates every process crash cell and owns its loopback port reservations.
 *
 * <p>Port probes are held only for the lifetime of this layout. The crash
 * profile therefore runs cells sequentially; enabling parallel forks requires
 * child-side bind retries.</p>
 */
final class DirectoryLayout implements AutoCloseable
{
	private final Path root;
	private final int livePort;
	private final int controlPort;

	private DirectoryLayout(final Path root, final int livePort, final int controlPort)
	{
		this.root = root;
		this.livePort = livePort;
		this.controlPort = controlPort;
	}

	static DirectoryLayout create() throws IOException
	{
		final Path root = Files.createTempDirectory("datagrid-provider-crash-");
		final int livePort = freePort();
		int controlPort;
		do
		{
			controlPort = freePort();
		}
		while (controlPort == livePort);
		return new DirectoryLayout(root, livePort, controlPort);
	}

	Path root() { return this.root; }
	int livePort() { return this.livePort; }
	int controlPort() { return this.controlPort; }

	@Override
	public void close() throws IOException
	{
		if (Boolean.getBoolean("dg.crash.keepArtifacts")) return;
		if (!Files.exists(this.root)) return;
		IOException failure = null;
		try (var paths = Files.walk(this.root))
		{
			for (final Path path : paths.sorted(Comparator.reverseOrder()).toList())
			{
				try { Files.deleteIfExists(path); }
				catch (final IOException deleteFailure)
				{
					if (failure == null) failure = deleteFailure;
					else failure.addSuppressed(deleteFailure);
				}
			}
		}
		if (failure != null) throw failure;
	}

	private static int freePort() throws IOException
	{
		try (ServerSocket socket = new ServerSocket(0))
		{
			return socket.getLocalPort();
		}
	}
}
