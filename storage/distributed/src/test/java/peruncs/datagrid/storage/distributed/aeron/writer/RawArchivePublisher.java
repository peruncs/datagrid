package peruncs.datagrid.storage.distributed.aeron.writer;

import java.nio.ByteBuffer;

/** Test-only bridge for crash fixtures that need raw Archive frames. */
public final class RawArchivePublisher
{
	private RawArchivePublisher()
	{
	}

	/** Publishes a fixture transaction without exposing the raw path in production APIs. */
	public static void publish(
		final AeronArchiveReplicationPublisher publisher,
		final byte[] dictionary,
		final ByteBuffer[] data
	)
	{
		publisher.publishTransaction(dictionary, data);
	}
}
