package peruncs.datagrid.storage.distributed.aeron.wire;

import peruncs.datagrid.storage.distributed.types.StorageBinaryDataException;

/** Signals malformed or corrupted bytes received from an Aeron peer. */
public final class ReplicationWireException extends StorageBinaryDataException
{
	/** Creates a wire failure with a diagnostic message. */
	public ReplicationWireException(final String message)
	{
		super(message);
	}

	/** Creates a wire failure with its underlying cause. */
	public ReplicationWireException(final String message, final Throwable cause)
	{
		super(message, cause);
	}
}
