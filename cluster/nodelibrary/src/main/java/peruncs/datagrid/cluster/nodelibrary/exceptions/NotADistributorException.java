package peruncs.datagrid.cluster.nodelibrary.exceptions;


import peruncs.datagrid.cluster.nodelibrary.http.HttpHeader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/** Reports that a request reached a node that is not the distributor. */
public class NotADistributorException extends BadRequestException
{
	/** Response header used when the node is not a distributor. */
	public static final String NAD_HEADER_KEY = "StorageNode-NAD";
	/** Response header value used when the node is not a distributor. */
	public static final String NAD_HEADER_VALUE = Boolean.TRUE.toString();

	/** Creates an exception without a message. */
	public NotADistributorException()
	{
		super();
	}

	/** Creates an exception with a message.
	 * @param message error message
	 */
	public NotADistributorException(final String message)
	{
		super(message);
	}

	/** Creates an exception with a cause.
	 * @param cause underlying cause
	 */
	public NotADistributorException(final Throwable cause)
	{
		super(cause);
	}

	/** Creates an exception with a message and cause.
	 * @param message error message
	 * @param cause underlying cause
	 */
	public NotADistributorException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	/** Creates an exception with full throwable settings.
	 * @param message error message
	 * @param cause underlying cause
	 * @param enableSuppression whether suppression is enabled
	 * @param writableStackTrace whether the stack trace may be written
	 */
	public NotADistributorException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace
	)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}

	@Override
	public Collection<HttpHeader> extraHeaders()
	{
		final var headers = new ArrayList<>(super.extraHeaders());
		headers.add(new HttpHeader(NAD_HEADER_KEY, NAD_HEADER_VALUE));
		return Collections.unmodifiableList(headers);
	}
}
