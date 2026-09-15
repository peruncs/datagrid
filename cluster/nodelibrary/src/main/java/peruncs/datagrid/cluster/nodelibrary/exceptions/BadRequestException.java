package peruncs.datagrid.cluster.nodelibrary.exceptions;

/** Reports a request that the node cannot accept. */
public class BadRequestException extends HttpResponseException
{
	/** Creates an exception without a message. */
	public BadRequestException()
	{
		super();
	}

	/** Creates an exception with a message.
	 *
	 * @param message error message
	 */
	public BadRequestException(final String message)
	{
		super(message);
	}

	/** Creates an exception with a cause.
	 *
	 * @param cause underlying cause
	 */
	public BadRequestException(final Throwable cause)
	{
		super(cause);
	}

	/** Creates an exception with a message and cause.
	 *
	 * @param message error message
	 * @param cause underlying cause
	 */
	public BadRequestException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	/** Creates an exception with full throwable settings.
	 *
	 * @param message error message
	 * @param cause underlying cause
	 * @param enableSuppression whether suppression is enabled
	 * @param writableStackTrace whether the stack trace may be written
	 */
	public BadRequestException(
		final String message,
		final Throwable cause,
		final boolean enableSuppression,
		final boolean writableStackTrace
	)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}

	@Override
	public int statusCode()
	{
		return 400;
	}
}
