package peruncs.datagrid.cluster.nodelibrary.exceptions;


import peruncs.datagrid.cluster.nodelibrary.http.HttpHeader;

import java.util.Collection;
import java.util.Collections;

/// When a subclass of this is thrown it should be mapped to the corresponding
/// http status. (e.g. [InternalServerErrorException] should map to a
/// status code 500 response)
public abstract class HttpResponseException extends NodelibraryException {
        /// Creates an HTTP response exception without a message.
    protected HttpResponseException() {
        super();
    }

        /// Creates an HTTP response exception with a message.
    ///
    /// @param message error message
    protected HttpResponseException(final String message) {
        super(message);
    }

        /// Creates an HTTP response exception with a cause.
    ///
    /// @param cause underlying cause
    protected HttpResponseException(final Throwable cause) {
        super(cause);
    }

        /// Creates an HTTP response exception with a message and cause.
    ///
    /// @param message error message
    /// @param cause   underlying cause
    protected HttpResponseException(final String message, final Throwable cause) {
        super(message, cause);
    }

        /// Creates an HTTP response exception with full throwable settings.
    ///
    /// @param message            error message
    /// @param cause              underlying cause
    /// @param enableSuppression  whether suppression is enabled
    /// @param writableStackTrace whether the stack trace may be written
    protected HttpResponseException(
            final String message,
            final Throwable cause,
            final boolean enableSuppression,
            final boolean writableStackTrace
    ) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

        /// Returns the HTTP status code represented by this exception.
    ///
    /// @return HTTP status code
    public abstract int statusCode();

        /// Returns additional response headers.
    ///
    /// @return response headers
    public Collection<HttpHeader> extraHeaders() {
        return Collections.emptyList();
    }
}
