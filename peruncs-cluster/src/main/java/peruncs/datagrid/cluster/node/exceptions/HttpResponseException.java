package peruncs.datagrid.cluster.node.exceptions;


import java.util.Collection;
import java.util.List;

/// Reports a node failure together with its HTTP mapping.
public final class HttpResponseException extends NodeLibraryException {
    private final int statusCode;
    private final List<HttpResponseHeader> extraHeaders;

    private HttpResponseException(
            final String message,
            final Throwable cause,
            final int statusCode,
            final List<HttpResponseHeader> extraHeaders
    ) {
        super(message, cause);
        if (statusCode < 400 || statusCode > 599) {
            throw new IllegalArgumentException("statusCode must be an HTTP error status");
        }
        this.statusCode = statusCode;
        this.extraHeaders = List.copyOf(extraHeaders);
    }

        /// Creates a bad-request response.
    public static HttpResponseException badRequest() {
        return badRequest(null, null);
    }

        /// Creates a bad-request response with a message.
    public static HttpResponseException badRequest(final String message) {
        return badRequest(message, null);
    }

        /// Creates a bad-request response with a message and cause.
    public static HttpResponseException badRequest(final String message, final Throwable cause) {
        return new HttpResponseException(message, cause, 400, List.of());
    }

        /// Creates a response for a node that is not the distributor.
    public static HttpResponseException notADistributor(final String message) {
        return new HttpResponseException(message, null, 400, List.of(
                new HttpResponseHeader("StorageNode-NAD", Boolean.TRUE.toString())));
    }

        /// Creates a conflict response for a request that cannot run concurrently.
    public static HttpResponseException conflict(final String message) {
        return conflict(message, null);
    }

        /// Creates a conflict response for a request that cannot run concurrently.
    public static HttpResponseException conflict(final String message, final Throwable cause) {
        return new HttpResponseException(message, cause, 409, List.of());
    }

        /// Creates an internal-server-error response.
    public static HttpResponseException internalServerError() {
        return new HttpResponseException(null, null, 500, List.of());
    }

        /// Creates an internal-server-error response with a cause.
    public static HttpResponseException internalServerError(final Throwable cause) {
        return new HttpResponseException(null, cause, 500, List.of());
    }

        /// Creates a service-unavailable response for a not-ready or unhealthy node.
    ///
    /// Kubernetes readiness and liveness probes treat 503 as retryable, unlike
    /// the 500 an internal error would produce.
    ///
    /// @param message failure message
    /// @return service-unavailable response
    public static HttpResponseException serviceUnavailable(final String message) {
        return new HttpResponseException(message, null, 503, List.of());
    }

        /// Returns the HTTP status code represented by this exception.
    ///
    /// @return HTTP status code
    public int statusCode() {
        return this.statusCode;
    }

        /// Returns additional response headers.
    ///
    /// @return response headers
    public Collection<HttpResponseHeader> extraHeaders() {
        return this.extraHeaders;
    }
}
