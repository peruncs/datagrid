package peruncs.datagrid.cluster.node.exceptions;

/// A response header carried across the HTTP boundary.
///
/// @param key   header name
/// @param value header value
public record HttpResponseHeader(String key, String value) {
}
