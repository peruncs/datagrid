package peruncs.datagrid.cluster.node.http;

/// A response header carried across the HTTP boundary.
///
/// @param key   header name
/// @param value header value
public record HttpHeader(String key, String value) {
}
