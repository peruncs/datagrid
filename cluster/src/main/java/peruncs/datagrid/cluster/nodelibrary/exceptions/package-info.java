/// This package defines failures at the node and storage boundaries.
///
/// HTTP-facing failures carry a status and optional headers so an adapter
/// can build a response without knowing internal implementation classes. The
/// remaining exceptions preserve the original cause and are intended for the
/// node-level error handler.
///
/// @since 1.0
package peruncs.datagrid.cluster.nodelibrary.exceptions;
