/// This package defines the settings that shape an Aeron storage stream.
///
/// Configuration values describe endpoints, stream identity, and frame
/// limits. Members that share a stream must use compatible values. Settings
/// are immutable after construction so a running reader and writer see one
/// stable configuration; the retry policy is a record while the stream
/// configuration itself is a validated builder.
///
/// @since 1.0
package peruncs.datagrid.cluster.storage.aeron.config;
