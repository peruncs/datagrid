/// This package defines the settings that shape an Aeron storage stream.
///
/// Configuration values describe endpoints, stream identity, and frame
/// limits. Members that share a stream must use compatible values. The records
/// are immutable after construction so a running reader and writer see one
/// stable configuration.
///
/// @since 1.0
package peruncs.datagrid.cluster.storage.aeron.config;
