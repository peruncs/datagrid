/// This package defines the settings that shape an Aeron storage stream.
///
/// Configuration values describe endpoints, stream identity, frame, and poll
/// limits. Members that share a stream must use compatible values. Settings
/// are immutable records validated at construction, so a running reader and
/// writer see one stable configuration; the optional builder keeps call sites
/// readable when only a few limits differ from the defaults.
///
/// @since 1.0
package peruncs.cluster.storage.aeron.config;
