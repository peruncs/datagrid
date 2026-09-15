/// This package defines the private envelopes used on an Aeron stream.
///
/// An envelope carries one fragment of a replication transaction and its
/// framing information. Readers validate the version and boundaries before
/// passing data to the neutral storage contract. Application code should use
/// the reader and writer packages instead of depending on these wire classes.
///
/// @since 1.0
package peruncs.datagrid.cluster.storage.aeron.wire;
