/// This package defines the private envelopes used on an Aeron stream.
///
/// An envelope carries one fragment of a replication transaction and its
/// framing information. Readers validate the version and boundaries before
/// passing data to the neutral storage contract. Application code should use
/// the reader and writer packages instead of depending on these wire classes.
///
/// # Explicit checksum context, no thread-local state
///
/// The CRC32C checksum context is passed through encode/decode as an explicit
/// parameter. An earlier revision bound it through a `ScopedValue` on every
/// encoded chunk and decoded frame; the scoped bind/unbind bought nothing
/// (the context is always the same instance per publisher or assembler) and
/// cost setup on the hottest path. Do not restore a thread-local or scoped
/// binding here: the context belongs to the operation that owns the buffers.
///
/// @since 1.0
package peruncs.datagrid.cluster.storage.aeron.wire;
