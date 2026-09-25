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
/// parameter because the context belongs to the publisher or assembler that
/// owns the buffers. The hot path therefore needs no thread-local or dynamic
/// scope.
///
/// @since 1.0
package peruncs.cluster.storage.aeron.wire;
