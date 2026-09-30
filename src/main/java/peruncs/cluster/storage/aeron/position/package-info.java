/// Aeron replication positions and reader watermarks.
///
/// The reader watermark acknowledges an applied Store boundary for retention.
/// The cursor carries the corresponding Aeron recording identity through the
/// transport-neutral position interface.
///
/// @since 1.0
package peruncs.cluster.storage.aeron.position;
