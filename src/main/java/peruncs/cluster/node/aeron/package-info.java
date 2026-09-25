/// This package carries cluster replication through Aeron.
///
/// [AeronTransport] is the public
/// transport facade; it composes one owner per lifecycle concern — runtime
/// (driver/Archive), writer, reader, and retention — over a small shared
/// lifecycle state, and delegates validated settings, capacity admission,
/// health, reader watermarks, and durable positions to package-private
/// components. The neutral cluster API starts and stops the node; this
/// package supplies the transport work between those calls. A transport
/// instance belongs to one node and must not be shared between nodes.
///
/// Choose the transport with
/// `ECLIPSE_DATAGRID_REPLICATION_TRANSPORT=aeron`. Keep the Aeron
/// settings consistent for every member that shares a stream.
///
/// @since 1.0
package peruncs.cluster.node.aeron;
