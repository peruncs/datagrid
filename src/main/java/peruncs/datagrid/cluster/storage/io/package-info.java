/// Atomic metadata file writing and path safety.
///
/// The single file-safety implementation for the node: small replication
/// metadata files are written through a forced temporary replacement, and
/// every path the library resolves on behalf of an operator or an archive is
/// rejected when it contains user-controlled symbolic links.
///
/// @since 1.0
package peruncs.datagrid.cluster.storage.io;
