/// This package keeps Store backups and their retention.
///
/// It owns backup metadata, the filesystem archive backend, the archive codec,
/// the backup manager, and its task executor. Callers must finish a node's write and replication work
/// before closing its storage; backup callbacks are valid only while their
/// owning node remains active.
///
/// @since 1.0
package peruncs.datagrid.cluster.node.backup;
