/// This package keeps Store backups and their retention.
///
/// It owns backup metadata, filesystem and network backends, the backup
/// manager and its task executor, and the proxy client used to fetch backups
/// from another node. Callers must finish a node's write and replication work
/// before closing its storage; backup callbacks are valid only while their
/// owning node remains active.
///
/// @since 1.0
package peruncs.datagrid.cluster.node.backup;
