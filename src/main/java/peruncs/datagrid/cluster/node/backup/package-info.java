/// This package keeps Store backups and their retention.
///
/// It owns backup metadata, the filesystem archive backend, the archive codec,
/// the backup manager, and its task executor. Backups are single-flight at the
/// task executor, and archive publication on a shared volume is serialized by
/// an advisory lock so two nodes never overwrite each other's archives. Backup
/// selection is ordered by replication sequence when known and by
/// NTP-disciplined wall-clock time otherwise. Callers must finish a node's
/// write and replication work before closing its storage; backup callbacks are
/// valid only while their owning node remains active.
///
/// @since 1.0
package peruncs.datagrid.cluster.node.backup;
