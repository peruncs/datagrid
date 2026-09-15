package peruncs.datagrid.cluster.nodelibrary.backup;

/// Identifies a backup by its creation time and backup slot.
///
/// @param timestamp  backup creation time
/// @param manualSlot whether the backup uses the manual slot
public record BackupMetadata(long timestamp, boolean manualSlot) {
}
