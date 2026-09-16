package peruncs.datagrid.cluster.node.http;

/// Request body for a manual backup trigger.
///
/// `true` selects the manual backup slot and `false` selects the automatic
/// slot. A null value is rejected with a 400 because the slot choice is
/// explicit: the boxed type exists only so the controller can tell "absent"
/// from "false".
///
/// @param useManualSlot whether to use the manual backup slot
public record PostBackupRequest(Boolean useManualSlot) { }
