package peruncs.datagrid.cluster.node.http;

/// Request body for a manual backup trigger.
///
/// A JSON body of `{"useManualSlot": true}` selects the manual backup slot and
/// `{"useManualSlot": false}` selects the automatic slot. An absent field is
/// rejected with a 400 because the slot choice is explicit: the boxed type
/// exists only so the controller can tell "absent" from "false".
public record PostBackupRequest(Boolean useManualSlot) {
}
