package peruncs.datagrid.cluster.node.http;

/// Request body for a manual backup trigger.
///
/// A JSON body of `{"useManualSlot": true}` selects the manual backup slot;
/// `false` or an absent field selects the automatic slot. The boxed type is
/// retained so an absent field is distinguishable from an explicit `false`.
public record PostBackupRequest(Boolean useManualSlot) {
}
