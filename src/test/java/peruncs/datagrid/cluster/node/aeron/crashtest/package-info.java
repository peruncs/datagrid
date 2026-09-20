/// Provider process harnesses and independent crash evidence oracles.
///
/// [ProviderCrashMatrixIT] covers writer/checkpoint/Archive crash points,
/// [ExternalArchiveCrashIT] covers the remote Archive deployment, and
/// `WriterTakeoverCrashMatrixIT` covers the cross-process fencing-lease
/// takeover: a SIGKILLed writer is stolen after the staleness bound, the
/// successor mints a strictly greater token, and old-token frames fail closed.
package peruncs.datagrid.cluster.node.aeron.crashtest;
