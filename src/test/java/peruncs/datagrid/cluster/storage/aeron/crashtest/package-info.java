/// Forked-process crash verification for the Aeron replication paths.
///
/// Durable-state machine correctness under a real SIGKILL cannot be tested
/// in-process: the failure semantics are precisely about what survives the
/// JVM. Every integration test here forks a child JVM, parks it at a named
/// crash boundary, kills it with `destroyForcibly()`, and then starts a fresh
/// process over the same directories to assert the recovery outcome
/// (resumed, `RESEED_REQUIRED`, or fail-closed) against the surviving files.
/// Parent and child communicate exclusively through atomic control files
/// (`ready`/`milestone.reached`/`release`/`outcome`) inside a per-test tree.
///
/// - `ProviderCrashMatrixIT`/`ProviderCrashChildMain`: writer-side commit,
///   checkpoint, and recovery cells.
/// - `AeronReaderCrashMatrixIT`/`ReaderCrashChildMain`: reader-side
///   import/cursor/marker cells, including corrupted and deleted cursors,
///   a torn in-flight marker write, the post-sync/pre-resolve window,
///   crashes inside recovery's own parse window, and a four-crash loop of
///   mixed write-side and read-side barrier windows.
/// - `AeronCrashMatrixIT`: the original AtomicFileWriter boundary matrix.
/// - `ArchiveArtifactMutator` and friends: offline evidence manipulation
///   (segment truncation, catalog surgery) for corruption cells.
package peruncs.datagrid.cluster.storage.aeron.crashtest;
