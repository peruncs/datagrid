/// Forked-process crash verification for the Aeron replication paths.
///
/// Test support for Aeron replication and metadata crash checks.
///
/// Forked tests use separate processes where crash survival matters. Store
/// mark atomicity, atomic metadata replacement, driver failure, and writer
/// publication seams each use their own small harness. ArchiveArtifactMutator
/// supplies offline segment and catalog corruption for reader/writer tests.
package peruncs.cluster.storage.aeron.crashtest;
