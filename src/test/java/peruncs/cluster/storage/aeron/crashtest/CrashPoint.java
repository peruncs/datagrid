package peruncs.cluster.storage.aeron.crashtest;

/// Named boundaries used by deterministic and forked crash tests.
///
/// Crash seams exercised by the writer and provider tests.
public enum CrashPoint {
    BEFORE_PUBLICATION_CONNECTED,
    BEFORE_PREPARE,
    AFTER_DICTIONARY_CHUNKS,
    AFTER_DATA_CHUNKS,
    AFTER_PREPARE,
    AFTER_PREPARE_BEFORE_LOCAL_WRITE,
    AFTER_LOCAL_WRITE_BEFORE_COMMIT,
    AFTER_PREPARE_FAILURE_ABORT_OFFERED,
    BEFORE_COMMIT_OFFER,
    AFTER_COMMIT_OFFER,
    AFTER_COMMIT_RECORDED
}
