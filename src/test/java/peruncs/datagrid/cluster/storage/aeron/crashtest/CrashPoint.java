package peruncs.datagrid.cluster.storage.aeron.crashtest;

/// Named boundaries used by deterministic and forked crash tests.
///
/// Writer points are consumed by the provider child, reader points by the
/// reader child, and file phases by the corresponding fixture. A name in this
/// enum is not itself evidence that every process tier implements that point;
/// each child rejects points outside its supported scope.
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
    AFTER_COMMIT_RECORDED,
    AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT,
    AFTER_ABORT_OFFERED,
    BEFORE_JOURNAL_SLOT_WRITE,
    DURING_JOURNAL_SLOT_WRITE,
    AFTER_JOURNAL_SLOT_FORCE,
    AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE,
    REPLAY_BEFORE_FIRST_IMPORT,
    DURING_STORE_IMPORT,
    DURING_STORE_IMPORT_FAILURE,
    AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE,
    DURING_CURSOR_FILE_WRITE,
    AFTER_CURSOR_TEMP_WRITE_BEFORE_RENAME,
    AFTER_CURSOR_RENAME_BEFORE_DIRECTORY_SYNC,
    AFTER_RECOVERY_CHECKPOINT_READ
}
