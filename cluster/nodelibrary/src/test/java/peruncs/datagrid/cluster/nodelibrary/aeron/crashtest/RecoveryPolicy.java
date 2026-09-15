package peruncs.datagrid.cluster.nodelibrary.aeron.crashtest;

/** Recovery result emitted by a provider crash child. */
enum RecoveryPolicy
{
	CONTINUE,
	REPLAY_FROM_ARCHIVE,
	RESEED_REQUIRED,
	FAIL_CLOSED,
	/** The child completed with an armed barrier that never fired. */
	HARNESS_ERROR
}
