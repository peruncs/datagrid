package peruncs.datagrid.cluster.node.aeron;

import peruncs.datagrid.cluster.node.NodeLibraryPropertiesProvider;

/// Neutral node-properties fixture for Aeron tests.
///
/// The environment-backed [NodeLibraryPropertiesProvider.Env] is final, so
/// tests that need per-test values subclass this fixture instead. Every method
/// has a benign default and tests override only the settings they exercise.
/// Public only so forked crash-test children in subpackages can extend it.
public abstract class TestNodeProperties implements NodeLibraryPropertiesProvider {
    @Override
    public boolean isBackupNode() {
        return false;
    }

    @Override
    public Integer keptBackupsCount() {
        return null;
    }

    @Override
    public Integer storageLimitCheckerIntervalMinutes() {
        return null;
    }

    @Override
    public Integer storageLimitGB() {
        return null;
    }

    @Override
    public boolean isProdMode() {
        return false;
    }

    @Override
    public Long dataMergerTimeoutMs() {
        return null;
    }

    @Override
    public Long dataMergerCachedDataLimit() {
        return null;
    }

    @Override
    public Long dataMergerApplyTimeoutMs() {
        return null;
    }

    @Override
    public Long writerLeaseStalenessMillis() {
        return null;
    }
}
