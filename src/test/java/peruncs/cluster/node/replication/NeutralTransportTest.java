package peruncs.cluster.node.replication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;

/// Tests neutral transport behavior.
class NeutralTransportTest {

    /// Verifies no op transport keeps core usable without any provider dependency.
    @Test
    void noOpTransportKeepsCoreUsableWithoutAnyProviderDependency() {
        final ClusterReplicationTransport transport = ClusterReplicationTransport.noOp();
        assertNull(transport.replicationMark());
        transport.close();
    }
}
