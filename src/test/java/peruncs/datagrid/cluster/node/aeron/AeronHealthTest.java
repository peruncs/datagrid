package peruncs.datagrid.cluster.node.aeron;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.api.ReplicationState;
import peruncs.datagrid.cluster.errors.ReseedRequiredException;
import peruncs.datagrid.cluster.node.replication.ClusterReplicationTransport.StorageControllerAdapter;
import peruncs.datagrid.cluster.storage.binary.ReplicationApplier;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/// Covers the cached Aeron health view: closed views stay side-effect free,
/// writers report their own readiness, and readers follow the client lifecycle.
class AeronHealthTest {
    private static final class Fixture {
        boolean closed;
        boolean driverFailed;
        boolean capacityAvailable = true;
        boolean writerReady;
        boolean writerRole;
        ReplicationState checkpoint;
        boolean clientRunning;
        boolean clientLive;
        RuntimeException clientFailure;
        boolean watermarkFailed;
        final boolean storageReady = true;
        final AtomicInteger writerReadyCalls = new AtomicInteger();

        AeronHealth health() {
            return this.health(null);
        }

        AeronHealth health(final ReplicationApplier client) {
            final StorageControllerAdapter storage = (StorageControllerAdapter) Proxy.newProxyInstance(
                    AeronHealthTest.class.getClassLoader(),
                    new Class<?>[]{StorageControllerAdapter.class},
                    (proxy, method, args) -> {
                        if (method.getDeclaringClass() == Object.class) {
                            return switch (method.getName()) {
                                case "equals" -> proxy == args[0];
                                case "hashCode" -> System.identityHashCode(proxy);
                                case "toString" -> "testStorageController";
                                default -> throw new AssertionError("unexpected Object method " + method.getName());
                            };
                        }
                        return this.storageReady;
                    });
            final BooleanSupplier writerReadyProbe = () -> {
                this.writerReadyCalls.incrementAndGet();
                return this.writerReady;
            };
            return new AeronHealth(
                    storage, client,
                    () -> this.closed,
                    () -> this.driverFailed,
                    () -> this.capacityAvailable,
                    writerReadyProbe,
                    () -> this.writerRole,
                    () -> this.checkpoint,
                    () -> -1L, () -> -1L, () -> -1L, () -> -1L,
                    () -> this.watermarkFailed);
        }

        ReplicationApplier client() {
            return (ReplicationApplier) Proxy.newProxyInstance(
                    AeronHealthTest.class.getClassLoader(),
                    new Class<?>[]{ReplicationApplier.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "failure" -> this.clientFailure;
                        case "isRunning" -> this.clientRunning;
                        case "isLive" -> this.clientLive;
                        default -> {
                            final Class<?> result = method.getReturnType();
                            if (result == boolean.class) yield false;
                            if (result == long.class) yield 0L;
                            yield null;
                        }
                    });
        }
    }

        /// A closed view is a pure failure: no lifecycle supplier runs.
    @Test
    void closedViewNeverInvokesWriterReadiness() {
        final Fixture fixture = new Fixture();
        fixture.closed = true;
        fixture.writerReady = true;
        final AeronHealth health = fixture.health(fixture.client());

        assertFalse(health.isReady());
        assertFalse(health.isHealthy());
        assertEquals(ReplicationState.FAILED, health.state());
        assertEquals(0, fixture.writerReadyCalls.get());
    }

        /// Closing deactivates the view even when every probe reports healthy.
    @Test
    void closeDeactivatesAHealthyWriter() {
        final Fixture fixture = new Fixture();
        fixture.writerRole = true;
        fixture.writerReady = true;
        final AeronHealth health = fixture.health();
        assertTrue(health.isReady());

        health.close();

        assertFalse(health.isReady());
        assertEquals(ReplicationState.FAILED, health.state());
    }

        /// A failed watermark fails readiness without consulting the writer.
    @Test
    void watermarkFailureFailsReadiness() {
        final Fixture fixture = new Fixture();
        fixture.writerRole = true;
        fixture.writerReady = true;
        fixture.watermarkFailed = true;

        assertFalse(fixture.health().isReady());
        assertEquals(ReplicationState.FAILED, fixture.health().state());
        assertEquals(0, fixture.writerReadyCalls.get());
    }

        /// A failed driver fails the view even for a ready writer.
    @Test
    void driverFailureFailsAReadyWriter() {
        final Fixture fixture = new Fixture();
        fixture.writerRole = true;
        fixture.writerReady = true;
        fixture.driverFailed = true;

        assertFalse(fixture.health().isReady());
        assertEquals(ReplicationState.FAILED, fixture.health().state());
    }

        /// A ready writer with capacity is live.
    @Test
    void readyWriterIsLive() {
        final Fixture fixture = new Fixture();
        fixture.writerRole = true;
        fixture.writerReady = true;
        final AeronHealth health = fixture.health();

        assertTrue(health.isReady());
        assertTrue(health.isHealthy());
        assertEquals(ReplicationState.LIVE, health.state());
        assertEquals(3, fixture.writerReadyCalls.get(), "one snapshot per evaluation");
    }

        /// A writer still publishing its first checkpoint is starting, not failed.
    @Test
    void unreadyWriterIsStarting() {
        final Fixture fixture = new Fixture();
        fixture.writerRole = true;
        fixture.writerReady = false;
        final AeronHealth health = fixture.health();

        assertFalse(health.isReady());
        assertEquals(ReplicationState.STARTING, health.state());
    }

        /// A writer without Archive capacity stays scrutable as degraded.
    @Test
    void writerWithoutCapacityIsDegraded() {
        final Fixture fixture = new Fixture();
        fixture.writerRole = true;
        fixture.writerReady = true;
        fixture.capacityAvailable = false;

        assertEquals(ReplicationState.DEGRADED, fixture.health().state());
    }

        /// A terminal writer checkpoint wins over a ready publication.
    @Test
    void writerCheckpointReseedWins() {
        final Fixture fixture = new Fixture();
        fixture.writerRole = true;
        fixture.writerReady = true;
        fixture.checkpoint = ReplicationState.RESEED_REQUIRED;

        assertEquals(ReplicationState.RESEED_REQUIRED, fixture.health().state());
    }

        /// A reader without a subscription yet is starting, not failed.
    @Test
    void readerWithoutClientIsStarting() {
        final Fixture fixture = new Fixture();
        final AeronHealth health = fixture.health(null);

        assertFalse(health.isReady());
        assertEquals(ReplicationState.STARTING, health.state());
    }

        /// A live reader client is ready.
    @Test
    void liveReaderIsReady() {
        final Fixture fixture = new Fixture();
        fixture.clientRunning = true;
        fixture.clientLive = true;
        final AeronHealth health = fixture.health(fixture.client());

        assertTrue(health.isReady());
        assertTrue(health.isHealthy());
        assertEquals(ReplicationState.LIVE, health.state());
    }

        /// A running reader that has not caught up is replaying but still healthy.
    @Test
    void catchingUpReaderIsReplaying() {
        final Fixture fixture = new Fixture();
        fixture.clientRunning = true;
        fixture.clientLive = false;
        final AeronHealth health = fixture.health(fixture.client());

        assertFalse(health.isReady(), "readiness requires the live boundary");
        assertTrue(health.isHealthy());
        assertEquals(ReplicationState.REPLAYING, health.state());
    }

        /// A reader whose bounded reconnect budget expired reports RESEED_REQUIRED.
    @Test
    void reseedReaderClientRequiresReseed() {
        final Fixture fixture = new Fixture();
        fixture.clientRunning = false;
        fixture.clientFailure = new ReseedRequiredException("archive response channel lost");
        final AeronHealth health = fixture.health(fixture.client());

        assertFalse(health.isReady());
        assertFalse(health.isHealthy());
        assertEquals(ReplicationState.RESEED_REQUIRED, health.state());
    }

        /// A failed reader client fails the view.
    @Test
    void failedReaderClientFails() {
        final Fixture fixture = new Fixture();
        fixture.clientRunning = true;
        fixture.clientLive = true;
        fixture.clientFailure = new IllegalStateException("replay failed");
        final AeronHealth health = fixture.health(fixture.client());

        assertFalse(health.isReady());
        assertFalse(health.isHealthy());
        assertEquals(ReplicationState.FAILED, health.state());
    }
}
