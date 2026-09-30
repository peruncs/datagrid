package peruncs.cluster.node.store;

import org.eclipse.store.storage.types.StorageController;
import peruncs.cluster.api.ReplicationState;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.node.replication.ReplicationHealth;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.eclipse.serializer.util.X.notNull;

/// Reports whether the Store and replication can serve requests.
public final class StorageNodeHealthCheck implements AutoCloseable {
    /// Creates a health check that also observes node maintenance health.
    ///
    /// @param storageController Store controller
    /// @param replicationHealth replication health
    /// @param maintenanceHealthy maintenance-health predicate
    /// @return health check
    public static StorageNodeHealthCheck create(
            final StorageController storageController,
            final ReplicationHealth replicationHealth,
            final BooleanSupplier maintenanceHealthy
    ) {
        return new StorageNodeHealthCheck(
                notNull(storageController), notNull(replicationHealth), notNull(maintenanceHealthy));
    }

    private final StorageController storageController;
    private final ReplicationHealth replicationHealth;
    private final BooleanSupplier maintenanceHealthy;
    private final AtomicBoolean active = new AtomicBoolean(true);

    private StorageNodeHealthCheck(
            final StorageController storageController,
            final ReplicationHealth replicationHealth,
            final BooleanSupplier maintenanceHealthy
    ) {
        this.storageController = storageController;
        this.replicationHealth = replicationHealth;
        this.maintenanceHealthy = maintenanceHealthy;
    }

    /// Reports whether Store and replication are ready.
    ///
    /// @return `true` when ready
    /// @throws NodeException if readiness cannot be checked
    public boolean isReady() throws NodeException {
        return this.available() && this.replicationHealth.isReady();
    }

    /// Reports whether Store and replication are healthy.
    ///
    /// @return `true` when healthy
    public boolean isHealthy() {
        return this.available() && this.replicationHealth.isHealthy();
    }

    /// Returns provider state used by status reporting.
    ///
    /// @return current replication state
    public ReplicationState replicationState() {
        return this.active.get() ? this.replicationHealth.state() : ReplicationState.FAILED;
    }

    /// Returns the provider's current Archive free-space estimate, or -1.
    ///
    /// @return usable Archive bytes, or -1 when unknown
    public long archiveUsableSpaceBytes() {
        return this.replicationHealth.archiveUsableSpaceBytes();
    }

    /// Returns the writer's last durable recording position, or -1.
    ///
    /// @return durable recording position, or -1 when unknown
    public long writerDurablePosition() {
        return this.replicationHealth.writerDurablePosition();
    }

    /// Returns the writer's last durable sequence, or -1.
    ///
    /// @return durable sequence, or -1 when unknown
    public long writerDurableSequence() {
        return this.replicationHealth.writerDurableSequence();
    }

    /// Returns the reader's last applied sequence, or -1.
    ///
    /// @return applied sequence, or -1 when unknown
    public long appliedSequence() {
        return this.replicationHealth.appliedSequence();
    }

    /// Combines Store readiness with provider health and lifecycle state.
    private boolean available() {
        return this.active.get() && this.maintenanceHealthy.getAsBoolean() &&
                this.storageController.isRunning() && !this.storageController.isStartingUp();
    }

    /// Stops the health view and closes its owned replication health.
    @Override
    public void close() {
        if (this.active.compareAndSet(true, false)) {
            this.replicationHealth.close();
        }
    }
}
