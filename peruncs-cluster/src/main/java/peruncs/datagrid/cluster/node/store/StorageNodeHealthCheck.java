package peruncs.datagrid.cluster.node.store;

import org.eclipse.store.storage.types.StorageController;
import peruncs.datagrid.cluster.node.exceptions.NodeLibraryException;
import peruncs.datagrid.cluster.node.replication.ReplicationHealth;

import static org.eclipse.serializer.util.X.notNull;

/// Neutral storage + replication readiness gate.
public interface StorageNodeHealthCheck extends AutoCloseable {
        /// Creates a health check.
    ///
    /// @param storageController Store controller
    /// @param replicationHealth replication health
    /// @return health check
    static StorageNodeHealthCheck New(
            final StorageController storageController,
            final ReplicationHealth replicationHealth
    ) {
        return new Default(notNull(storageController), notNull(replicationHealth));
    }

        /// Reports whether Store and replication are ready.
    ///
    /// @return `true` when ready
    /// @throws NodeLibraryException if readiness cannot be checked
    boolean isReady() throws NodeLibraryException;

        /// Reports whether Store and replication are healthy.
    ///
    /// @return `true` when healthy
    boolean isHealthy();

        /// Returns the provider state used by monitoring and readiness diagnostics.
    ///
    /// @return provider state
    default ReplicationHealth.State replicationState() {
        return isHealthy() ? ReplicationHealth.State.LIVE : ReplicationHealth.State.STARTING;
    }

        /// Returns the provider's current Archive free-space estimate, or `-1`.
    ///
    /// @return free bytes
    default long archiveUsableSpaceBytes() {
        return -1L;
    }

        /// Returns the writer's last durable recording position, or `-1`.
    ///
    /// @return durable position
    default long writerDurablePosition() {
        return -1L;
    }

        /// Returns the writer's last durable sequence, or `-1`.
    ///
    /// @return durable sequence
    default long writerDurableSequence() {
        return -1L;
    }

        /// Returns the reader's last applied sequence, or `-1`.
    ///
    /// @return applied sequence
    default long appliedSequence() {
        return -1L;
    }

    @Override
    void close();

        /// Combines Store readiness with provider health and lifecycle state.
    final class Default implements StorageNodeHealthCheck {
        private final StorageController storageController;
        private final ReplicationHealth replicationHealth;
        private volatile boolean active = true;

        private Default(
                final StorageController storageController,
                final ReplicationHealth replicationHealth
        ) {
            this.storageController = storageController;
            this.replicationHealth = replicationHealth;
        }

        @Override
        public boolean isHealthy() {
            return this.active && this.storageReady() && this.replicationHealth.isHealthy();
        }

        @Override
        public ReplicationHealth.State replicationState() {
            return this.active ? this.replicationHealth.state() : ReplicationHealth.State.FAILED;
        }

        @Override
        public long archiveUsableSpaceBytes() {
            return this.replicationHealth.archiveUsableSpaceBytes();
        }

        @Override
        public long writerDurablePosition() {
            return this.replicationHealth.writerDurablePosition();
        }

        @Override
        public long writerDurableSequence() {
            return this.replicationHealth.writerDurableSequence();
        }

        @Override
        public long appliedSequence() {
            return this.replicationHealth.appliedSequence();
        }

        @Override
        public boolean isReady() throws NodeLibraryException {
            return this.active && this.storageReady() && this.replicationHealth.isReady();
        }

        private boolean storageReady() {
            return this.storageController.isRunning() && !this.storageController.isStartingUp();
        }

        @Override
        public synchronized void close() {
            if (!this.active) {
                return;
            }
            this.active = false;
            this.replicationHealth.close();
        }
    }
}
