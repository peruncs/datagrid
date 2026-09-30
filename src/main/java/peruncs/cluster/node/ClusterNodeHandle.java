package peruncs.cluster.node;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import peruncs.cluster.api.*;
import peruncs.cluster.errors.WrongRoleException;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/// Implements the public node handle behind the [ClusterStorage] entry point.
final class ClusterNodeHandle<T> implements ClusterNode<T> {
    private final NodeLifecycle assembly;
    private final ClusterStorageManager<T> storage;
    private final NodeRole role;

    private ClusterNodeHandle(
            final NodeLifecycle assembly,
            final ClusterStorageManager<T> storage,
            final NodeRole role) {
        this.assembly = assembly;
        this.storage = storage;
        this.role = role;
    }

    @SuppressWarnings("unchecked")
    static <T> ClusterNode<T> start(
            final Supplier<? extends T> rootSupplier,
            final EmbeddedStorageFoundation<?> foundation,
            final NodeConfig config) {
        Objects.requireNonNull(rootSupplier, "rootSupplier");
        final NodeLifecycle.Builder builder = NodeLifecycle.create()
                .setRootSupplier(rootSupplier::get);
        if (foundation != null) builder.setEmbeddedStorageFoundation(foundation);
        if (config != null) builder.setNodeConfig(config);
        final NodeLifecycle assembly = builder.build();
        try {
            /* The supplier fixes the intended root type, while the assembly stores
             * Supplier<Object> because its construction is role-generic. Existing
             * on-disk data must also satisfy the application's schema contract. */
            final ClusterStorageManager<T> storage =
                    (ClusterStorageManager<T>) assembly.startStorageManager();
            return new ClusterNodeHandle<>(assembly, storage, assembly.nodeRole());
        } catch (final RuntimeException | Error failure) {
            try {
                assembly.close();
            } catch (final Throwable closeFailure) {
                if (closeFailure != failure) failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    @Override
    public ClusterStorageManager<T> storageManager() {
        return this.storage;
    }

    @Override
    public void startStorageChecks() {
        this.control().startStorageChecks();
    }

    @Override
    public CompletableFuture<BackupInfo> createBackup(final BackupSlot slot) {
        Objects.requireNonNull(slot, "slot");
        if (this.role != NodeRole.BACKUP_READER) {
            throw new WrongRoleException("createBackup is only available on a backup-reader");
        }
        return this.assembly.backupNodeManager().createStorageBackup(slot);
    }

    @Override
    public NodeStatus status() {
        final StorageNodeControl control = this.control();
        return new NodeStatus(this.role, control.isReady(), control.isHealthy(),
                control.isRunningStorageChecks(), control.readStorageSizeBytes(),
                control.replicationStatus(), this.role == NodeRole.BACKUP_READER
                        ? this.assembly.backupNodeManager().backupStatus() : BackupStatus.empty());
    }

    private StorageNodeControl control() {
        return switch (this.role) {
            case STANDALONE, WRITER, READER -> this.assembly.storageNodeManager();
            case BACKUP_READER -> this.assembly.backupNodeManager().storage();
        };
    }

    @Override
    public void close() {
        this.assembly.close();
    }
}
