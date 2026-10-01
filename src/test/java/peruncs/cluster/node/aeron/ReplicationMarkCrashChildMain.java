package peruncs.cluster.node.aeron;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.serializer.persistence.types.Storer;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/// Forked writer used by the named-root Store commit crash matrix.
public final class ReplicationMarkCrashChildMain {
    static final int ENTITY_COUNT = 512;
    static final int ENTITY_BYTES = 8 * 1024;

    private ReplicationMarkCrashChildMain() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 4)
            throw new IllegalArgumentException("store, control, cluster, and generation are required");
        final Path storePath = Path.of(arguments[0]);
        final Path controlPath = Path.of(arguments[1]);
        final UUID clusterId = UUID.fromString(arguments[2]);
        final UUID generation = UUID.fromString(arguments[3]);
        final Path enteredPath = controlPath.resolve("commit-entered");
        final Path donePath = controlPath.resolve("commit-done");
        final ReplicationMark mark = new ReplicationMark(clusterId, generation, 1L, 17L);
        final AtomicBoolean commitEntered = new AtomicBoolean();
        final EmbeddedStorageFoundation<?> foundation = foundation(storePath, mark);
        final var connectionFoundation = foundation.getConnectionFoundation();
        final PersistenceTarget<Binary> delegate = connectionFoundation.getPersistenceTarget();
        connectionFoundation.setPersistenceTarget(new PersistenceTarget<>() {
            @Override
            public void write(final Binary data) {
                if (commitEntered.compareAndSet(false, true)) {
                    try {
                        Files.createFile(enteredPath);
                    } catch (final IOException failure) {
                        throw new IllegalStateException("cannot publish the commit barrier", failure);
                    }
                }
                delegate.write(data);
            }

            @Override
            public boolean isWritable() {
                return delegate.isWritable();
            }

            @Override
            public void prepareTarget() {
                delegate.prepareTarget();
            }

            @Override
            public void closeTarget() {
                delegate.closeTarget();
            }
        });

        final EmbeddedStorageManager storage = foundation.start();
        try {
            final CrashRoot root = (CrashRoot) storage.root();
            final long sequence = mark.sequence() + 1L;
            final CrashBatch batch = batch(sequence);
            root.latest = batch;
            mark.reserve(mark.recordingId(), 1L, sequence, 8_192L);
            final Storer storer = storage.createStorer();
            storer.store(root);
            storer.store(batch);
            storer.store(mark);
            storer.commit();
            Files.createFile(donePath);
        } finally {
            storage.shutdown();
        }
    }

    static EmbeddedStorageFoundation<?> foundation(final Path path, final ReplicationMark mark) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(path))
                .setChannelCountProvider(Storage.ChannelCountProvider(4))
                .createConfiguration();
        final EmbeddedStorageFoundation<?> foundation = EmbeddedStorage.Foundation(configuration);
        foundation.getConnectionFoundation().getRootResolverProvider()
                .registerRoot(ReplicationMark.ROOT_ID, mark);
        return foundation;
    }

    static CrashBatch batch(final long sequence) {
        final CrashEntity[] entities = new CrashEntity[ENTITY_COUNT];
        for (int i = 0; i < entities.length; i++) {
            final byte[] payload = new byte[ENTITY_BYTES];
            Arrays.fill(payload, (byte) (sequence + i));
            entities[i] = new CrashEntity(sequence, i, payload);
        }
        return new CrashBatch(sequence, entities);
    }

    /// Default root updated atomically with the mark.
    public static final class CrashRoot {
        /// Latest committed four-megabyte batch.
        public CrashBatch latest;
    }

    /// One application transaction carried by the commit.
    public static final class CrashBatch {
        /// Transaction sequence.
        public long sequence;
        /// Payload entities for this transaction.
        public CrashEntity[] entities;

        public CrashBatch() {
        }

        CrashBatch(final long sequence, final CrashEntity[] entities) {
            this.sequence = sequence;
            this.entities = entities;
        }
    }

    /// Payload whose bytes must be wholly present or absent after recovery.
    public static final class CrashEntity {
        /// Owning sequence.
        public long sequence;
        /// Entity index within the sequence.
        public int index;
        /// Fixed-size payload bytes.
        public byte[] payload;

        public CrashEntity() {
        }

        CrashEntity(final long sequence, final int index, final byte[] payload) {
            this.sequence = sequence;
            this.index = index;
            this.payload = payload;
        }
    }
}
