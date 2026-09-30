package peruncs.cluster.storage.index;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataAcceptor;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataIterator;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;

/// Measures the writer prefilter over one real Store commit against Serializer's raw walk.
@State(Scope.Thread)
public class EntityHeadersBenchmark {
    private static final int ENTITY_BYTES = 1024;

    @Param({"65536", "131072", "262144", "524288", "655360", "786432", "917504", "1048576"})
    public int payloadBytes;

    private Path storePath;
    private EmbeddedStorageManager storage;
    private ByteBuffer data;
    private Binary binary;
    private PersistenceTypeDictionary dictionary;
    private long replicationMarkObjectId;
    private long count;
    private long replicationMarkTypeId;
    private final BinaryEntityRawDataAcceptor typeAcceptor = (address, _) -> {
        final long typeId = XMemory.get_long(address + Long.BYTES);
        this.count = checksum(this.count, typeId);
        if (typeId == this.replicationMarkTypeId) {
            this.count = checksum(this.count, XMemory.get_long(address + 2 * Long.BYTES));
        }
        return true;
    };
    private final BinaryEntityRawDataIterator iterator = BinaryEntityRawDataIterator.New();
    private final ClusterIndexValidation.CommitPrefilterScratch scratch =
            new ClusterIndexValidation.CommitPrefilterScratch();

    @Setup(Level.Trial)
    public void setup() throws Exception {
        this.storePath = Files.createTempDirectory("entity-header-jmh-");
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(this.storePath))
                .setChannelCountProvider(Storage.ChannelCountProvider(1))
                .createConfiguration();
        final var foundation = EmbeddedStorage.Foundation(configuration);
        final var connectionFoundation = foundation.getConnectionFoundation();
        final PersistenceTarget<Binary> delegate = connectionFoundation.getPersistenceTarget();
        final ByteArrayOutputStream serialized = new ByteArrayOutputStream(this.payloadBytes + 4096);
        connectionFoundation.setPersistenceTarget(new PersistenceTarget<>() {
            @Override
            public void write(final Binary binary) {
                copyBytes(binary, serialized);
                delegate.write(binary);
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

        final byte[][] entities = new byte[(this.payloadBytes + ENTITY_BYTES - 1) / ENTITY_BYTES][];
        int remaining = this.payloadBytes;
        for (int index = 0; index < entities.length; index++) {
            final int length = Math.min(ENTITY_BYTES, remaining);
            entities[index] = new byte[length];
            remaining -= length;
        }
        final BenchmarkRoot root = new BenchmarkRoot(entities,
                new ReplicationMark(UUID.randomUUID(), UUID.randomUUID(), 1L, 1L));
        this.storage = foundation.start(root);
        this.storage.storeRoot();
        this.dictionary = this.storage.persistenceManager().typeDictionary();
        this.replicationMarkTypeId = this.dictionary.lookupTypeByName(ReplicationMark.class.getName()).typeId();
        this.replicationMarkObjectId = this.storage.persistenceManager().objectRegistry().lookupObjectId(root.mark());
        final byte[] bytes = serialized.toByteArray();
        this.data = ByteBuffer.allocateDirect(bytes.length).order(ByteOrder.nativeOrder());
        this.data.put(bytes).flip();
        final ByteBuffer source = this.data.duplicate().order(ByteOrder.nativeOrder());
        source.position(source.limit());
        this.binary = ChunksWrapper.New(source);
        if (this.data.remaining() < this.payloadBytes) {
            throw new IllegalStateException("Serializer commit is smaller than the requested benchmark payload");
        }
        final int scan = ClusterIndexValidation.inspectWriterCommit(
                this.binary, this.dictionary, this.scratch, this.replicationMarkObjectId);
        if ((scan & ClusterStoreIndexes.COMMIT_HAS_REPLICATION_MARK) == 0) {
            throw new IllegalStateException("writer prefilter did not find its replication mark");
        }
    }

    @Benchmark
    public int productionWriterCommitPrefilter() {
        return ClusterIndexValidation.inspectWriterCommit(
                this.binary, this.dictionary, this.scratch, this.replicationMarkObjectId);
    }

    @Benchmark
    public long serializerRawWriterCommitScan() {
        this.count = 0L;
        scanRawWriterCommit();
        return this.count;
    }

    private void scanRawWriterCommit() {
        final long start = XMemory.getDirectByteBufferAddress(this.data);
        if (this.iterator.iterateEntityRawData(start, start + this.data.limit(), this.typeAcceptor) != 0L) {
            throw new IllegalStateException("Serializer left an incomplete trailing item");
        }
    }

    @TearDown(Level.Trial)
    public void close() throws IOException {
        if (this.storage != null) this.storage.shutdown();
        if (this.storePath != null) {
            try (var paths = Files.walk(this.storePath)) {
                for (final Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
            }
        }
    }

    private static void copyBytes(final Binary binary, final ByteArrayOutputStream output) {
        final boolean wrapped = binary instanceof ChunksWrapper;
        binary.iterateChannelChunks(channel -> {
            for (final ByteBuffer source : channel.buffers()) {
                final int length = wrapped ? source.position() : source.limit();
                final ByteBuffer view = source.duplicate();
                view.position(0).limit(length);
                final byte[] bytes = new byte[length];
                view.get(bytes);
                output.writeBytes(bytes);
            }
        });
    }

    private static long checksum(final long current, final long typeId) {
        return Long.rotateLeft(current, 9) ^ typeId;
    }

    private record BenchmarkRoot(byte[][] entities, ReplicationMark mark) {
    }
}
