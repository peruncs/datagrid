package peruncs.cluster.storage.index;

import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataAcceptor;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataIterator;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/// Compares the checked writer pre-filter with Serializer's raw type-id walk over one real commit.
@State(Scope.Thread)
public class EntityHeadersBenchmark {
    private static final int ENTITY_BYTES = 1024;

    @Param({"65536", "131072", "262144", "524288", "655360", "786432", "917504", "1048576"})
    public int payloadBytes;

    private Path storePath;
    private EmbeddedStorageManager storage;
    private ByteBuffer data;
    private Binary binary;
    private Binary nonNativeOrderBinary;
    private long count;
    private final EntityHeaders.EntityVisitor visitor = (typeId, objectId) -> {
        this.count = checksum(this.count, typeId);
        this.count = checksum(this.count, objectId);
    };
    private final BinaryEntityRawDataAcceptor acceptor = (address, _) -> {
        this.count = checksum(this.count, XMemory.get_long(address + Long.BYTES));
        this.count = checksum(this.count, XMemory.get_long(address + 2L * Long.BYTES));
        return true;
    };
    private final BinaryEntityRawDataIterator iterator = BinaryEntityRawDataIterator.New();

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
        this.storage = foundation.start(new BenchmarkRoot(entities));
        this.storage.storeRoot();
        final byte[] bytes = serialized.toByteArray();
        this.data = ByteBuffer.allocateDirect(bytes.length).order(ByteOrder.nativeOrder());
        this.data.put(bytes).flip();
        final ByteBuffer source = this.data.duplicate().order(ByteOrder.nativeOrder());
        source.position(source.limit());
        this.binary = ChunksWrapper.New(source);
        final ByteBuffer nonNativeOrder = this.data.duplicate().order(ByteOrder.BIG_ENDIAN);
        nonNativeOrder.position(nonNativeOrder.limit());
        this.nonNativeOrderBinary = ChunksWrapper.New(nonNativeOrder);
        if (this.data.remaining() < this.payloadBytes) {
            throw new IllegalStateException("Serializer commit is smaller than the requested benchmark payload");
        }
    }

    @Benchmark
    public long boundsCheckedProductionBinaryHeaderScan() {
        this.count = 0L;
        EntityHeaders.forEach(this.binary, this.visitor);
        return this.count;
    }

    @Benchmark
    public long boundsCheckedProductionBinaryHeaderScanNonNativeOrder() {
        this.count = 0L;
        EntityHeaders.forEach(this.nonNativeOrderBinary, this.visitor);
        return this.count;
    }

    @Benchmark
    public long serializerRawHeaderScan() {
        this.count = 0L;
        final long start = XMemory.getDirectByteBufferAddress(this.data);
        if (this.iterator.iterateEntityRawData(start, start + this.data.limit(), this.acceptor) != 0L) {
            throw new IllegalStateException("Serializer left an incomplete trailing item");
        }
        return this.count;
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

    private record BenchmarkRoot(byte[][] entities) {
    }
}
