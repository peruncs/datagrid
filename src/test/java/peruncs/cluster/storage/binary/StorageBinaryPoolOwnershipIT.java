package peruncs.cluster.storage.binary;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.node.store.DistributedStorage;
import peruncs.cluster.storage.StorageGraphCoordinator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Checks 10,000 real Store updates while the reader retains at most one native buffer.
class StorageBinaryPoolOwnershipIT {
    private static final int TRANSACTIONS = 10_000;
    private static final int MAX_PAYLOAD_BYTES = 1 << 20;

    @Test
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    void checkedSingleBufferPoolPreservesRandomPayloads(@TempDir final Path root) throws Exception {
        final Path writerPath = root.resolve("writer");
        final Path readerPath = root.resolve("reader");
        try (EmbeddedStorageManager seed = foundation(writerPath).start(new PayloadRecord())) {
            seed.storeRoot();
        }
        copyDirectory(writerPath, readerPath);

        final EmbeddedStorageFoundation<?> readerFoundation = foundation(readerPath);
        try (final EmbeddedStorageManager reader = readerFoundation.start()) {
            final StorageGraphCoordinator coordinator = new StorageGraphCoordinator();
            final StorageBinaryDataMerger merger = StorageBinaryDataMerger.create(
                    new StorageBinaryDataMerger.Configuration(
                            readerFoundation.getConnectionFoundation(), reader, coordinator::write,
                            0L, 2L << 20, 2L << 20, MAX_PAYLOAD_BYTES,
                            60_000L, 30_000L, 5_000L, 4_096, coordinator));
            final ReplicationPublisher publisher = ReplicationPublisher.Caching(new ReplicationPublisher() {
                @Override
                public void distributeData(final Binary data) {
                    merger.receiveData(data);
                    merger.awaitApplied();
                }

                @Override
                public void distributeTypeDictionary(final String dictionary) {
                    merger.receiveTypeDictionary(dictionary);
                }

                @Override
                public void dispose() {
                }
            });

            try {
                final EmbeddedStorageFoundation<?> writerFoundation = foundation(writerPath);
                DistributedStorage.configureWriting(writerFoundation, publisher, new ReplicatingTargetFactory(publisher));
                try (final EmbeddedStorageManager writer = writerFoundation.start()) {
                    final PayloadRecord record = (PayloadRecord) writer.root();
                    final PayloadRecord replicated = (PayloadRecord) reader.root();
                    final Random random = new Random(0x4E31504F4F4CL);
                    for (int sequence = 0; sequence < TRANSACTIONS; sequence++) {
                        final byte[] payload = new byte[random.nextInt(MAX_PAYLOAD_BYTES) + 1];
                        random.nextBytes(payload);
                        final long checksum = crc32c(payload);
                        record.sequence = sequence;
                        record.payload = payload;
                        record.checksum = checksum;
                        writer.store(record);

                        assertEquals(sequence, replicated.sequence, "reader sequence at transaction " + sequence);
                        assertEquals(checksum, replicated.checksum, "writer CRC at transaction " + sequence);
                        assertEquals(checksum, crc32c(replicated.payload), "reader payload CRC at transaction " + sequence);
                    }
                }
            } finally {
                try {
                    merger.dispose();
                } finally {
                    publisher.dispose();
                }
            }
        }
    }

    private static EmbeddedStorageFoundation<?> foundation(final Path path) {
        return org.eclipse.store.storage.embedded.types.EmbeddedStorage.Foundation(
                StorageConfiguration.Builder()
                        .setStorageFileProvider(Storage.FileProvider(path))
                        .setChannelCountProvider(Storage.ChannelCountProvider(4))
                        .createConfiguration());
    }

    private static void copyDirectory(final Path source, final Path target) throws IOException {
        try (var paths = Files.walk(source)) {
            for (final Path path : paths.toList()) {
                final Path destination = target.resolve(source.relativize(path));
                if (Files.isDirectory(path)) Files.createDirectories(destination);
                else {
                    Files.createDirectories(destination.getParent());
                    Files.copy(path, destination);
                }
            }
        }
    }

    private static long crc32c(final byte[] payload) {
        final CRC32C crc = new CRC32C();
        crc.update(payload);
        return crc.getValue();
    }

    /// Mutable Store root updated with one random payload per replication transaction.
    public static final class PayloadRecord {
        public long sequence;
        public byte[] payload = new byte[0];
        public long checksum;

        public PayloadRecord() {
        }
    }
}
