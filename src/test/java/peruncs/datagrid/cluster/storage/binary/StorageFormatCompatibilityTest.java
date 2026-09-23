package peruncs.datagrid.cluster.storage.binary;

import org.eclipse.serializer.chars.VarString;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Guards durable Store binary compatibility across dependency upgrades.
///
/// Writes a Store with the current Eclipse Store/Serializer dependencies,
/// records its type dictionary plus the storage-file manifest, then reopens the
/// same files. A serializer or storage upgrade that changes the durable format
/// must fail loudly here — by refusing to reopen, by resolving different type
/// ids, or by materializing different data — instead of silently diverging a
/// replicated reader from its writer.
class StorageFormatCompatibilityTest {
    /// Verifies a store written with the current dependencies reopens with identical data and type dictionary.
    @Test
    void writtenStoreReopensWithIdenticalDictionaryAndData() throws Exception {
        final Path root = Files.createTempDirectory("datagrid-store-format-");
        try {
            final Fixture initial = new Fixture();
            initial.values.add("format-seed");
            initial.entries.add(new Entry("one", "first"));
            final EmbeddedStorageManager writer = foundation(root).start(initial);
            initial.values.add("format-second");
            initial.entries.add(new Entry("two", "second"));
            writer.storeRoot();
            writer.store(initial.values);
            writer.store(initial.entries);
            final byte[] dictionaryBefore = typeDictionaryBytes(writer);
            final String manifestBefore = storageManifest(root);
            assertFalse(manifestBefore.isEmpty(), "the written Store must own storage files");
            writer.shutdown();

            final EmbeddedStorageManager reopened = foundation(root).start();
            try {
                final Fixture reloaded = reopened.root();
                assertEquals(List.of("format-seed", "format-second"), reloaded.values);
                assertEquals(List.of("one", "two"),
                        reloaded.entries.stream().map(entry -> entry.id).toList());
                assertEquals(List.of("first", "second"),
                        reloaded.entries.stream().map(entry -> entry.value).toList());
                assertEquals(new String(dictionaryBefore, StandardCharsets.UTF_8),
                        new String(typeDictionaryBytes(reopened), StandardCharsets.UTF_8));
                System.out.printf("store format fixture: dictionary=%s bytes, files:%n%s%n", dictionaryBefore.length, manifestBefore);
            } finally {
                reopened.shutdown();
            }
        } finally {
            delete(root);
        }
    }

    private static byte[] typeDictionaryBytes(final EmbeddedStorageManager storage) {
        final PersistenceTypeDictionary dictionary = storage.createConnection()
                .persistenceManager()
                .typeDictionary();
        return PersistenceTypeDictionary
                .assembleTypesPerTypeId(VarString.New(), dictionary.allTypeDefinitions())
                .toString()
                .getBytes(StandardCharsets.UTF_8);
    }

    private static String storageManifest(final Path root) throws Exception {
        final TreeMap<String, Long> files = new TreeMap<>();
        try (var paths = Files.walk(root)) {
            for (final Path path : paths.toList()) {
                if (Files.isRegularFile(path)) {
                    files.put(root.relativize(path).toString(), Files.size(path));
                }
            }
        }
        final VarString manifest = VarString.New();
        files.forEach((name, size) -> manifest.add(name).add("=").add(size.toString()).add("\n"));
        return manifest.toString();
    }

    private static EmbeddedStorageFoundation<?> foundation(final Path path) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(path))
                .setChannelCountProvider(Storage.ChannelCountProvider(2))
                .createConfiguration();
        return EmbeddedStorage.Foundation(configuration);
    }

        /// Opens a Store image written by the previous supported dependency set.
    ///
    /// The same-build reopen above cannot catch a format break that ships with
    /// a dependency upgrade, because writer and reader run one build. This
    /// fixture closes that gap: a checked-in Store directory produced by the
    /// previous Eclipse Store/Serializer version (same [Fixture] graph, plus
    /// the recorded type dictionary) must reopen with identical data and an
    /// identical dictionary.
    ///
    /// The dependency set is currently a mutable snapshot that has no released
    /// predecessor, so there is no fixture image to check in yet. When the
    /// release profile pins the first real version (see the parent POM's
    /// `release` profile), generate the image with that version, commit it
    /// under `src/test/resources/store-format-fixture/` together with the
    /// dictionary file, remove the {@code Disabled} annotation, and assert the
    /// recorded dictionary bytes — an upgrade that changes the durable format
    /// then fails here instead of silently diverging a replicated reader.
    @Test
    @org.junit.jupiter.api.Disabled(
            "requires a checked-in fixture image written by the previous released dependency set; enable when the first version pin lands")
    void storeWrittenByPreviousDependencySetReopensIdentically() throws Exception {
        final Path fixtureRoot = fixtureRoot();
        if (Files.notExists(fixtureRoot)) {
            org.junit.jupiter.api.Assertions.fail("""
                    Store format fixture image is missing; generate it with the previous \
                    supported Eclipse Store/Serializer version and commit it under \
                    src/test/resources/store-format-fixture/ together with dictionary.txt""");
        }
        final EmbeddedStorageManager reopened = foundation(fixtureRoot()).start();
        try {
            final Fixture reloaded = reopened.root();
            assertEquals(List.of("fixture-seed"), reloaded.values);
            assertEquals(List.of("fixture-entry"),
                    reloaded.entries.stream().map(entry -> entry.id).toList());
            final byte[] recorded = Files.readAllBytes(
                    fixtureRoot().getParent().resolve("dictionary.txt"));
            assertEquals(new String(recorded, StandardCharsets.UTF_8),
                    new String(typeDictionaryBytes(reopened), StandardCharsets.UTF_8),
                    "the type dictionary must resolve identically across the upgrade");
        } finally {
            reopened.shutdown();
        }
    }

    private static Path fixtureRoot() {
        return Path.of("src", "test", "resources", "store-format-fixture", "store");
    }

    private static void delete(final Path path) throws Exception {        if (Files.exists(path)) {
            try (var paths = Files.walk(path)) {
                paths.sorted(Comparator.reverseOrder()).forEach(value ->
                {
                    try {
                        Files.deleteIfExists(value);
                    } catch (final Exception ignored) {
                    }
                });
            }
        }
    }

    public static final class Fixture {
        public final List<String> values = new ArrayList<>();
        public final List<Entry> entries = new ArrayList<>();
    }

    public static final class Entry {
        public String id;
        public String value;

        public Entry() {
        }

        Entry(final String id, final String value) {
            this.id = id;
            this.value = value;
        }
    }
}
