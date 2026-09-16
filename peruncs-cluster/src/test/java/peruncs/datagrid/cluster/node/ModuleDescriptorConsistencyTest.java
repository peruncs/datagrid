package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;

import java.lang.module.ModuleDescriptor;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/// Guards the JPMS descriptor against drift without running tests on the
/// module path.
///
/// The suites stay on the class path (see the surefire `useModulePath`
/// comment in `peruncs-cluster/pom.xml`): forked crash fixtures, dynamic
/// test hooks, and non-modular test helpers assume an unnamed module. This
/// test instead reads the compiled `module-info.class` and checks that its
/// name, requirements, and exports still match the tree and the OSGi
/// manifest configuration in the root `pom.xml`.
class ModuleDescriptorConsistencyTest {
    private static Path classesDirectory() {
        try {
            return Paths.get(ReplicationMetrics.class.getProtectionDomain()
                    .getCodeSource().getLocation().toURI());
        } catch (final URISyntaxException failure) {
            throw new IllegalStateException("cannot locate the compiled classes directory", failure);
        }
    }

    private static ModuleDescriptor descriptor() {
        final Path descriptorClass = classesDirectory().resolve("module-info.class");
        assertTrue(Files.isRegularFile(descriptorClass),
                "module-info.class must be compiled into " + classesDirectory());
        try {
            final byte[] bytes = Files.readAllBytes(descriptorClass);
            return ModuleDescriptor.read(ByteBuffer.wrap(bytes));
        } catch (final Exception failure) {
            throw new IllegalStateException("cannot parse " + descriptorClass, failure);
        }
    }

        /// The descriptor names this module.
    @Test
    void descriptorNamesThisModule() {
        assertEquals("peruncs.datagrid.cluster", descriptor().name());
    }

        /// Every export maps to a package directory that actually holds classes.
    @Test
    void everyExportMapsToAPackageWithClasses() throws Exception {
        final Path classes = classesDirectory();
        for (final ModuleDescriptor.Exports exported : descriptor().exports()) {
            assertFalse(exported.isQualified(), "exports must stay unqualified: " + exported.source());
            final Path packageDirectory = classes.resolve(exported.source().replace('.', '/'));
            assertTrue(Files.isDirectory(packageDirectory),
                    "exported package has no directory: " + exported.source());
            try (Stream<Path> entries = Files.list(packageDirectory)) {
                assertTrue(entries.anyMatch(entry -> entry.toString().endsWith(".class")),
                        "exported package holds no classes: " + exported.source());
            }
        }
    }

        /// The `requires` set tracks the upstream gigamap-jvector spelling.
    ///
    /// The published gigamap-jvector descriptor misspells its own module name
    /// (`org.eclipes.store.gigamap.jvector`); our `requires` directive must
    /// match it byte-for-byte or module-path compilation fails. If upstream
    /// ever fixes the spelling, this test and the compile both break: adopt
    /// the fixed name in `module-info.java` then.
    @Test
    void requiresTrackTheUpstreamJvectorSpelling() {
        final Set<String> required = descriptor().requires().stream()
                .map(ModuleDescriptor.Requires::name)
                .collect(Collectors.toSet());

        assertTrue(required.contains("org.eclipes.store.gigamap.jvector"),
                "requires must match the published upstream module name: " + required);
        assertFalse(required.contains("org.eclipse.store.gigamap.jvector"),
                "upstream has not published the corrected spelling yet: " + required);
    }

        /// The OSGi `Export-Package` list covers every JPMS export.
    ///
    /// The root `pom.xml` configures the bundle manifest with an explicit
    /// export list that must match the module descriptor (no wildcards, no
    /// dynamic imports); a package exported to JPMS but missing from the
    /// manifest is invisible to OSGi consumers.
    @Test
    void osgiExportsCoverJpmsExports() throws Exception {
        final Path rootPom = classesDirectory().resolve("../../../pom.xml").normalize();
        assertTrue(Files.isRegularFile(rootPom), "root pom must sit three levels above " + classesDirectory());
        final String pom = Files.readString(rootPom);
        final int start = pom.indexOf("<Export-Package>");
        final int end = pom.indexOf("</Export-Package>");
        assertTrue(start >= 0 && end > start, "root pom must configure an explicit Export-Package list");
        final String exports = pom.substring(start, end);
        for (final ModuleDescriptor.Exports exported : descriptor().exports()) {
            assertTrue(exports.contains(exported.source()),
                    "OSGi Export-Package is missing the JPMS export " + exported.source());
        }
    }
}
