package peruncs.datagrid.cluster.node;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cluster.node.replication.ReplicationMetrics;

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
/// name, requirements, and exports still match the source tree.
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

        /// Public method signatures expose these upstream contracts, so their
    /// modules must be readable by consumers without repeating every dependency.
    @Test
    void publicUpstreamContractsAreTransitive() {
        /* Keep this list synchronized with public exported signatures. A new
         * upstream type in an exported method must add its module here, or the
         * consumer-facing JPMS contract is no longer checked. */
        final Set<ModuleDescriptor.Requires> required = descriptor().requires();
        final Set<String> transitive = required.stream()
                .filter(requirement -> requirement.modifiers().contains(ModuleDescriptor.Requires.Modifier.TRANSITIVE))
                .map(ModuleDescriptor.Requires::name)
                .collect(Collectors.toSet());

        assertTrue(transitive.containsAll(Set.of(
                        "org.eclipse.store.storage.embedded",
                        "org.eclipse.serializer.persistence",
                        "org.eclipse.serializer.persistence.binary",
                        "org.eclipse.store.storage",
                        "org.eclipse.store.gigamap",
                        "org.eclipse.store.gigamap.lucene",
                        "org.eclipes.store.gigamap.jvector")),
                () -> "public upstream contracts require transitive modules: " + transitive);
    }

        /// No public type from a non-exported package of this module may
    /// appear in the signature of an exported API member.
    ///
    /// Consumers on the module path can only read exported packages; a public
    /// helper type (for example a shared exception) referenced by exported
    /// code but living in an unexported package would be undeclarable in
    /// consumer catch clauses and unchecked exception specifications. The
    /// sweep reflects over every public class of every exported package and
    /// collects referenced types, so a regression fails with the exact
    /// member and type to relocate or export.
    @Test
    void exportedSignaturesReferenceNoPublicTypeOfUnexportedPackages() throws Exception {
        final Path classes = classesDirectory();
        final Set<String> exportedPackages = descriptor().exports().stream()
                .map(ModuleDescriptor.Exports::source)
                .collect(Collectors.toSet());

        /* Every public class of this module's non-exported packages is
         * forbidden in exported signatures; package-private types cannot
         * cross packages, so only public ones can leak. */
        final Set<String> forbiddenTypes = new java.util.HashSet<>();
        try (Stream<Path> moduleClasses = Files.walk(classes)) {
            moduleClasses.filter(path -> path.toString().endsWith(".class"))
                    .filter(path -> !path.toString().endsWith("module-info.class"))
                    .forEach(path -> {
                        final String binaryName = classes.relativize(path).toString()
                                .replace(".class", "").replace('/', '.');
                        if (binaryName.indexOf('$') >= 0) return;
                        final String packageName = binaryName.lastIndexOf('.') >= 0
                                ? binaryName.substring(0, binaryName.lastIndexOf('.')) : "";
                        if (exportedPackages.contains(packageName)) return;
                        try {
                            final Class<?> type = Class.forName(binaryName, false,
                                    ReplicationMetrics.class.getClassLoader());
                            if (java.lang.reflect.Modifier.isPublic(type.getModifiers())) {
                                forbiddenTypes.add(binaryName);
                            }
                        } catch (final ClassNotFoundException unresolved) {
                            throw new IllegalStateException("cannot load " + binaryName, unresolved);
                        }
                    });
        }

        final Set<String> violations = new java.util.TreeSet<>();
        for (final String exportedPackage : exportedPackages) {
            final Path packageDirectory = classes.resolve(exportedPackage.replace('.', '/'));
            try (Stream<Path> entries = Files.list(packageDirectory)) {
                for (final Path entry : entries.filter(file -> file.toString().endsWith(".class")).toList()) {
                    final String binaryName = classes.relativize(entry).toString()
                            .replace(".class", "").replace('/', '.');
                    final Class<?> type;
                    try {
                        type = Class.forName(binaryName, false, ReplicationMetrics.class.getClassLoader());
                    } catch (final ClassNotFoundException unresolved) {
                        throw new IllegalStateException("cannot load " + binaryName, unresolved);
                    }
                    if (!java.lang.reflect.Modifier.isPublic(type.getModifiers())) continue;
                    violations.addAll(forbiddenSignatureReferences(type, forbiddenTypes));
                }
            }
        }
        assertTrue(violations.isEmpty(),
                "exported signatures reference public types of unexported packages: " + violations);
    }

        /// Collects forbidden types referenced by one exported type's public
    /// surface: methods (including inherited public ones), constructors,
    /// fields, and public nested types.
    private static Set<String> forbiddenSignatureReferences(final Class<?> exported, final Set<String> forbidden) {
        final Set<String> found = new java.util.HashSet<>();
        for (final java.lang.reflect.Method method : exported.getMethods()) {
            if (isOurMember(method.getDeclaringClass())) {
                checkType(method.getReturnType(), forbidden, found, method);
                for (final Class<?> parameter : method.getParameterTypes()) {
                    checkType(parameter, forbidden, found, method);
                }
                for (final Class<?> exception : method.getExceptionTypes()) {
                    checkType(exception, forbidden, found, method);
                }
            }
        }
        for (final java.lang.reflect.Constructor<?> constructor : exported.getConstructors()) {
            for (final Class<?> parameter : constructor.getParameterTypes()) {
                checkType(parameter, forbidden, found, constructor);
            }
        }
        for (final java.lang.reflect.Field field : exported.getFields()) {
            if (isOurMember(field.getDeclaringClass())) {
                checkType(field.getType(), forbidden, found, field);
            }
        }
        for (final Class<?> nested : exported.getClasses()) {
            if (forbidden.contains(nested.getName())) {
                found.add(nested.getName() + " nested in " + exported.getName());
            }
        }
        return found;
    }

    private static boolean isOurMember(final Class<?> declaring) {
        return declaring.getName().startsWith("peruncs.");
    }

    private static void checkType(final Class<?> type, final Set<String> forbidden,
                                  final Set<String> found, final java.lang.reflect.Member member) {
        if (forbidden.contains(type.getName())) {
            found.add(type.getName() + " in " + member.getDeclaringClass().getName()
                    + "." + member.getName());
        }
    }
}
