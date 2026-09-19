package peruncs.datagrid.cluster.storage.types;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/// Shared path-safety checks for every file the library resolves on behalf of
/// an operator or an archive.
///
/// Symbolic links are rejected component by component so a link inserted
/// anywhere in a path cannot redirect a read, write, or delete outside the
/// validated directory. The only exception is the macOS `/private` system
/// aliases for `/tmp`, `/var`, and `/etc`, which require root privileges to
/// create and are otherwise ubiquitous.
public final class PathSecurity {
    private PathSecurity() {
    }

    /// Rejects paths containing user-controlled symbolic links.
    ///
    /// @param path path to check, or `null` for no check
    /// @throws IOException if a link is found or a component cannot be inspected
    public static void ensureNoSymbolicLinks(final Path path) throws IOException {
        if (path == null) return;
        final Path absolute = path.toAbsolutePath().normalize();
        Path current = absolute.getRoot();
        for (final Path component : absolute) {
            current = current == null ? component : current.resolve(component);
            if (Files.isSymbolicLink(current) && !isSystemPrivateAlias(current)) {
                throw new IOException("Path contains a symbolic link: %s".formatted(current));
            }
        }
    }

    /// Reports the macOS system symlinks (`/tmp`, `/var`, `/etc`) that point
    /// into `/private`. Only a root-level link whose target is the matching
    /// self-named `/private` entry qualifies; creating such a link requires
    /// privileges outside the threat model, and everything else still fails
    /// closed. The allowlist is gated on macOS: on other operating systems a
    /// `private/<name>` target is not a system alias and must be rejected.
    ///
    /// @param path link to inspect
    /// @return `true` for a macOS system `/private` alias
    public static boolean isSystemPrivateAlias(final Path path) {
        if (!isMacOs()) return false;
        final Path root = path.getRoot();
        if (root == null || !root.equals(path.getParent())) return false;
        final Path name = path.getFileName();
        if (name == null) return false;
        try {
            final Path target = Files.readSymbolicLink(path);
            return target.equals(Path.of("private").resolve(name)) ||
                   target.equals(Path.of("/private").resolve(name));
        } catch (final IOException failure) {
            return false;
        }
    }

        /// Reports whether the current operating system is macOS.
    ///
    /// @return `true` on macOS
    public static boolean isMacOs() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("mac");
    }
}
