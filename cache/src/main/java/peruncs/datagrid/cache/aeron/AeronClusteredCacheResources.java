package peruncs.datagrid.cache.aeron;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/// Owns the Aeron client, optional embedded driver, publication, and
/// subscription shared by one clustered-cache provider.
///
/// All access is synchronized. The client and driver connect lazily on first
/// use, so a provider can be constructed from configuration but only pays for
/// Aeron when a cache sender or receiver is actually requested. When an
/// embedded driver has no configured directory it creates its own private,
/// auto-generated directory, and the Aeron client follows
/// [MediaDriver#aeronDirectoryName()]; when a directory is configured it
/// must be exclusive to this provider, because sharing a driver directory
/// corrupts the driver.
///
/// The resources are single-lifecycle: after both the publication and the
/// subscription are closed, the shared client is released and the resources
/// cannot be used again. Create a new provider for a new lifecycle.
final class AeronClusteredCacheResources implements AutoCloseable {
    private static final System.Logger LOGGER =
            System.getLogger(AeronClusteredCacheResources.class.getName());

    private final String aeronDirectory;
    private final String channel;
    private final int streamId;
    private final long driverTimeoutMillis;
    private final boolean embeddedDriver;
    private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
    private String resolvedAeronDirectory;
    private boolean generatedAeronDirectory;
    private Aeron aeron;
    private MediaDriver mediaDriver;
    private ConcurrentPublication publication;
    private Subscription subscription;
    private boolean closed;

        /// Creates the resource owner with no connection yet.
    ///
    /// @param aeronDirectory      Aeron driver directory, or `null` to use Aeron's directory
    /// @param channel             Aeron channel shared by all participants
    /// @param streamId            Aeron stream id shared by all participants
    /// @param driverTimeoutMillis maximum time the Aeron client waits for a driver
    /// @param embeddedDriver      whether to launch a private embedded MediaDriver for this provider
    AeronClusteredCacheResources(
            final String aeronDirectory,
            final String channel,
            final int streamId,
            final long driverTimeoutMillis,
            final boolean embeddedDriver
    ) {
        this.aeronDirectory = aeronDirectory;
        this.channel = channel;
        this.streamId = streamId;
        this.driverTimeoutMillis = driverTimeoutMillis;
        this.embeddedDriver = embeddedDriver;
    }

        /// Aggregates a close failure with an earlier one.
    private static Throwable append(final Throwable current, final Throwable additional) {
        if (current == null) {
            return additional;
        }
        if (current != additional) {
            current.addSuppressed(additional);
        }
        return current;
    }

        /// Converts an aggregated close failure into a throwable runtime failure.
    private static RuntimeException rethrowAsRuntime(final Throwable failure) {
        if (failure instanceof final Error error) {
            throw error;
        }
        if (failure instanceof final RuntimeException runtime) {
            return runtime;
        }
        return new IllegalStateException("Aeron resource close failed", failure);
    }

        /// Returns whether this owner was created with the given configuration.
    boolean matches(final String channel, final int streamId, final String directory,
                    final long driverTimeoutMillis, final boolean embeddedDriver) {
        return this.channel.equals(channel) && this.streamId == streamId
               && Objects.equals(this.aeronDirectory, directory)
               && this.driverTimeoutMillis == driverTimeoutMillis && this.embeddedDriver == embeddedDriver;
    }

        /// Returns the bound configuration for diagnostics.
    String describe() {
        return "channel=%s, streamId=%s, directory=%s, driverTimeoutMillis=%s, embeddedDriver=%s".formatted(this.channel, this.streamId, this.aeronDirectory, this.driverTimeoutMillis, this.embeddedDriver);
    }

        /// Returns the shared publication, connecting Aeron if needed.
    ///
    /// @return the shared publication
    synchronized ConcurrentPublication publication() {
        this.ensureConnected();
        if (this.publication == null) {
            this.publication = this.aeron.addPublication(this.channel, this.streamId);
        }
        return this.publication;
    }

        /// Returns the shared subscription, connecting Aeron if needed.
    ///
    /// @return the shared subscription
    synchronized Subscription subscription() {
        this.ensureConnected();
        if (this.subscription == null) {
            this.subscription = this.aeron.addSubscription(this.channel, this.streamId);
        }
        return this.subscription;
    }

        /// Closes the publication; releases Aeron when no other resource remains open.
    synchronized void closePublication() {
        final ConcurrentPublication current = this.publication;
        if (current != null) {
            current.close();
            this.publication = null;
        }
        this.closeIfUnused();
    }

        /// Closes the subscription; releases Aeron when no other resource remains open.
    synchronized void closeSubscription() {
        final Subscription current = this.subscription;
        if (current != null) {
            current.close();
            this.subscription = null;
        }
        this.closeIfUnused();
    }

    private void ensureConnected() {
        if (this.closed) {
            throw new IllegalStateException("Aeron clustered-cache resources are closed");
        }
        if (this.aeron != null) {
            return;
        }
        try {
            if (this.embeddedDriver && this.mediaDriver == null) {
                this.ensureDirectory();
                final MediaDriver.Context context = new MediaDriver.Context()
                        .dirDeleteOnStart(false)
                        /* The directory may be configured outside the application and can
                         * contain unrelated state. Never delete it as a close side effect. */
                        .dirDeleteOnShutdown(false)
                        .threadingMode(ThreadingMode.SHARED)
                        .errorHandler(this::recordFailure);
                if (this.aeronDirectory != null) {
                    context.aeronDirectoryName(this.aeronDirectory);
                }
                this.mediaDriver = MediaDriver.launchEmbedded(context);
                this.resolvedAeronDirectory = this.mediaDriver.aeronDirectoryName();
                this.generatedAeronDirectory = this.aeronDirectory == null;
                this.hardenDirectory(this.resolvedAeronDirectory);
            }
            final Aeron.Context context = new Aeron.Context()
                    .driverTimeoutMs(this.driverTimeoutMillis)
                    .errorHandler(this::recordFailure)
                    .subscriberErrorHandler(this::recordFailure);
            final String directory =
                    this.resolvedAeronDirectory != null ? this.resolvedAeronDirectory : this.aeronDirectory;
            if (directory != null) {
                context.aeronDirectoryName(directory);
            }
            this.aeron = Aeron.connect(context);
        } catch (final RuntimeException | Error failure) {
            /* this.aeron is still null here: the only assignment above either
             * succeeded (no catch) or threw before publishing the client. Only
             * an embedded driver started earlier in this method can need cleanup. */
            if (this.mediaDriver != null) {
                try {
                    this.mediaDriver.close();
                    this.mediaDriver = null;
                    this.deleteGeneratedDirectory();
                    this.resolvedAeronDirectory = null;
                } catch (final Throwable closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (failure instanceof final Error error) {
                throw error;
            }
            throw (RuntimeException) failure;
        }
    }

        /// Returns the first terminal driver/client failure, if one was reported.
    synchronized RuntimeException failure() {
        return this.failure.get();
    }

        /// Returns whether this owner has completed its terminal close.
    synchronized boolean isClosed() {
        return this.closed;
    }

        /// Retains the first transport failure so senders and receivers can fail closed.
    private void recordFailure(final Throwable failure) {
        final RuntimeException normalized = failure instanceof RuntimeException runtime
                ? runtime : new IllegalStateException("Aeron clustered-cache transport failed", failure);
        if (this.failure.compareAndSet(null, normalized)) {
            LOGGER.log(System.Logger.Level.ERROR, "Aeron clustered-cache transport error", failure);
        }
    }

    private void closeIfUnused() {
        if (this.publication == null && this.subscription == null) {
            this.close();
        }
    }

        /// Releases the Aeron client and any embedded driver, aggregating close
    /// failures. Idempotent; the resources are terminal after this call.
    @Override
    public synchronized void close() {
        if (this.closed) {
            return;
        }
        /* Never close the client or embedded driver underneath an owner that still
         * holds a publication/subscription. Provider shutdown disposes those handles
         * first; rejecting an out-of-order close turns accidental use-after-close
         * into a retryable lifecycle error instead of corrupting an active poller. */
        if (this.publication != null || this.subscription != null) {
            throw new IllegalStateException(
                    "cannot close Aeron clustered-cache resources while publication or subscription is open");
        }
        Throwable failure = null;
        if (this.aeron != null) {
            try {
                this.aeron.close();
                this.aeron = null;
            } catch (final Throwable closeFailure) {
                failure = closeFailure;
            }
        }
        if (this.mediaDriver != null) {
            try {
                this.mediaDriver.close();
                this.mediaDriver = null;
                this.deleteGeneratedDirectory();
                this.resolvedAeronDirectory = null;
            } catch (final Throwable closeFailure) {
                failure = append(failure, closeFailure);
            }
        }
        if (failure != null) {
            throw rethrowAsRuntime(failure);
        }
        this.closed = true;
    }

        /// Creates a private driver directory with owner-only permissions.
    private void ensureDirectory() {
        if (this.aeronDirectory == null) {
            return;
        }
        final Path path = Paths.get(this.aeronDirectory);
        try {
            if (Files.isSymbolicLink(path) || Files.exists(path, java.nio.file.LinkOption.NOFOLLOW_LINKS) &&
                                              !Files.isDirectory(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Aeron driver path is not a real directory: %s".formatted(path));
            }
            try {
                Files.createDirectories(path,
                        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
            } catch (final UnsupportedOperationException ignored) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "Aeron clustered-cache driver directory does not support POSIX permissions: %s".formatted(path));
                Files.createDirectories(path);
            }
            try {
                Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"));
            } catch (final UnsupportedOperationException ignored) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "Aeron clustered-cache driver directory permissions cannot be hardened on this filesystem: %s".formatted(path));
            }
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot create or protect Aeron driver directory %s".formatted(path), failure);
        }
    }

        /// Deletes only the private directory generated by this resource owner.
    private void deleteGeneratedDirectory() {
        if (!this.generatedAeronDirectory || this.resolvedAeronDirectory == null) return;
        final Path generated = Paths.get(this.resolvedAeronDirectory);
        try {
            if (Files.exists(generated, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                try (final var paths = Files.walk(generated)) {
                    final List<Path> ordered = paths.sorted(Comparator.reverseOrder()).toList();
                    for (final Path path : ordered) Files.deleteIfExists(path);
                }
            }
            this.generatedAeronDirectory = false;
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot delete generated Aeron directory %s".formatted(generated), failure);
        }
    }

        /// Restricts a driver directory to the owner, including a generated one.
    private void hardenDirectory(final String directory) {
        try {
            Files.setPosixFilePermissions(Paths.get(directory), PosixFilePermissions.fromString("rwx------"));
        } catch (final UnsupportedOperationException ignored) {
            LOGGER.log(System.Logger.Level.WARNING,
                    "Aeron clustered-cache generated driver directory permissions cannot be hardened on this filesystem: %s".formatted(directory));
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot protect Aeron driver directory %s".formatted(directory), failure);
        }
    }

}
