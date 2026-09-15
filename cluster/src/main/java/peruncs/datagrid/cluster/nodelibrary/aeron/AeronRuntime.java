package peruncs.datagrid.cluster.nodelibrary.aeron;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.exceptions.ActiveDriverException;
import org.agrona.ErrorHandler;
import peruncs.datagrid.cluster.storage.types.ReplicationRetry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

/// Owns one node's MediaDriver, Aeron client, and Archive client lifecycle.
final class AeronRuntime implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(AeronRuntime.class.getName());
    private static final long STALE_DRIVER_RETRY_DELAY_MILLIS = 100L;

    private final AeronSettings settings;
    private final ErrorHandler errorHandler;
    private AutoCloseable driver;
    private Aeron aeron;
    private AeronArchive archive;

    private AeronRuntime(final AeronSettings settings, final ErrorHandler errorHandler) {
        this.settings = settings;
        this.errorHandler = errorHandler;
    }

    static AeronRuntime start(final AeronSettings settings, final ErrorHandler errorHandler,
                              final Runnable beforeDriverLaunch) {
        final AeronRuntime runtime = new AeronRuntime(settings, errorHandler);
        try {
            runtime.start(beforeDriverLaunch);
            return runtime;
        } catch (final RuntimeException | Error failure) {
            final Throwable closeFailure = runtime.closeAllQuietly();
            if (closeFailure != null) failure.addSuppressed(closeFailure);
            throw failure;
        }
    }

    private static Throwable append(final Throwable current, final Throwable additional,
                                    final String resource) {
        if (additional == null) return current;
        final Throwable normalized = additional instanceof RuntimeException || additional instanceof Error
                ? additional : new IllegalStateException("failed to close %s".formatted(resource), additional);
        if (current == null) return normalized;
        if (current != normalized) current.addSuppressed(normalized);
        return current;
    }

    private static <T extends AutoCloseable> T launchDriver(final MediaDriver.Context context,
                                                            final Supplier<T> launcher) {
        RuntimeException lastFailure = null;
        final long timeoutMillis;
        try {
            timeoutMillis = Math.max(3_000L, Math.addExact(context.driverTimeoutMs(), 1_000L));
        } catch (final ArithmeticException overflow) {
            throw new IllegalArgumentException("Aeron driver timeout is too large", overflow);
        }
        final long deadline = ReplicationRetry.deadlineNanos(TimeUnit.MILLISECONDS.toNanos(timeoutMillis));
        int attempts = 0;
        while (!ReplicationRetry.expired(deadline)) {
            try {
                return launcher.get();
            } catch (final ActiveDriverException failure) {
                lastFailure = failure;
                attempts++;
                LOGGER.log(DEBUG,
                        "Aeron driver launch is waiting for a stale driver to release its mark file (attempt=%d, timeoutMillis=%d)"
                                .formatted(attempts, timeoutMillis), failure);
            }
            try {
                Thread.sleep(STALE_DRIVER_RETRY_DELAY_MILLIS);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting for stale Aeron driver cleanup", interrupted);
            }
        }
        throw new IllegalStateException("Aeron driver directory remained active after %d milliseconds".formatted(timeoutMillis), lastFailure);
    }

    static void ensurePrivateDirectory(final Path path) {
        ensurePrivateDirectory(path, false);
    }

        /// Creates a private Aeron directory and optionally fails closed when the
    /// filesystem cannot enforce owner-only permissions.
    ///
    /// @param path                         directory to create
    /// @param failIfPermissionsUnsupported whether a production caller must reject
    ///                                     a filesystem without POSIX permission support
    static void ensurePrivateDirectory(final Path path, final boolean failIfPermissionsUnsupported) {
        try {
            if (path == null) throw new NullPointerException("path");
            rejectSymbolicLinkComponents(path);
            try {
                Files.createDirectories(path,
                        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
            } catch (final UnsupportedOperationException ignored) {
                if (failIfPermissionsUnsupported) {
                    throw new IOException("Aeron production directories require owner-only permissions: %s".formatted(path));
                }
                LOGGER.log(WARNING, "Aeron directory filesystem does not support POSIX permissions: %s".formatted(path));
                Files.createDirectories(path);
            }
            if (!Files.isDirectory(path)) throw new IOException("path is not a directory");
            if (Files.isSymbolicLink(path)) throw new IOException("symbolic-link directory is not allowed: %s".formatted(path));
            try {
                Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"));
            } catch (final UnsupportedOperationException ignored) {
                if (failIfPermissionsUnsupported) {
                    throw new IOException("Aeron production directories require owner-only permissions: %s".formatted(path));
                }
                LOGGER.log(WARNING, "Aeron directory permissions cannot be hardened on this filesystem: %s".formatted(path));
            }
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot create or protect Aeron directory %s".formatted(path), failure);
        }
    }

        /// Rejects a path that reaches its directory through a symbolic-link
    /// component. Checking only the final path is insufficient: a link in a
    /// parent component can redirect driver, Archive, or checkpoint files outside
    /// the operator-owned directory after validation. Missing components are
    /// ignored and are created only after this check.
    private static void rejectSymbolicLinkComponents(final Path path) throws IOException {
        final Path absolute = path.toAbsolutePath().normalize();
        Path current = absolute.getRoot();
        for (final Path component : absolute) {
            current = current == null ? component : current.resolve(component);
            /* macOS exposes /tmp as the system-managed /private/tmp alias. It is
             * safe to follow that well-known alias; user-created links below it are
             * still rejected by the same walk. */
            if (Files.isSymbolicLink(current) && !isSystemTemporaryAlias(current)) {
                throw new IOException("symbolic-link path component is not allowed: %s".formatted(current));
            }
        }
    }

    private static boolean isSystemTemporaryAlias(final Path path) {
        /* macOS exposes both /tmp and /var through /private. They are OS-owned
         * aliases, not application-controlled links; rejecting them would make
         * every Files.createTempDirectory path unusable on the supported test and
         * developer platform. Links below either alias are still rejected. */
        return "/tmp".equals(path.toString()) || "/var".equals(path.toString());
    }

    private static boolean explicitlyDisablesSpySimulation(final String channel) {
        final int query = channel.indexOf('?');
        if (query < 0) return false;
        for (final String option : channel.substring(query + 1).split("\\|")) {
            if (option.trim().equalsIgnoreCase("ssc=false")) return true;
        }
        return false;
    }

    private void start(final Runnable beforeDriverLaunch) {
        ensurePrivateDirectory(this.settings.aeronDirectory(), this.settings.productionMode());
        final Path checkpointParent = this.settings.checkpointPath().toAbsolutePath().getParent();
        if (checkpointParent == null) throw new IllegalArgumentException("Aeron checkpoint path must have a parent directory");
        ensurePrivateDirectory(checkpointParent, this.settings.productionMode());
        final boolean embeddedWriter = "writer".equals(this.settings.role()) && !this.settings.externalArchive();
        if (embeddedWriter) ensurePrivateDirectory(this.settings.archiveDirectory(), this.settings.productionMode());
        final MediaDriver.Context media = new MediaDriver.Context()
                .aeronDirectoryName(this.settings.aeronDirectory().toString())
                .driverTimeoutMs(this.settings.driverTimeoutMillis())
                .threadingMode(this.settings.threadingMode())
                .mtuLength(this.settings.replication().mtuLength())
                .publicationTermBufferLength(this.settings.replication().termLength())
                .spiesSimulateConnection(embeddedWriter && !explicitlyDisablesSpySimulation(this.settings.liveChannel()))
                .errorHandler(this.errorHandler)
                .dirDeleteOnStart(false)
                .dirDeleteOnShutdown(false);
        beforeDriverLaunch.run();
        if (embeddedWriter) {
            final Archive.Context archiveContext = new Archive.Context()
                    .aeronDirectoryName(this.settings.aeronDirectory().toString())
                    .archiveDir(this.settings.archiveDirectory().toFile())
                    .deleteArchiveOnStart(false)
                    .threadingMode(this.settings.archiveThreadingMode())
                    .controlChannel(this.settings.controlChannel())
                    .localControlChannel("aeron:ipc")
                    .replicationChannel(this.settings.archiveReplicationChannel())
                    .segmentFileLength(this.settings.archiveSegmentFileLength())
                    .lowStorageSpaceThreshold(this.settings.archiveLowStorageSpaceThreshold())
                    .maxConcurrentReplays(this.settings.maxConcurrentReplays())
                    .errorHandler(this.errorHandler)
                    .fileSyncLevel(this.settings.archiveFileSyncLevel())
                    .catalogFileSyncLevel(this.settings.archiveFileSyncLevel());
            this.driver = launchDriver(media,
                    () -> ArchivingMediaDriver.launch(media.clone(), archiveContext.clone()));
        } else {
            this.driver = launchDriver(media, () -> MediaDriver.launch(media.clone()));
        }
        this.aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(this.settings.aeronDirectory().toString())
                .driverTimeoutMs(this.settings.driverTimeoutMillis())
                .errorHandler(this.errorHandler)
                .subscriberErrorHandler(this.errorHandler));
        this.archive = AeronArchive.connect(this.archiveContext());
    }

    Aeron aeron() {
        return this.aeron;
    }

    AeronArchive archive() {
        return this.archive;
    }

    AeronArchive.Context archiveContext() {
        return new AeronArchive.Context()
                .aeron(this.aeron)
                .aeronDirectoryName(this.settings.aeronDirectory().toString())
                .controlRequestChannel(this.settings.controlChannel())
                .controlResponseChannel(this.settings.controlResponseChannel())
                .errorHandler(this.errorHandler)
                .messageTimeoutNs(this.settings.replication().offerTimeoutNanos());
    }

    void stopDriver() {
        /* Shutdown is retryable and callers may legitimately stop a runtime that
         * failed between driver launch and Aeron client connection. Treat an already
         * absent driver as the idempotent terminal state instead of manufacturing a
         * second failure that masks the original startup error. */
        if (this.driver == null) return;
        try {
            this.driver.close();
            this.driver = null;
        } catch (final Exception failure) {
            throw new IllegalStateException("failed to stop Aeron driver", failure);
        }
    }

    @Override
    public void close() {
        final Throwable failure = this.closeAllQuietly();
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure != null) throw new IllegalStateException("failed to close Aeron runtime", failure);
    }

        /// Exhaustively releases every owned resource and aggregates close failures.
    private Throwable closeAllQuietly() {
        Throwable failure = null;
        if (this.archive != null) {
            try {
                this.archive.close();
                this.archive = null;
            } catch (final Throwable closeFailure) {
                failure = append(failure, closeFailure, "Archive");
            }
        }
        if (this.aeron != null) {
            try {
                this.aeron.close();
                this.aeron = null;
            } catch (final Throwable closeFailure) {
                failure = append(failure, closeFailure, "Aeron client");
            }
        }
        if (this.driver != null) {
            try {
                this.driver.close();
                this.driver = null;
            } catch (final Throwable closeFailure) {
                failure = append(failure, closeFailure, "Aeron driver");
            }
        }
        return failure;
    }
}
