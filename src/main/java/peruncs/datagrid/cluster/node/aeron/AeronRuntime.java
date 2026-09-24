package peruncs.datagrid.cluster.node.aeron;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveMarkFile;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.archive.codecs.mark.MessageHeaderDecoder;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.exceptions.ActiveDriverException;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;
import peruncs.datagrid.cluster.storage.ReplicationRetry;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

/// Owns one node's MediaDriver, Aeron client, and Archive client lifecycle.
///
/// When [AeronSettings] enables Aeron authentication, the embedded Archive
/// challenges every control session and the client presents the configured
/// credentials. Authentication is one layer only: the live, replay, control,
/// and watermark channels carry no encryption, so they must stay on an
/// isolated network. That network policy is the defense-in-depth boundary
/// against observers and denial-of-service; auth only keeps unauthenticated
/// peers from driving the Archive protocol.
final class AeronRuntime implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(AeronRuntime.class.getName());
    private static final long STALE_DRIVER_RETRY_DELAY_MILLIS = 100L;
    /// Matches the upstream rejection of a mark file whose semantic version never
    /// matched this build, as produced by [ArchiveMarkFile] validation.
    private static final Pattern REJECTED_MARK_VERSION = Pattern.compile(
            "mark file \\((.+?)\\) major version (\\d+) does not match software: \\d+");
    /// Matches the upstream rejection of a mark file whose last owner still appears alive.
    private static final Pattern ACTIVE_MARK_FILE = Pattern.compile("active mark file detected: (.+)");

    private final AeronSettings settings;
    private final ErrorHandler errorHandler;
    private final ErrorHandler subscriberErrorHandler;
    private AutoCloseable driver;
    private Aeron aeron;
    private AeronArchive archive;

    private AeronRuntime(final AeronSettings settings, final ErrorHandler errorHandler,
                         final ErrorHandler subscriberErrorHandler) {
        this.settings = settings;
        this.errorHandler = errorHandler;
        this.subscriberErrorHandler = subscriberErrorHandler;
    }

    static AeronRuntime start(final AeronSettings settings, final ErrorHandler errorHandler,
                              final ErrorHandler subscriberErrorHandler,
                              final Runnable beforeDriverLaunch) {
        final AeronRuntime runtime = new AeronRuntime(settings, errorHandler, subscriberErrorHandler);
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

    /// Launches the embedded Aeron driver and, for a writer, its embedded Archive,
    /// retrying while a previously crashed instance still owns the driver or archive
    /// mark files.
    ///
    /// A writer acquires its lease before [start], so this directory has a single
    /// launcher at a time and two crash leftovers can be recovered here:
    /// <ul>
    /// <li>A never-signaled archive mark file (major version 0) left by a writer
    /// killed during Archive startup is removed before the first attempt, and again
    /// if upstream rejects the launch anyway; see [repairsNeverSignaledArchiveMarkFile].
    /// Removing it up front matters: the driver and Archive launch as one unit, so a
    /// repair after a partial launch would restart the driver age-out wait and could
    /// exhaust the deadline.</li>
    /// <li>An active driver or archive mark file left by a process killed less than
    /// Aeron's liveness timeout ago is waited out, bounded by {@code timeoutMillis}
    /// ({@code driverTimeout + 1s}, which covers Aeron's fixed 10-second archive-mark
    /// liveness at the default 10-second driver timeout; a driver timeout below that
    /// liveness can exhaust the deadline and the next restart inherits the wait).</li>
    /// </ul>
    /// Any other failure propagates immediately: a wrong major version, a foreign
    /// path, or an unreadable file all fail closed.
    ///
    /// @param context         media driver context whose driver timeout bounds the retry
    /// @param archiveMarkFile archive mark file this launch owns, empty when the node
    ///                        runs without an embedded Archive
    /// @param launcher        starts the driver, or the driver and Archive together
    /// @return the launched resource, owned by the caller for shutdown
    static <T extends AutoCloseable> T launchDriver(final MediaDriver.Context context,
                                                    final Optional<Path> archiveMarkFile,
                                                    final Supplier<T> launcher) {
        RuntimeException lastFailure = null;
        final long timeoutMillis;
        try {
            timeoutMillis = Math.max(3_000L, Math.addExact(context.driverTimeoutMs(), 1_000L));
        } catch (final ArithmeticException overflow) {
            throw new IllegalArgumentException("Aeron driver timeout is too large", overflow);
        }
        archiveMarkFile.ifPresent(AeronRuntime::removeNeverSignaledArchiveMarkFile);
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
            } catch (final IllegalArgumentException failure) {
                if (!archiveMarkFile.map(markFile -> repairsNeverSignaledArchiveMarkFile(markFile, failure))
                        .orElse(false)) throw failure;
                lastFailure = failure;
                attempts++;
            } catch (final IllegalStateException failure) {
                if (!namesOwnActiveArchiveMarkFile(archiveMarkFile, failure)) throw failure;
                lastFailure = failure;
                attempts++;
                LOGGER.log(DEBUG,
                        "Aeron archive launch is waiting for a stale writer to release its mark file (attempt=%d, timeoutMillis=%d)"
                                .formatted(attempts, timeoutMillis), failure);
            }
            try {
                Thread.sleep(STALE_DRIVER_RETRY_DELAY_MILLIS);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting for stale Aeron driver cleanup", interrupted);
            }
        }
        throw new IllegalStateException("Aeron driver or archive did not become available within %d milliseconds"
                .formatted(timeoutMillis), lastFailure);
    }

    /// Removes an archive mark file that a writer killed during Archive startup left
    /// behind without ever signaling it, if and only if it still reads that way.
    ///
    /// The mark file version is written only by signalReady at the very end of Archive
    /// startup, so a kill between file creation and a completed startup leaves a file
    /// that still reads major version 0. Aeron rejects that file on every future launch,
    /// permanently bricking the archive directory: no archive data is lost, but no writer
    /// can start either. Major version 0 can only mean never signaled: signalReady only
    /// ever writes a positive version, and the writer lease plus this node's private
    /// directory make a live concurrent Archive in this directory unreachable. The mark
    /// file carries liveness and error state, never recordings, so removal is safe; a
    /// positive but different major is a real format mismatch and fails closed. The
    /// version is read immediately before the delete so a file signaled in the meantime
    /// is kept.
    ///
    /// @param markFile archive mark file to remove
    /// @return true when the file was removed
    private static boolean removeNeverSignaledArchiveMarkFile(final Path markFile) {
        try {
            if (!Files.exists(markFile) || !stillNeverSignaled(markFile)) return false;
            Files.delete(markFile);
            LOGGER.log(WARNING, "removed never-signaled archive mark file left by a killed writer: %s"
                    .formatted(markFile));
            return true;
        } catch (final IOException | RuntimeException failure) {
            LOGGER.log(WARNING, "failed to remove never-signaled archive mark file %s".formatted(markFile), failure);
            return false;
        }
    }

    /// Applies [removeNeverSignaledArchiveMarkFile] only when upstream's rejection
    /// names this launch's own mark file with the never-signaled major version 0.
    ///
    /// @param archiveMarkFile mark file this launch owns
    /// @param rejection       upstream rejection of that launch
    /// @return true when the file was removed and the launch may be retried
    private static boolean repairsNeverSignaledArchiveMarkFile(final Path archiveMarkFile,
                                                               final IllegalArgumentException rejection) {
        final Matcher rejected = REJECTED_MARK_VERSION.matcher(String.valueOf(rejection.getMessage()));
        if (!rejected.matches() || !"0".equals(rejected.group(2))) return false;
        try {
            final Path named = Path.of(rejected.group(1)).toRealPath();
            if (!named.equals(archiveMarkFile.toRealPath())) return false;
            return removeNeverSignaledArchiveMarkFile(named);
        } catch (final IOException | RuntimeException unreadable) {
            return false;
        }
    }

    /// Reads the semantic version from an archive mark file without modifying it,
    /// locating the field exactly as [ArchiveMarkFile] does.
    ///
    /// @param markFile archive mark file to read
    /// @return true only when the file positively reads a never-signaled version of 0
    /// @throws IOException when the file cannot be read or cannot hold a version field
    private static boolean stillNeverSignaled(final Path markFile) throws IOException {
        final int versionOffset = MessageHeaderDecoder.ENCODED_LENGTH + MarkFileHeaderDecoder.versionEncodingOffset();
        final ByteBuffer header = ByteBuffer.allocate(versionOffset + Integer.BYTES);
        try (final FileChannel channel = FileChannel.open(markFile, StandardOpenOption.READ)) {
            while (header.hasRemaining()) {
                if (channel.read(header, header.position()) < 0) break;
            }
        }
        if (header.hasRemaining()) throw new EOFException("archive mark file is too short: %s".formatted(markFile));
        final UnsafeBuffer buffer = new UnsafeBuffer(header.array());
        final MessageHeaderDecoder decoder = new MessageHeaderDecoder();
        decoder.wrap(buffer, 0);
        final int headerOffset = MarkFileHeaderDecoder.TEMPLATE_ID == decoder.templateId()
                && MarkFileHeaderDecoder.SCHEMA_ID == decoder.schemaId()
                ? MessageHeaderDecoder.ENCODED_LENGTH : 0;
        return 0 == buffer.getInt(headerOffset + MarkFileHeaderDecoder.versionEncodingOffset());
    }

    /// Reports whether an upstream rejection names this launch's own archive mark file
    /// as still active, that is, owned by a writer killed within Aeron's liveness window.
    ///
    /// @param archiveMarkFile archive mark file this launch owns, empty without an Archive
    /// @param rejection       upstream rejection of that launch
    /// @return true when the launch may wait and retry
    private static boolean namesOwnActiveArchiveMarkFile(final Optional<Path> archiveMarkFile,
                                                         final IllegalStateException rejection) {
        return archiveMarkFile
                .map(own -> matchesMarkFile(own, ACTIVE_MARK_FILE, rejection))
                .orElse(false);
    }

    /// Reports whether an upstream failure message names the given mark file, resolved
    /// to its real path so aliases such as macOS's {@code /var} still compare equal.
    ///
    /// @param own     mark file this launch owns
    /// @param pattern message shape carrying the rejected file path in its first group
    /// @param failure upstream failure to inspect
    /// @return true when the failure names exactly the given file
    private static boolean matchesMarkFile(final Path own, final Pattern pattern, final RuntimeException failure) {
        final Matcher rejected = pattern.matcher(String.valueOf(failure.getMessage()));
        if (!rejected.matches()) return false;
        try {
            return Path.of(rejected.group(1)).toRealPath().equals(own.toRealPath());
        } catch (final IOException | RuntimeException unreadable) {
            return false;
        }
    }

    /// Resolves the archive mark file an embedded Archive will open, honouring Aeron's
    /// optional mark-file directory override the same way [Archive] itself does.
    ///
    /// @param settings node settings holding the archive directory
    /// @return absolute path of the archive mark file
    private static Path archiveMarkFile(final AeronSettings settings) {
        final String relocated = Archive.Configuration.markFileDir();
        final Path directory = relocated == null || relocated.isEmpty()
                ? settings.topology().directories().archiveDirectory()
                : Path.of(relocated);
        return directory.resolve(ArchiveMarkFile.FILENAME);
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
            Objects.requireNonNull(path, "path");
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
        final ChannelUri uri = ChannelUri.parse(channel);
        return "false".equalsIgnoreCase(uri.get(CommonContext.SPIES_SIMULATE_CONNECTION_PARAM_NAME));
    }

    private void start(final Runnable beforeDriverLaunch) {
        ensurePrivateDirectory(this.settings.topology().directories().aeronDirectory(), this.settings.productionMode());
        final Path checkpointParent = this.settings.topology().directories().checkpointPath().toAbsolutePath().getParent();
        if (checkpointParent == null) throw new IllegalArgumentException("Aeron checkpoint path must have a parent directory");
        ensurePrivateDirectory(checkpointParent, this.settings.productionMode());
        final boolean embeddedWriter = this.settings.topology().role().isWriter() && !this.settings.archivePolicy().externalArchive();
        if (embeddedWriter) ensurePrivateDirectory(this.settings.topology().directories().archiveDirectory(), this.settings.productionMode());
        final MediaDriver.Context media = new MediaDriver.Context()
                .aeronDirectoryName(this.settings.topology().directories().aeronDirectory().toString())
                .driverTimeoutMs(this.settings.timeouts().driverTimeoutMillis())
                .threadingMode(this.settings.threadingMode())
                .mtuLength(this.settings.replication().mtuLength())
                .publicationTermBufferLength(this.settings.replication().termLength())
                .spiesSimulateConnection(embeddedWriter && !explicitlyDisablesSpySimulation(this.settings.topology().channels().live()))
                .errorHandler(this.errorHandler)
                .dirDeleteOnStart(false)
                .dirDeleteOnShutdown(false);
        beforeDriverLaunch.run();
        if (embeddedWriter) {
            final Archive.Context archiveContext = new Archive.Context()
                    .aeronDirectoryName(this.settings.topology().directories().aeronDirectory().toString())
                    .archiveDir(this.settings.topology().directories().archiveDirectory().toFile())
                    .deleteArchiveOnStart(false)
                    .threadingMode(this.settings.archiveThreadingMode())
                    .controlChannel(this.settings.topology().channels().control())
                    .localControlChannel("aeron:ipc")
                    .replicationChannel(this.settings.topology().channels().archiveReplication())
                    .segmentFileLength(this.settings.archivePolicy().segmentFileLength())
                    .lowStorageSpaceThreshold(this.settings.archivePolicy().lowStorageSpaceThreshold())
                    .maxConcurrentReplays(this.settings.archivePolicy().maxConcurrentReplays())
                    .errorHandler(this.errorHandler)
                    .fileSyncLevel(this.settings.archivePolicy().fileSyncLevel())
                    .catalogFileSyncLevel(this.settings.archivePolicy().fileSyncLevel());
            /* The MediaDriver context in this Aeron version exposes no authentication
             * hooks, so the embedded Archive is the enforcement point: it authenticates
             * control sessions while the driver stays a local IPC detail. */
            if (this.settings.authentication().enabled()) {
                archiveContext.authenticatorSupplier(this.settings.authenticatorSupplier());
                archiveContext.authorisationServiceSupplier(this.settings.authorisationServiceSupplier());
            }
            this.driver = launchDriver(media, Optional.of(archiveMarkFile(this.settings)),
                    () -> ArchivingMediaDriver.launch(media.clone(), archiveContext.clone()));
        } else {
            this.driver = launchDriver(media, Optional.empty(), () -> MediaDriver.launch(media.clone()));
        }
        this.aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(this.settings.topology().directories().aeronDirectory().toString())
                .driverTimeoutMs(this.settings.timeouts().driverTimeoutMillis())
                .errorHandler(this.errorHandler)
                .subscriberErrorHandler(this.subscriberErrorHandler));
        this.archive = AeronArchive.connect(this.archiveContext());
    }

    Aeron aeron() {
        return this.aeron;
    }

    AeronArchive archive() {
        return this.archive;
    }

    AeronArchive.Context archiveContext() {
        final AeronArchive.Context context = new AeronArchive.Context()
                .aeron(this.aeron)
                .aeronDirectoryName(this.settings.topology().directories().aeronDirectory().toString())
                .controlRequestChannel(this.settings.topology().channels().control())
                .controlResponseChannel(this.settings.topology().channels().controlResponse())
                .errorHandler(this.errorHandler)
                .messageTimeoutNs(this.settings.timeouts().archiveControlTimeoutNanos());
        if (this.settings.authentication().enabled()) {
            context.credentialsSupplier(this.settings.credentialsSupplier());
        }
        return context;
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
    ///
    /// The settings-held auth credentials are erased only once every resource
    /// is released: closing never re-authenticates, so erasing earlier is
    /// unnecessary, while erasing on a partial close would leave a retried
    /// close without diagnostics context.
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
        if (failure == null) {
            this.settings.clearAuthCredentials();
        }
        return failure;
    }
}
