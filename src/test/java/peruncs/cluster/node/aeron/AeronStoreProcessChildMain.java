package peruncs.cluster.node.aeron;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.cluster.api.NodeConfig;
import peruncs.cluster.node.replication.ClusterReplicationTransport;
import peruncs.cluster.storage.aeron.crashtest.RecordingInspector;
import peruncs.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.cluster.storage.io.FaultInjection;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/// Forked real-Store writer used to prove channel routing across process restart.
public final class AeronStoreProcessChildMain {
    private AeronStoreProcessChildMain() {
    }

    static void main(final String[] arguments) throws Exception {
        if (arguments.length != 1) throw new IllegalArgumentException("mode is required");
        final String mode = arguments[0];
        if (!mode.equals("initial") && !mode.equals("restart") && !mode.equals("dictionary") &&
            !mode.equals("crash-before-local-write") && !mode.equals("crash-after-local-write") &&
            !mode.equals("recover"))
            throw new IllegalArgumentException("unknown Store process mode: %s".formatted(mode));
        final Path root = Path.of(System.getProperty("dg.aeron.store.root"));
        final UUID clusterId = UUID.fromString(System.getProperty("dg.aeron.store.cluster"));
        final UUID nodeId = UUID.fromString(System.getProperty("dg.aeron.store.node"));
        final UUID generation = UUID.fromString(System.getProperty("dg.aeron.store.generation"));
        final Path storePath = root.resolve("store");
        final int replayControlPort = AeronStoreIntegrationIT.freePort();
        Files.createDirectories(root.resolve("control"));
        final AtomicBoolean rejectNext = new AtomicBoolean();
        final AtomicBoolean sawFourChannels = new AtomicBoolean();
        try (ClusterReplicationTransport transport = new AeronTransport(
                properties(root, clusterId, nodeId, generation, replayControlPort))) {
            final AtomicInteger dictionaryChunks = new AtomicInteger();
            final long[] beforeRejectedWrite = {-1L};
            /* Count dictionary chunks at the publication seam, rather than counting
             * exporter notifications.  A rejected ARCHIVE_FIRST transaction must
             * publish its retained dictionary again even though the Store exporter
             * quite correctly reports that type only once. */
            FaultInjection.callWithHook((name, sequence, path) ->
            {
                if (name == FaultInjection.Point.AFTER_DICTIONARY_CHUNKS) dictionaryChunks.incrementAndGet();
            }, () -> {
                final Distribution distributor = new Distribution();
                final Function<Supplier<StorageConnection>, UnaryOperator<PersistenceTarget<Binary>>>
                        targetFactory = storage -> delegate ->
                        transport.persistenceTargetFactory(distributor.outbox, distributor.enabled(), storage).apply(new PersistenceTarget<>() {
                            @Override
                            public void write(final Binary data) {
                                final int[] channels = {0};
                                data.iterateChannelChunks(ignored -> channels[0]++);
                                if (channels[0] >= 4) sawFourChannels.set(true);
                                if (rejectNext.compareAndSet(true, false))
                                    throw new IllegalStateException("injected real-Store rejection");
                                delegate.write(data);
                            }

                            @Override
                            public boolean isWritable() {
                                return delegate.isWritable();
                            }
                        });
                final EmbeddedStorageManager manager;
                if (mode.equals("initial")) {
                    final Root value = new Root();
                    for (int i = 0; i < 2_048; i++) value.objects.add(new StoreType("object-%s".formatted(i)));
                    manager = AeronStoreIntegrationIT.start(storePath, value, distributor, transport, targetFactory);
                    try {
                        for (final StoreType object : value.objects) object.value += "-updated";
                        AeronStoreIntegrationIT.store(transport, manager, value.objects.toArray());
                    } finally {
                        manager.shutdown();
                    }
                } else {
                    manager = AeronStoreIntegrationIT.startExisting(storePath, distributor, transport, targetFactory);
                    try {
                        final Root value = manager.root();
                        if (mode.equals("restart")) {
                            value.objects.getFirst().value += "-restart";
                            AeronStoreIntegrationIT.store(transport, manager, value.objects.getFirst());
                        } else if (mode.equals("dictionary")) {
                            /* Inject one local write rejection. After that
                             * the local store outcome is uncertain (the seen
                             * dictionary was already dispatched), so the
                             * publisher fails closed and the driver's retry
                             * must refuse instead of hiding acknowledged
                             * bytes behind a REJECTED restart marker. */
                            beforeRejectedWrite[0] = transport.positionProvider().latest().sequence();
                            value.retryObjects.add(new RetryType());
                            rejectNext.set(true);
                            try {
                                AeronStoreIntegrationIT.store(transport, manager, value.retryObjects.toArray());
                                throw new AssertionError("injected local rejection did not occur");
                            } catch (final RuntimeException expectedFromInjection) {
                                boolean refused = false;
                                try {
                                    AeronStoreIntegrationIT.store(transport, manager, value.retryObjects.toArray());
                                } catch (final RuntimeException alsoRefused) {
                                    /* expected: publisher refuses the retry once the local
                                     * outcome is uncertain. */
                                    refused = true;
                                }
                                if (!refused) throw new AssertionError("retry after fail-closed publisher was accepted");
                            }
                        } else if (mode.startsWith("crash-")) {
                            final long beforeSequence = transport.replicationMark().sequence();
                            final String crashCase = mode;
                            final String crashPoint = mode.equals("crash-before-local-write")
                                    ? "AFTER_PREPARE_BEFORE_LOCAL_WRITE" : "AFTER_LOCAL_WRITE_BEFORE_COMMIT";
                            value.objects.add(new StoreType(crashCase));
                            FaultInjection.callWithHook((point, sequence, path) ->
                            {
                                if (!crashPoint.equals(point.name())) return;
                                try {
                                    Files.writeString(root.resolve("control").resolve("crash-hook"),
                                            "%s;%s;%s".formatted(beforeSequence, sequence, crashPoint));
                                } catch (final Exception failure) {
                                    throw new IllegalStateException("cannot publish the crash hook", failure);
                                }
                                while (true) LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
                            }, () -> {
                                AeronStoreIntegrationIT.store(transport, manager, value.objects);
                                return null;
                            });
                            throw new AssertionError("process crash hook was not reached: " + crashPoint);
                        } else if (mode.equals("recover")) {
                            final String[] crash = Files.readString(root.resolve("control").resolve("crash-hook")).split(";");
                            final long beforeSequence = Long.parseLong(crash[0]);
                            final long crashSequence = Long.parseLong(crash[1]);
                            final AeronReplicationEnvelope.Kind expectedTerminal = crash[2].equals(
                                    "AFTER_PREPARE_BEFORE_LOCAL_WRITE")
                                    ? AeronReplicationEnvelope.Kind.ABORT : AeronReplicationEnvelope.Kind.COMMIT;
                            final boolean recoveredCrashValue = value.objects.stream()
                                    .anyMatch(object -> object.value.equals(
                                            crash[2].equals("AFTER_PREPARE_BEFORE_LOCAL_WRITE")
                                                    ? "crash-before-local-write" : "crash-after-local-write"));
                            final boolean expectedCrashValue = expectedTerminal == AeronReplicationEnvelope.Kind.COMMIT;
                            if (recoveredCrashValue != expectedCrashValue) {
                                throw new AssertionError("Store outcome disagrees with recovery terminal " + expectedTerminal);
                            }
                            final AeronTransport aeron = (AeronTransport) transport;
                            final var config = properties(root, clusterId, nodeId, generation, replayControlPort);
                            final var archive = aeron.runtimeOwner().archive();
                            final long stopPosition = archive.getRecordingPosition(transport.replicationMark().recordingId());
                            final var evidence = RecordingInspector.inspect(
                                    archive,
                                    transport.replicationMark().recordingId(),
                                    config.aeron().channels().replay(),
                                    config.aeron().streamId(),
                                    clusterId,
                                    config.aeron().epoch(),
                                    30_000L,
                                    stopPosition);
                            if (evidence.terminalBySequence().get(crashSequence) != expectedTerminal) {
                                throw new AssertionError("expected recovered terminal %s for sequence %s, got %s"
                                        .formatted(expectedTerminal, crashSequence,
                                                evidence.terminalBySequence().get(crashSequence)));
                            }
                            final StoreType followup = new StoreType("after-recovery");
                            value.objects.add(followup);
                            AeronStoreIntegrationIT.store(transport, manager, value.objects);
                            final long nextSequence = transport.positionProvider().latest().sequence();
                            if (nextSequence != beforeSequence + 3L) {
                                throw new AssertionError("recovery must consume the crash sequence; before=%s next=%s"
                                        .formatted(beforeSequence, nextSequence));
                            }
                            Files.writeString(root.resolve("control").resolve("recovery-outcome"),
                                    "terminal=%s;store=%s;next=%s"
                                            .formatted(expectedTerminal, recoveredCrashValue, nextSequence));
                        }
                    } finally {
                        manager.shutdown();
                    }
                }
                final long sequence = transport.positionProvider().latest().sequence();
                Files.writeString(root.resolve("control").resolve(mode),
                        "channels=%s;dictionaries=%s;before=%s;sequence=%s"
                                .formatted(sawFourChannels.get(), dictionaryChunks.get(), beforeRejectedWrite[0], sequence));
                return null;
            });
        }
    }

    private static NodeConfig properties(
            final Path root, final UUID clusterId, final UUID nodeId, final UUID generation,
            final int replayControlPort) {
        return TestNodeConfig.aeron(root, "writer", false, Map.of(
                NodeConfig.Setting.AERON_CLUSTER_ID.key(), clusterId.toString(),
                NodeConfig.Setting.AERON_NODE_ID.key(), nodeId.toString(),
                NodeConfig.Setting.AERON_STORE_GENERATION.key(), generation.toString(),
                NodeConfig.Setting.AERON_DIRECTORY.key(), root.resolve("driver").toString(),
                NodeConfig.Setting.AERON_ARCHIVE_DIRECTORY.key(), root.resolve("archive").toString(),
                NodeConfig.Setting.AERON_REPLAY_CHANNEL.key(),
                "aeron:udp?endpoint=localhost:0|control=localhost:%s|control-mode=dynamic".formatted(replayControlPort)));
    }

    /// Fixture Store root holding the ordinary and retry entity collections.
    public static final class Root {
        /// Creates an empty fixture root.
        public Root() {
        }

        /// Ordinary replicated entities.
        public final List<StoreType> objects = new ArrayList<>();
        /// Entities introduced only by the rejection/retry phase.
        public final List<RetryType> retryObjects = new ArrayList<>();
    }

    /// Ordinary fixture entity carrying one value.
    public static final class StoreType {
        /// Entity payload.
        public String value;

        /// Creates a fixture entity.
        ///
        /// @param value entity payload
        StoreType(final String value) {
            this.value = value;
        }
    }

    /// Entity introduced only by the rejection/retry phase of the process fixture.
    public static final class RetryType {
        /// Fixed retry marker payload.
        public final String value;

        /// Creates the retry marker entity.
        public RetryType() {
            this.value = "dictionary-retry";
        }
    }

}
