package peruncs.cluster.storage.aeron.reader;

import io.aeron.Aeron;
import io.aeron.archive.client.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.concurrent.IdleStrategy;
import org.eclipse.serializer.typing.Disposable;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.storage.ReplicationRetry;
import peruncs.cluster.storage.aeron.checkpoint.AeronReplicationCursor;
import peruncs.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.cluster.storage.binary.ReplicationApplier;
import peruncs.cluster.storage.binary.StorageBinaryDataReceiver;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/// Reads committed Store transactions from an Archive and then from the live
/// publication.
///
/// Replay starts at the supplied durable position. The reader reports live
/// only after replay catches up, so a restart needs no separate snapshot path.
/// The subscription belongs to this reader; the caller remains responsible for
/// the shared Aeron and Archive clients.
///
/// A lost Archive control channel (writer restart of its embedded Archive) is
/// treated as recoverable. An escaping [ArchiveException] swaps the
/// subscription for a fresh one resuming at the last resolved position; a
/// silent loss — this Aeron client self-heals by retrying forever, reporting
/// every failed attempt through [PersistentSubscriptionListener#onError] —
/// is bounded by a per-incident budget of the configured reader stop timeout:
/// while the incident lasts the replay makes no resolved progress, and once
/// the budget expires the reader latches a typed [ReseedRequiredException]
/// so the node fails closed with a RESEED_REQUIRED diagnosis instead of
/// stalling silently or dying on a raw transport stack. Resolved progress or
/// reaching the live stream clears the incident and resets the budget.
public final class AeronArchiveReader implements Disposable {
    /// Immutable setup for one Archive replay and live reader.
    ///
    /// The builder groups subscription wiring, recovered cursor, and delivery
    /// callbacks so same-typed values cannot be swapped at the call site.
    ///
    /// @param aeron                    shared Aeron client
    /// @param archiveContext           Archive connection settings
    /// @param recordingId              Archive recording id
    /// @param startPosition            first replay position
    /// @param liveChannel              live publication channel
    /// @param liveStreamId             live publication stream
    /// @param replayChannel            replay channel
    /// @param replayStreamId           replay stream
    /// @param replicationConfiguration framing and timeout limits
    /// @param clusterId                expected cluster identity
    /// @param wireNonce                expected accidental-cross-wiring nonce
    /// @param epoch                    expected writer epoch
    /// @param initialSequence          last sequence already applied
    /// @param initialPosition          last resolved Archive position
    /// @param receiver                 destination for complete Store binaries
    /// @param transactionResolved      callback after a transaction is delivered
    /// @param deliveryListener         callback around Store materialization
    public record Configuration(
            Aeron aeron,
            AeronArchive.Context archiveContext,
            long recordingId,
            long startPosition,
            String liveChannel,
            int liveStreamId,
            String replayChannel,
            int replayStreamId,
            AeronReplicationConfiguration replicationConfiguration,
            UUID clusterId,
            long wireNonce,
            long epoch,
            long initialSequence,
            long initialPosition,
            StorageBinaryDataReceiver receiver,
            Consumer<CursorSnapshot> transactionResolved,
            ReaderDeliveryListener deliveryListener
    ) {
        /// Validates required reader collaborators and recovered cursor bounds.
    public Configuration {
            Objects.requireNonNull(aeron, "aeron");
            Objects.requireNonNull(archiveContext, "archiveContext");
            Objects.requireNonNull(replicationConfiguration, "replicationConfiguration");
            Objects.requireNonNull(clusterId, "clusterId");
            if (wireNonce == 0L) throw new IllegalArgumentException("wireNonce must not be zero");
            Objects.requireNonNull(receiver, "receiver");
            Objects.requireNonNull(transactionResolved, "transactionResolved");
            if (initialSequence < -1 || initialSequence == Long.MAX_VALUE || initialPosition < -1) {
                throw new IllegalArgumentException("initial cursor must be sequence >= -1 and position >= -1");
            }
        }

        /// Starts a builder for an Archive reader.
        ///
        /// @return empty reader configuration builder
        public static Builder builder() {
            return new Builder();
        }

        /// Builds reader configuration without a long positional argument list.
        public static final class Builder {
            private Aeron aeron;
            private AeronArchive.Context archiveContext;
            private long recordingId;
            private long startPosition;
            private String liveChannel;
            private int liveStreamId;
            private String replayChannel;
            private int replayStreamId;
            private AeronReplicationConfiguration replicationConfiguration;
            private UUID clusterId;
            private long wireNonce;
            private boolean wireNonceSet;
            private long epoch;
            private long initialSequence = -1L;
            private long initialPosition = -1L;
            private StorageBinaryDataReceiver receiver;
            private Consumer<CursorSnapshot> transactionResolved = ignored -> {
            };
            private ReaderDeliveryListener deliveryListener;

            /// Creates an empty reader configuration builder.
            public Builder() {
            }

            /// Sets the shared Aeron client.
            ///
            /// @param value shared Aeron client
            /// @return this builder
            public Builder aeron(final Aeron value) { this.aeron = value; return this; }
            /// Sets the Archive connection settings.
            ///
            /// @param value Archive connection settings
            /// @return this builder
            public Builder archiveContext(final AeronArchive.Context value) { this.archiveContext = value; return this; }
            /// Sets the Archive recording id.
            ///
            /// @param value Archive recording id
            /// @return this builder
            public Builder recordingId(final long value) { this.recordingId = value; return this; }
            /// Sets the first replay position.
            ///
            /// @param value first replay position
            /// @return this builder
            public Builder startPosition(final long value) { this.startPosition = value; return this; }
            /// Sets the live channel.
            ///
            /// @param value live channel
            /// @return this builder
            public Builder liveChannel(final String value) { this.liveChannel = value; return this; }
            /// Sets the live stream id.
            ///
            /// @param value live stream id
            /// @return this builder
            public Builder liveStreamId(final int value) { this.liveStreamId = value; return this; }
            /// Sets the replay channel.
            ///
            /// @param value replay channel
            /// @return this builder
            public Builder replayChannel(final String value) { this.replayChannel = value; return this; }
            /// Sets the replay stream id.
            ///
            /// @param value replay stream id
            /// @return this builder
            public Builder replayStreamId(final int value) { this.replayStreamId = value; return this; }
            /// Sets the replication framing and timeout configuration.
            ///
            /// @param value replication framing and timeout configuration
            /// @return this builder
            public Builder replicationConfiguration(final AeronReplicationConfiguration value) { this.replicationConfiguration = value; return this; }
            /// Sets the cluster identity.
            ///
            /// @param value cluster identity
            /// @return this builder
            public Builder clusterId(final UUID value) { this.clusterId = value; return this; }
            /// Sets the accidental-cross-wiring nonce shared with the writer.
            ///
            /// Every reader must receive a deployment-chosen nonzero value.
            ///
            /// @param value shared deployment nonce
            /// @return this builder
            public Builder wireNonce(final long value) {
                this.wireNonce = value;
                this.wireNonceSet = true;
                return this;
            }
            /// Sets the writer epoch.
            ///
            /// @param value writer epoch
            /// @return this builder
            public Builder epoch(final long value) { this.epoch = value; return this; }
            /// Sets the already applied sequence.
            ///
            /// @param value already applied sequence
            /// @return this builder
            public Builder initialSequence(final long value) { this.initialSequence = value; return this; }
            /// Sets the already resolved Archive position.
            ///
            /// @param value already resolved Archive position
            /// @return this builder
            public Builder initialPosition(final long value) { this.initialPosition = value; return this; }
            /// Sets the Store binary receiver.
            ///
            /// @param value Store binary receiver
            /// @return this builder
            public Builder receiver(final StorageBinaryDataReceiver value) { this.receiver = value; return this; }
            /// Sets the post-transaction callback.
            ///
            /// @param value post-transaction callback
            /// @return this builder
            public Builder transactionResolved(final Consumer<CursorSnapshot> value) { this.transactionResolved = value; return this; }
            /// Sets the Store materialization callback, or `null`.
            ///
            /// @param value Store materialization callback, or `null`
            /// @return this builder
            public Builder deliveryListener(final ReaderDeliveryListener value) { this.deliveryListener = value; return this; }

            /// Builds the immutable reader configuration.
            ///
            /// @return immutable reader configuration
            public Configuration build() {
                if (!this.wireNonceSet) {
                    throw new IllegalStateException("wireNonce must be configured explicitly");
                }
                return new Configuration(aeron, archiveContext, recordingId, startPosition, liveChannel,
                        liveStreamId, replayChannel, replayStreamId, replicationConfiguration, clusterId,
                        this.wireNonce, epoch, initialSequence, initialPosition, receiver, transactionResolved, deliveryListener);
            }
        }
    }

    /* The subscription is replaced when the Archive control channel is lost
     * mid-replay: the polling thread closes the dead one and creates a fresh
     * PersistentSubscription resuming at the last resolved position. Only the
     * polling thread and the disposal path (after the polling thread exited)
     * ever touch this field. */
    private volatile PersistentSubscription subscription;
    private final Configuration configuration;
    private final TransactionAssembler assembler;
    private final ControlledFragmentHandler fragmentHandler;
    private final long stopTimeoutNanos;
    private final int fragmentsPerPoll;
    private final IdleStrategy idleStrategy;
    /* Poller-thread barrier coalescing state: when the first idle poll stamps
     * it and the idle delay runs out, the staged barrier flushes. */
    private long barrierIdleSinceNanos;
    private final long barrierIdleFlushNanos;
    private final AtomicBoolean active = new AtomicBoolean();
    private final AtomicLong stopDeadlineNanos = new AtomicLong();
    /* Sliding stop deadline: while a requested stop drains a replay backlog,
     * every newly resolved transaction — or newly materialized one, when the
     * Store receiver trails resolution under load — pushes the deadline out
     * by another stop timeout, so a slow-but-advancing reader is never timed
     * out. The overall cap still bounds a stop whose live tail perpetually
     * outruns replay. */
    private final AtomicLong stopRequestedNanos = new AtomicLong();
    private final AtomicLong stopProgressSequence = new AtomicLong(-1L);
    private final AtomicLong stopProgressApplied = new AtomicLong(-1L);
    private static final long STOP_OVERALL_TIMEOUT_MULTIPLIER = 10L;
    private volatile Thread thread;
    private volatile CountDownLatch stopped = new CountDownLatch(0);
    private volatile boolean disposed;
    /* Close-once guard for the subscription: a failed or timed-out disposal
     * leaves it false so a retry can still close after the polling thread
     * exits, while a successful close makes every later attempt a no-op. */
    private final AtomicBoolean subscriptionClosed = new AtomicBoolean();
    /* Seeding closes on the first start: the stale-token floor must be fixed
     * before any frame is accepted, and a late seed would silently lower it. */
    private volatile boolean seedingClosed;
    /* A disposal request is permanent even when its bounded wait times out.  Keep
     * it separate from disposed so callers can retry disposal without starting a
     * second poller against the same subscription and assembler. */
    private volatile boolean disposeRequested;
    private volatile boolean live;
    private volatile boolean stopAtLatest;
    /* A terminal outcome (FAILED, TIMED_OUT, CLOSED) is sticky: no later state,
     * including disposal, may downgrade it to a success.  All transitions go
     * through the atomic reference so a polling-thread timeout cannot race a
     * concurrent disposal into the wrong final state. */
     private final AtomicReference<ReplicationApplier.StopOutcome> stopOutcome =
            new AtomicReference<>(ReplicationApplier.StopOutcome.NOT_STARTED);
    /* Reconnect book-keeping, touched only by the polling thread. A zero
     * deadline means no disconnect incident is in flight; each incident gets
     * one full reconnect budget, and confirmed recovery (resolved progress or
     * reaching the live stream) resets deadline, baseline, cause, and
     * attempts. `incidentBaseline` is the resolved sequence at the moment the
     * incident started, so recovery is proven by the assembler advancing past
     * it. */
    private long reconnectDeadlineNanos;
    private long reconnectAttempts;
    private long incidentBaselineSequence = -1L;
    private Exception reconnectCause;
    /* Set from the subscription listener (which runs on the polling thread
     * inside controlledPoll) and consumed by pollSubscription: a non-null
     * value means the Archive client signalled a control-channel problem.
     * Shared with every replacement subscription created on reconnect. */
    private final AtomicReference<Exception> archiveIncidentSignal;


    AeronArchiveReader(
            final PersistentSubscription subscription,
            final Configuration configuration,
            final AtomicReference<Exception> incidentSignal) {
        this.subscription = Objects.requireNonNull(subscription, "subscription");
        try {
            final Configuration required = Objects.requireNonNull(configuration, "configuration");
            this.configuration = required;
            this.archiveIncidentSignal = Objects.requireNonNull(incidentSignal, "incidentSignal");
            final AeronReplicationConfiguration requiredConfiguration = required.replicationConfiguration();
            this.stopTimeoutNanos = requiredConfiguration.readerStopTimeoutNanos();
            this.fragmentsPerPoll = requiredConfiguration.readerFragmentsPerPoll();
            this.barrierIdleFlushNanos = requiredConfiguration.readerBarrierIdleFlushNanos();
            this.idleStrategy = requiredConfiguration.retryPolicy().idleStrategy();
            this.assembler = new TransactionAssembler(
                    requiredConfiguration, required.clusterId(), required.epoch(), required.initialSequence(),
                    required.initialPosition(), required.receiver(), required.transactionResolved(),
                    required.deliveryListener(), required.wireNonce()
            );
            this.fragmentHandler = (buffer, offset, length, header) -> {
                this.assembler.onFragment(buffer, offset, length, header);
                return this.assembler.deliveryBarrierFull()
                        ? ControlledFragmentHandler.Action.BREAK
                        : ControlledFragmentHandler.Action.CONTINUE;
            };
        } catch (final RuntimeException | Error failure) {
            try {
                subscription.close();
            } catch (final RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

        /// Creates a reader with the supplied Archive, live, and cursor setup.
    /// The reader does not close the shared Aeron or Archive clients.
    ///
    /// @param configuration immutable reader configuration
    /// @return a reader that owns its subscription
    public static AeronArchiveReader create(final Configuration configuration) {
        final Configuration settings = Objects.requireNonNull(configuration, "configuration");
        final AtomicReference<Exception> incidentSignal = new AtomicReference<>();
        final PersistentSubscription.Context subscriptionContext = subscriptionContext(
                settings, settings.startPosition(), incidentListener(incidentSignal));
        PersistentSubscription subscription = null;
        try {
            subscription = PersistentSubscription.create(subscriptionContext);
            return new AeronArchiveReader(subscription, settings, incidentSignal);
        } catch (final RuntimeException | Error failure) {
            if (subscription != null) {
                try {
                    subscription.close();
                } catch (final RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            } else {
                try {
                    subscriptionContext.close();
                } catch (final RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

        /// Builds the subscription context for one replay attempt.
    ///
    /// Shared by the initial create and by a mid-replay reconnect so both
    /// paths wire identical channels, stream ids, and Archive settings; only
    /// the start position moves (to the last resolved boundary on a
    /// reconnect).
    private static PersistentSubscription.Context subscriptionContext(
            final Configuration settings, final long startPosition,
            final PersistentSubscriptionListener listener) {
        final AeronArchive.Context subscriptionArchiveContext =
                settings.archiveContext().clone().aeron(settings.aeron());
        return new PersistentSubscription.Context()
                .aeron(settings.aeron())
                .ownsAeronClient(false)
                .recordingId(settings.recordingId())
                .startPosition(startPosition)
                .liveChannel(settings.liveChannel())
                .liveStreamId(settings.liveStreamId())
                .replayChannel(settings.replayChannel())
                .replayStreamId(settings.replayStreamId())
                .listener(listener)
                .aeronArchiveContext(subscriptionArchiveContext);
    }

        /// Reports Archive control-channel problems into the incident signal.
    ///
    /// The Archive client inside a [PersistentSubscription] self-heals a lost
    /// control channel by reconnecting forever; it never throws and only
    /// reports each failed attempt here. The reader consumes the signal on
    /// the polling thread (the only poller of the subscription, hence the
    /// only invoker of this listener) and bounds the heal window with the
    /// reconnect budget.
    private static PersistentSubscriptionListener incidentListener(
            final AtomicReference<Exception> incidentSignal) {
        return new PersistentSubscriptionListener()
        {
            @Override
            public void onLiveJoined() {
            }

            @Override
            public void onLiveLeft() {
            }

            @Override
            public void onError(final Exception error) {
                incidentSignal.compareAndSet(null, error);
            }
        };
    }

        /// Sets the next stop outcome unless a terminal outcome already won.
    ///
    /// Every transition goes through here so a later event can never downgrade a
    /// reader that already failed, timed out, or closed to a cleaner-looking
    /// state. Non-terminal states (RUNNING, STOPPING, and the resolved boundary
    /// markers) are overwritten as usual.
    private void updateOutcome(final ReplicationApplier.StopOutcome next) {
        this.stopOutcome.updateAndGet(current -> switch (current) {
            case FAILED, TIMED_OUT, CLOSED -> current;
            default -> next;
        });
    }

        /// Starts replay and live polling; repeated calls have no effect.
    public synchronized void start() {
        this.seedingClosed = true;
        if (this.disposed || this.disposeRequested) {
            throw new IllegalStateException("Aeron Archive reader is disposed or stopping for disposal");
        }
        if (this.assembler.failure() != null) {
            throw new IllegalStateException(
                    "cannot start a failed Aeron Archive reader; create a new reader from its durable cursor",
                    this.assembler.failure());
        }
        final Thread existing = this.thread;
        if (existing != null && existing.isAlive()) {
            throw new IllegalStateException("Aeron Archive reader is still stopping");
        }
        if (this.active.getAndSet(true)) {
            return;
        }
        this.stopAtLatest = false;
        this.stopDeadlineNanos.set(0L);
        this.stopRequestedNanos.set(0L);
        this.stopProgressSequence.set(-1L);
        this.stopProgressApplied.set(-1L);
        this.live = false;
        /* A new polling run starts with a full reconnect budget: the previous
         * run's disconnect incident must not eat into this one's. */
        this.reconnectDeadlineNanos = 0L;
        this.reconnectAttempts = 0L;
        this.reconnectCause = null;
        this.incidentBaselineSequence = -1L;
        this.archiveIncidentSignal.set(null);
        /* Preserve any terminal outcome: a restart after STOPPED/RESOLVED_BOUNDARY
         * becomes RUNNING, but a failed or timed-out reader never looks healthy. */
        this.updateOutcome(ReplicationApplier.StopOutcome.RUNNING);
        this.stopped = new CountDownLatch(1);
        /* A raw daemon thread, deliberately not Agrona's AgentRunner: the
         * subscription is poll-driven (poll returns at N fragments or on
         * idle), and AgentRunner's duty-cycle re-invocation would add a
         * second idling layer on top of the fragment-pull loop while this
         * thread must also bridge blockingly to the Store importer. */
        this.thread = Thread.ofPlatform().daemon().name("datagrid-aeron-archive-reader").unstarted(this::run);
        this.thread.start();
    }

    private void run() {
        final CountDownLatch lifecycleStopped = this.stopped;
        try {
            AeronReaderLifecycle.runPollingLoop(
                    this.active,
                    () ->
                    {
                        final PersistentSubscription current = this.subscription;
                        return current != null && current.hasFailed() || this.assembler.failure() != null;
                    },
                    this::pollSubscription,
                    () -> this.stopAtLatest && this.live && !this.assembler.hasIncompleteTransaction(),
                    () -> this.stopAtLatest && ReplicationRetry.expired(this.stopDeadlineNanos.get()),
                    () ->
                    {
                        this.updateOutcome(ReplicationApplier.StopOutcome.TIMED_OUT);
                        this.assembler.failure(new IllegalStateException(
                                "Timed out waiting for Aeron Archive replay to reach the live tail"));
                    },
                    this.idleStrategy
            );
            /* Publish whatever the final polls staged so a stop at the live
             * tail always ends at a flushed cursor boundary. */
            this.assembler.flushDeliveries();
            this.completeRun();
        } catch (final RuntimeException e) {
            this.assembler.failure(e);
        } catch (final Error e) {
            this.assembler.failure(new IllegalStateException("Aeron Archive reader polling failed", e));
            throw e;
        } finally {
            this.finishRun(lifecycleStopped);
        }
    }

        /// Polls the current subscription, reconnecting once per Archive loss.
    ///
    /// An [ArchiveException] escaping [PersistentSubscription#controlledPoll]
    /// never carries a replay-protocol verdict (those arrive through the
    /// listener and set `hasFailed`); it means the Archive control channel
    /// itself dropped, typically because the writer process restarted its
    /// embedded Archive mid-replay. That is recoverable: the recording is the
    /// durable source, so the reader swaps in a fresh subscription resuming at
    /// the last resolved position instead of dying on a raw transport stack.
    ///
    /// @return fragments consumed by this poll, or `0` while reconnecting
    private int pollSubscription() {
        final PersistentSubscription current = this.subscription;
        if (current == null) {
            if (!this.active.get() || this.disposeRequested) {
                return 0;
            }
            /* A previous reconnect attempt could not even create the
             * subscription (channel still unresolved). Stay in reconnect mode
             * until either create succeeds or the reconnect budget expires. */
            this.reconnectAfterArchiveLoss();
            return 0;
        }
        this.failIfReconnectBudgetExpired();
        try {
            final int work = current.controlledPoll(this.fragmentHandler, this.fragmentsPerPoll);
            this.live = current.isLive();
            this.trackArchiveIncident();
            if (this.stopAtLatest) extendStopDeadline();
            /* A full window broke the poll above; flush here, outside the
             * fragment callback, so the blocking materialization wait never
             * stalls the subscription's fragment handler. */
            if (this.assembler.deliveryBarrierFull()) {
                this.assembler.flushDeliveries();
                this.barrierIdleSinceNanos = 0L;
            }
            /* Time-based barrier flush: a replay backlog that drips in one
             * fragment per poll still fills whole barriers between flushes,
             * while a quiet live tail gets its cursor after the configured
             * bounded idle delay. A busy poll never pays for this check. */
            if (work == 0) {
                if (this.assembler.unflushedDeliveryCount() > 0) {
                    if (this.barrierIdleSinceNanos == 0L) {
                        this.barrierIdleSinceNanos = System.nanoTime();
                    } else if (System.nanoTime() - this.barrierIdleSinceNanos >= this.barrierIdleFlushNanos) {
                        this.assembler.flushDeliveries();
                        this.barrierIdleSinceNanos = 0L;
                    }
                } else {
                    this.barrierIdleSinceNanos = 0L;
                }
            } else {
                this.barrierIdleSinceNanos = 0L;
            }
            return work;
        } catch (final ArchiveException disconnect) {
            if (!this.active.get() || this.disposeRequested) {
                /* Stopping or disposing: the transport failure surfaces as-is
                 * — reconnecting a reader that is about to go away would only
                 * hide the shutdown behind Archive noise. */
                throw disconnect;
            }
            this.tearDownReconnectableSubscription(current, disconnect);
            this.reconnectAfterArchiveLoss();
            return 0;
        }
    }

        /// Tracks Archive control-channel incidents signalled by the subscription
    /// listener and clears them on confirmed recovery.
    ///
    /// The Archive client self-heals a lost channel silently, so an incident
    /// is only observable as listener errors plus absent resolved progress.
    /// The incident opens at the first signal; a signal after the live stream
    /// joined is harmless (the live image no longer needs the Archive), and
    /// resolved progress or reaching live proves the channel recovered and
    /// closes the incident with the full budget restored.
    private void trackArchiveIncident() {
        final Exception signalled = this.archiveIncidentSignal.getAndSet(null);
        if (this.reconnectDeadlineNanos == 0L) {
            if (signalled != null && !this.live) {
                this.reconnectDeadlineNanos = ReplicationRetry.deadlineNanos(this.stopTimeoutNanos);
                this.incidentBaselineSequence = this.assembler.lastResolvedSequence();
                this.reconnectCause = signalled;
            }
            return;
        }
        if (this.live ||
            this.assembler.lastResolvedSequence() != this.incidentBaselineSequence) {
            this.reconnectDeadlineNanos = 0L;
            this.incidentBaselineSequence = -1L;
            this.reconnectAttempts = 0L;
            this.reconnectCause = null;
        }
    }

        /// Latches the typed reseed failure once an incident outlives its budget.
    private void failIfReconnectBudgetExpired() {
        if (this.reconnectDeadlineNanos != 0L && ReplicationRetry.expired(this.reconnectDeadlineNanos)) {
            throw new ReseedRequiredException(
                    ("Aeron Archive response channel stayed disconnected past the %dns reconnect budget " +
                     "after %d attempts; recording %d cannot be replayed further from position %d without a reseed")
                            .formatted(this.stopTimeoutNanos, this.reconnectAttempts,
                                    this.configuration.recordingId(), this.assembler.lastResolvedPosition()),
                    this.reconnectCause);
        }
    }

    /// Retires the dead subscription after committing the current barrier.
    ///
    /// Runs on the polling thread, so closing here never races a fragment
    /// callback. Completed staged transactions must become durable before the
    /// subscription is replaced; otherwise replay resumes from the older
    /// position while the assembler still expects the later sequence. Only a
    /// partially assembled transaction is dropped and replayed.
    private void tearDownReconnectableSubscription(
            final PersistentSubscription current, final ArchiveException disconnect) {
        this.subscription = null;
        this.live = false;
        try {
            this.assembler.flushDeliveries();
        } finally {
            this.assembler.dispose();
            this.reconnectCause = disconnect;
            try {
                current.close();
            } catch (final RuntimeException closeFailure) {
                disconnect.addSuppressed(closeFailure);
            }
        }
    }

        /// Attempts one reconnect, or fails closed once the reconnect budget is spent.
    ///
    /// The budget is the configured reader stop timeout: a writer restart —
    /// the only healthy cause of an Archive disconnect — completes in seconds,
    /// while a channel that is still down after the full stop budget is not a
    /// restart but an operator problem, and a fresh reader from the durable
    /// cursor has no better odds of succeeding. Each attempt is paced by the
    /// retry policy's idle strategy so a dead Archive is not hammered in a
    /// tight loop. Expiry latches a typed [ReseedRequiredException]
    /// so the node reports RESEED_REQUIRED instead of an anonymous transport
    /// stack.
    private void reconnectAfterArchiveLoss() {
        if (this.reconnectDeadlineNanos == 0L) {
            this.reconnectDeadlineNanos = ReplicationRetry.deadlineNanos(this.stopTimeoutNanos);
            this.incidentBaselineSequence = this.assembler.lastResolvedSequence();
        }
        this.failIfReconnectBudgetExpired();
        try {
            final long resolvedPosition = this.assembler.lastResolvedPosition();
            final long resumePosition = resolvedPosition >= 0
                    ? resolvedPosition : this.configuration.startPosition();
            this.subscription = PersistentSubscription.create(subscriptionContext(
                    this.configuration, resumePosition, incidentListener(this.archiveIncidentSignal)));
            /* The replacement needs its own close-once domain: after a dispose
             * raced a reconnect, the old guard value may already be set. The
             * incident budget survives creation: the replacement still has to
             * prove recovery with resolved progress within the same budget. */
            this.subscriptionClosed.set(false);
        } catch (final RuntimeException createFailure) {
            if (this.reconnectCause == null) {
                this.reconnectCause = createFailure;
            } else if (this.reconnectCause != createFailure) {
                this.reconnectCause.addSuppressed(createFailure);
            }
        }
        this.reconnectAttempts++;
        this.idleStrategy.idle(0);
    }

        /// Maps a terminal subscription failure to a reader failure type.
    ///
    /// A replay that can no longer start because the recording no longer covers
    /// the reader's position — or no longer exists at all — is unrecoverable
    /// from this node's local state: the durable cursor points into deleted
    /// history. Surface it as a typed reseed signal instead of a generic
    /// failure so the node health view reports RESEED_REQUIRED.
    private RuntimeException classifySubscriptionFailure(final PersistentSubscription current) {
        final Exception reason = current.failureReason();
        if (reason instanceof PersistentSubscriptionException subscriptionFailure &&
            (subscriptionFailure.reason() == PersistentSubscriptionException.Reason.INVALID_START_POSITION ||
             subscriptionFailure.reason() == PersistentSubscriptionException.Reason.RECORDING_NOT_FOUND)) {
            return new ReseedRequiredException(
                    ("Aeron recording %d no longer covers this reader's durable cursor (position %d, " +
                     "sequence %d); reseed required: %s").formatted(
                            this.configuration.recordingId(), this.assembler.lastResolvedPosition(),
                            this.assembler.lastResolvedSequence(), subscriptionFailure.getMessage()),
                    subscriptionFailure);
        }
        return new IllegalStateException("PersistentSubscription failed", reason);
    }

    private synchronized void completeRun() {
        final ReplicationApplier.StopOutcome current = this.stopOutcome.get();
        if (current == ReplicationApplier.StopOutcome.TIMED_OUT ||
            current == ReplicationApplier.StopOutcome.CLOSED) {
            return;
        }
        final PersistentSubscription currentSubscription = this.subscription;
        if (currentSubscription != null && currentSubscription.hasFailed()) {
            this.assembler.failure(this.classifySubscriptionFailure(currentSubscription));
        }
        if (this.assembler.failure() != null) {
            this.updateOutcome(ReplicationApplier.StopOutcome.FAILED);
            return;
        }
        this.updateOutcome(this.stopAtLatest && this.live
                ? ReplicationApplier.StopOutcome.RESOLVED_BOUNDARY
                : ReplicationApplier.StopOutcome.STOPPED);
    }

    private synchronized void finishRun(final CountDownLatch lifecycleStopped) {
        if (this.assembler.failure() != null) {
            this.updateOutcome(ReplicationApplier.StopOutcome.FAILED);
        }
        this.active.set(false);
        this.live = false;
        lifecycleStopped.countDown();
    }

        /// Restarts a reader stopped at the live tail. A replay or validation failure
    /// is terminal; create a new reader from the durable cursor instead of
    /// reusing incomplete transaction state.
    public synchronized void resume() {
        final RuntimeException failure = this.failure();
        if (failure != null) {
            throw new IllegalStateException(
                    "cannot resume a failed Aeron Archive reader; create a new reader from its durable cursor", failure);
        }
        this.stopAtLatest = false;
        this.start();
    }

        /// Requests a stop after replay reaches the current live tail.
    public synchronized void stopAtLatestMessage() {
        if (this.disposed) {
            return;
        }
        if (this.disposeRequested) {
            throw new IllegalStateException("Aeron Archive reader is stopping for disposal");
        }
        if (this.stopOutcome.get() == ReplicationApplier.StopOutcome.FAILED || this.failure() != null) {
            return;
        }
        /* The deadline precedes the flag: the polling thread tests the flag
         * first and the deadline second, so publishing the flag first would
         * let one iteration observe a live stop request against the reset
         * (already-expired) deadline and time out instantly. A reader that
         * observes the flag also observes this write: both sit in one
         * synchronized block ahead of the volatile flag publication. */
        this.stopDeadlineNanos.set(ReplicationRetry.deadlineNanos(this.stopTimeoutNanos));
        this.stopRequestedNanos.set(System.nanoTime());
        this.stopProgressSequence.set(this.assembler.lastResolvedSequence());
        this.stopProgressApplied.set(this.assembler.lastAppliedSequence());
        this.stopAtLatest = true;
        if (this.active.get()) this.updateOutcome(ReplicationApplier.StopOutcome.STOPPING);
    }

        /// Pushes the stop deadline out while replay resolution or Store
    /// materialization advances.
    ///
    /// Each newly resolved transaction — or newly materialized one, when the
    /// receiver trails resolution — grants another full stop timeout, so a
    /// slow-but-advancing drain never times out. The extension is capped at an
    /// overall multiple of the stop timeout from the request: a stalled drain
    /// still fails at one timeout, and a live tail that perpetually outruns
    /// replay fails bounded instead of hanging a rolling restart.
    private void extendStopDeadline() {
        final long resolved = this.assembler.lastResolvedSequence();
        final long applied = this.assembler.lastAppliedSequence();
        final boolean resolving = resolved != this.stopProgressSequence.getAndSet(resolved);
        final boolean applying = applied != this.stopProgressApplied.getAndSet(applied);
        if (!resolving && !applying) return;
        final long extended = ReplicationRetry.deadlineNanos(this.stopTimeoutNanos);
        final long overall =
                this.stopRequestedNanos.get() + this.stopTimeoutNanos * STOP_OVERALL_TIMEOUT_MULTIPLIER;
        this.stopDeadlineNanos.set(Math.min(extended, overall));
    }

        /// Returns the last sequence delivered after commit and checksum validation.
    ///
    /// @return last resolved transaction sequence
    public long lastResolvedSequence() {
        return this.assembler.lastResolvedSequence();
    }

        /// Returns the last sequence materialized by the Store receiver.
    ///
    /// @return last applied transaction sequence
    public long lastAppliedSequence() {
        return this.assembler.lastAppliedSequence();
    }

        /// Returns whether the polling lifecycle is still active and has not failed.
    ///
    /// @return `true` when the reader is running
    public boolean isRunning() {
        final Thread pollingThread = this.thread;
        return !this.disposeRequested &&
               (this.active.get() || (pollingThread != null && pollingThread.isAlive())) &&
               this.failure() == null;
    }

        /// Returns whether replay has transitioned to the live subscription.
    ///
    /// @return `true` after replay reaches the live stream
    public boolean isLive() {
        return this.live;
    }

        /// Returns the delivered transactions not yet published by a barrier flush.
    ///
    /// Cursor callbacks persist their durable boundary only when this is zero:
    /// every earlier cursor in one delivery barrier is immediately superseded
    /// by the barrier's tail, so forcing it would cost a pair of fsyncs per
    /// transaction without advancing the restart point for a live workflow.
    ///
    /// @return staged-but-unflushed delivery count
    public int unflushedDeliveryCount() {
        return this.assembler.unflushedDeliveryCount();
    }

        /// Returns the Archive position of the last resolved commit.
    ///
    /// @return last resolved Archive position
    public long lastResolvedPosition() {
        return this.assembler.lastResolvedPosition();
    }

        /// Returns an atomic sequence/position snapshot for cursor persistence.
    ///
    /// @return current cursor snapshot
    public CursorSnapshot cursorSnapshot() {
        return this.assembler.cursorSnapshot();
    }

        /// Builds a cursor that can resume this reader from the same recording.
    ///
    /// @param nodeId          node that will own the resumed cursor
    /// @param storeGeneration Store image identity
    /// @param recordingId     Aeron Archive recording identity
    /// @return durable cursor for the current reader boundary
    public AeronReplicationCursor cursor(
            final UUID nodeId,
            final UUID storeGeneration,
            final long recordingId
    ) {
        final CursorSnapshot snapshot = this.assembler.cursorSnapshot();
        return new AeronReplicationCursor(
                this.assembler.clusterId(),
                nodeId,
                storeGeneration,
                this.assembler.epoch(),
                this.assembler.fencingToken(),
                recordingId,
                snapshot.position(),
                snapshot.sequence()
        );
    }

        /// Seeds the assembler's stale-token floor from the durable cursor.
    ///
    /// Call before [#start()] only. The floor must be fixed before the reader
    /// accepts any frame, so a restart never re-accepts history from a writer
    /// its cursor already moved past; seeding after start would silently move
    /// the floor under live validation and is rejected.
    ///
    /// @param fencingToken greatest token the persisted cursor accepted, or `0` for a new reader
    /// @throws IllegalStateException when the reader is already started
    public synchronized void seedFencingToken(final long fencingToken) {
        if (this.seedingClosed) {
            throw new IllegalStateException("Aeron Archive reader fencing seed is only accepted before start()");
        }
        this.assembler.startingFencingToken(fencingToken);
    }

        /// Returns the greatest writer fencing token accepted so far.
    ///
    /// @return greatest accepted fencing token
    public long fencingToken() {
        return this.assembler.fencingToken();
    }

        /// Returns the terminal polling failure, or `null` while healthy.
    ///
    /// @return terminal failure, or `null`
    public RuntimeException failure() {
        return this.assembler.failure();
    }

        /// Returns the stop boundary outcome and never infers success from a dead thread.
    ///
    /// @return current stop outcome
    public ReplicationApplier.StopOutcome stopOutcome() {
        return this.stopOutcome.get();
    }

        /// Returns the terminal stop state and the last resolved sequence/position.
    ///
    /// @return current stop result
    public ReplicationApplier.StopResult stopResult() {
        final CursorSnapshot cursor = this.assembler.cursorSnapshot();
        return new ReplicationApplier.StopResult(this.stopOutcome.get(), cursor.sequence(), cursor.position());
    }

        /// Stops polling after a terminal Aeron client or MediaDriver failure.
    ///
    /// @param failure terminal failure
    public synchronized void fail(final RuntimeException failure) {
        this.assembler.failure(Objects.requireNonNull(failure, "failure"));
        this.active.set(false);
        this.live = false;
        this.updateOutcome(ReplicationApplier.StopOutcome.FAILED);
    }

        /// Stops polling and releases this reader's subscriptions.
    ///
    /// Disposal is idempotent and retry-safe. If the polling thread does not
    /// terminate within the bounded shutdown window this method throws and
    /// leaves the subscription and assembler-owned buffers intact. A later call
    /// retries the stop and closes the subscription only once the thread has
    /// exited, which preserves native-buffer ownership and avoids closing a
    /// subscription under the polling thread. After a successful close every
    /// later call is a no-op.
    @Override
    public void dispose() {
        final Thread pollingThread;
        synchronized (this) {
            if (this.disposed) return;
            this.disposeRequested = true;
            if (this.active.get()) this.updateOutcome(ReplicationApplier.StopOutcome.STOPPING);
            pollingThread = this.thread;
        }
        AeronReaderLifecycle.stopAndClose(this.active, pollingThread, this.stopped, this.subscriptionClosed,
                () ->
                {
                    final PersistentSubscription current = this.subscription;
                    if (current != null) current.close();
                }, this.stopTimeoutNanos);
        synchronized (this) {
            this.assembler.dispose();
            this.thread = null;
            this.disposed = true;
            /* A failed or timed-out reader must not be reported as a clean close. */
            this.updateOutcome(ReplicationApplier.StopOutcome.CLOSED);
        }
    }

}
