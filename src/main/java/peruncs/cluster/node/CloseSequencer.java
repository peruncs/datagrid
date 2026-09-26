package peruncs.cluster.node;

import peruncs.cluster.errors.NodeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;

import static java.lang.System.Logger.Level.DEBUG;

/// Runs an ordered list of close stages, aggregating every failure with
/// {@link Error} priority.
///
/// This is the module's single close-aggregation utility: assembly/node
/// teardown, transport shutdown, and manager close loops all share it, so
/// failure precedence cannot drift between them. Stages run in insertion
/// order, which must be reverse dependency order, and a stage whose
/// {@link Stage#ready()} precondition is false at run time is skipped — that is what
/// makes a retried close re-run only the stages still owing work instead of
/// tearing down beneath live resources.
///
/// Every ready stage is attempted even when an earlier stage fails. The
/// aggregation keeps the first {@link Error} and the first other failure
/// separately and always surfaces the {@link Error} with the other failure
/// attached as suppressed, so a fatal condition is never buried under a
/// preceding {@link RuntimeException}.
///
/// The class is public only because the node, node.aeron, and node.backup
/// packages share it; it is not application API.
///
/// @since 1.0
public final class CloseSequencer {
    private static final System.Logger LOGGER = System.getLogger(CloseSequencer.class.getName());

    private final List<Stage> stages = new ArrayList<>();

    /// Creates an empty sequencer for the builder-style {@link #add(Stage)}.
    public CloseSequencer() {
    }

    /// Creates a sequencer from pre-declared, retryable stages.
    ///
    /// @param stages close stages, run in list order
    public CloseSequencer(final List<Stage> stages) {
        this.stages.addAll(List.copyOf(stages));
    }

    /// Adds one stage to the close graph.
    ///
    /// @param name    human-readable stage name used in diagnostics
    /// @param enabled whether the resource was initialized and must be closed
    /// @param action  the close action
    /// @return this sequencer
    public CloseSequencer add(final String name, final boolean enabled, final Runnable action) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(action, "action");
        return this.add(new Stage(name, () -> enabled, action::run));
    }

    /// Adds one stage with its readiness evaluated at run time.
    ///
    /// @param stage stage to append
    /// @return this sequencer
    public CloseSequencer add(final Stage stage) {
        this.stages.add(Objects.requireNonNull(stage, "stage"));
        return this;
    }

    /// Runs every ready stage in order and throws the prioritized aggregate.
    ///
    /// @param failureMessage context message for a wrapped checked failure
    public void run(final String failureMessage) {
        final Throwable failure = this.close();
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure != null) throw new NodeException(failureMessage, failure);
    }

    /// Runs every ready stage in order and returns the aggregated failure.
    ///
    /// @return the prioritized failure, or `null` when all ready stages succeeded
    public Throwable close() {
        Throwable fatal = null;
        Throwable failure = null;
        for (final Stage stage : this.stages) {
            if (!stage.ready().getAsBoolean()) continue;
            try {
                LOGGER.log(System.Logger.Level.TRACE, "Closing stage '%s'".formatted(stage.name()));
                stage.action().run();
            } catch (final Throwable stageFailure) {
                if (stageFailure instanceof Error error) {
                    if (fatal == null) fatal = error;
                    else if (fatal != error) fatal.addSuppressed(error);
                } else if (failure == null) {
                    failure = stageFailure;
                } else if (failure != stageFailure) {
                    failure.addSuppressed(stageFailure);
                }
                LOGGER.log(DEBUG,
                        "Close stage '%s' failed".formatted(stage.name()), stageFailure);
            }
        }
        if (fatal != null) {
            if (failure != null) fatal.addSuppressed(failure);
            return fatal;
        }
        return failure;
    }

    /// Appends one close failure to an aggregation with [Error] priority.
    ///
    /// Shared by close loops that are not expressed as stages (for example a
    /// manager close with retry-dependent ordering): the first [Error] is
    /// kept apart from ordinary failures and always wins when the aggregate
    /// is finally surfaced, so a fatal condition is never suppressed beneath
    /// a [RuntimeException].
    ///
    /// @param current    aggregated failure so far, or `null`
    /// @param additional failure to append, or `null` to ignore
    /// @return updated aggregate, or `null` when both are `null`
    public static Throwable append(final Throwable current, final Throwable additional) {
        if (additional == null) return current;
        if (additional instanceof Error) {
            if (current instanceof Error currentError) {
                if (currentError != additional) currentError.addSuppressed(additional);
                return currentError;
            }
            if (current != null) additional.addSuppressed(current);
            return additional;
        }
        if (current == null) return additional;
        if (current != additional) current.addSuppressed(additional);
        return current;
    }

    /// One named close step.
    ///
    /// @param name  diagnostic stage name
    /// @param ready precondition checked immediately before the step runs; a
    ///              retry skips every step whose resource is already released
    /// @param action release action
    public record Stage(String name, BooleanSupplier ready, Action action) {
        /// Validates the stage parts.
        public Stage {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(ready, "ready");
            Objects.requireNonNull(action, "action");
        }
    }

    /// A release action that may report a checked failure to the sequencer.
    @FunctionalInterface
    public interface Action {
        /// Runs the release step.
        ///
        /// @throws Throwable when the release fails
        void run() throws Throwable;
    }

    /// Builds one stage.
    ///
    /// @param name   diagnostic stage name
    /// @param ready  precondition evaluated at run time
    /// @param action release action
    /// @return close stage
    public static Stage stage(final String name, final BooleanSupplier ready, final Action action) {
        return new Stage(name, ready, action);
    }
}
