package peruncs.datagrid.cluster.node.aeron;

import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/// Ordered, retryable close sequencer for transports that own several
/// dependent resources.
///
/// Stages run in declaration order and a stage whose [#ready()] precondition is
/// false is skipped. A failed stage leaves its resource in place, so a retried
/// close re-runs only the stages still owing work instead of tearing down
/// beneath live threads. Failures from independent stages are aggregated as
/// suppressed exceptions instead of masking one another.
final class CloseSequencer {
    /// One named close step.
    ///
    /// @param name  diagnostic stage name
    /// @param ready precondition checked immediately before the step runs; a
    ///              retry skips every step whose resource is already released
    /// @param action release action
    record Stage(String name, BooleanSupplier ready, Action action) {
        Stage {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(ready, "ready");
            Objects.requireNonNull(action, "action");
        }
    }

    /// A release action that may report a checked failure to the sequencer.
    @FunctionalInterface
    interface Action {
        void run() throws Throwable;
    }

    private final List<Stage> stages;

    CloseSequencer(final List<Stage> stages) {
        this.stages = List.copyOf(stages);
    }

    /// Runs every ready stage in order.
    ///
    /// @return aggregated failure, or `null` when all ready stages succeeded
    Throwable close() {
        Throwable failure = null;
        for (final Stage stage : this.stages) {
            if (!stage.ready().getAsBoolean()) continue;
            try {
                stage.action().run();
            } catch (final Throwable stageFailure) {
                failure = append(failure, stageFailure);
            }
        }
        return failure;
    }

    private static Throwable append(final Throwable current, final Throwable additional) {
        if (additional == null) return current;
        if (current == null) return additional;
        if (current != additional) current.addSuppressed(additional);
        return current;
    }

    /// Builds one stage.
    ///
    /// @param name   diagnostic stage name
    /// @param ready  precondition
    /// @param action release action
    /// @return close stage
    static Stage stage(final String name, final BooleanSupplier ready, final Action action) {
        return new Stage(name, ready, action);
    }
}
