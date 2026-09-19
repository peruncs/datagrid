package peruncs.datagrid.cluster.node.aeron;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the ordered retryable close sequencer.
class CloseSequencerTest {
        /// Verifies ready stages run in declaration order and unready stages are skipped.
    @Test
    void stagesRunInOrderAndSkipWhenNotReady() {
        final List<String> ran = new ArrayList<>();
        final boolean[] ready = {true, true, true};
        final CloseSequencer sequencer = new CloseSequencer(List.of(
                CloseSequencer.stage("first", () -> ready[0], () -> ran.add("first")),
                CloseSequencer.stage("second", () -> ready[1], () -> ran.add("second")),
                CloseSequencer.stage("third", () -> ready[2], () -> ran.add("third"))));
        assertNull(sequencer.close());
        assertEquals(List.of("first", "second", "third"), ran);

        ran.clear();
        ready[1] = false;
        assertNull(sequencer.close());
        assertEquals(List.of("first", "third"), ran, "an already released stage must be skipped");
    }

        /// Verifies a failed stage leaves later retries to run only the stages still owing work.
    @Test
    void failedStageStaysRetryableAndLaterStagesStillRun() {
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicBoolean released = new AtomicBoolean();
        final CloseSequencer sequencer = new CloseSequencer(List.of(
                CloseSequencer.stage("flaky", () -> !released.get(), () -> {
                    if (attempts.incrementAndGet() == 1) throw new IllegalStateException("first attempt fails");
                    released.set(true);
                }),
                CloseSequencer.stage("after", () -> true, () -> {
                })));
        final Throwable firstFailure = sequencer.close();
        assertNotNull(firstFailure);
        assertEquals("first attempt fails", firstFailure.getMessage());
        assertNull(sequencer.close(), "a retry must finish the failed stage");
        assertEquals(2, attempts.get());
    }

        /// Verifies failures from independent stages are aggregated as suppressed exceptions.
    @Test
    void independentStageFailuresAreAggregated() {
        final CloseSequencer sequencer = new CloseSequencer(List.of(
                CloseSequencer.stage("one", () -> true, () -> {
                    throw new IllegalStateException("one failed");
                }),
                CloseSequencer.stage("two", () -> true, () -> {
                    throw new IllegalStateException("two failed");
                })));
        final Throwable failure = sequencer.close();
        assertNotNull(failure);
        assertEquals("one failed", failure.getMessage());
        assertEquals(1, failure.getSuppressed().length);
        assertEquals("two failed", failure.getSuppressed()[0].getMessage());
    }

        /// Verifies a checked failure is reported rather than swallowed.
    @Test
    void checkedStageFailureIsReported() {
        final CloseSequencer sequencer = new CloseSequencer(List.of(
                CloseSequencer.stage("checked", () -> true, () -> {
                    throw new java.io.IOException("checked failure");
                })));
        final Throwable failure = sequencer.close();
        assertInstanceOf(java.io.IOException.class, failure);
    }
}
