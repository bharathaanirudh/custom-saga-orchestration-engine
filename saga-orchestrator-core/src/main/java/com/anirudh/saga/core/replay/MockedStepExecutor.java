package com.anirudh.saga.core.replay;

import java.util.Map;

/**
 * Resolves a step's outcome during replay (P2-027) from the outcomes reconstructed off the
 * original event log — instead of invoking a real participant. This is what makes replay
 * side-effect-free: no Kafka publish, no HTTP call, no Mongo write.
 *
 * <p>Steps present in the new definition but absent from the original run resolve to
 * {@link StepOutcome#UNKNOWN} (the operator added them) — the simulator assumes SUCCESS for
 * routing but the diff flags them.
 */
public class MockedStepExecutor {

    private final Map<String, StepOutcome> originalOutcomes;

    public MockedStepExecutor(Map<String, StepOutcome> originalOutcomes) {
        this.originalOutcomes = originalOutcomes;
    }

    public StepOutcome outcomeFor(String stepName) {
        return originalOutcomes.getOrDefault(stepName, StepOutcome.UNKNOWN);
    }
}
