package com.anirudh.saga.core.replay;

import java.util.List;
import java.util.Map;

/**
 * Result of replaying one historical saga against a new definition (P2-027 — "time machine").
 * Read-only simulation: no Mongo writes, no Kafka publish.
 *
 * @param sagaId                the historical saga replayed
 * @param definition            the target definition name it was replayed against
 * @param originalOutcomesByStep per-step outcomes reconstructed from the original event log
 * @param originalFinalStatus    terminal status of the original run
 * @param replayedTimeline       simulated step-by-step trajectory against the new definition
 * @param replayedFinalStatus    simulated terminal status under the new definition
 * @param diff                   structural + behavioral differences
 */
public record ReplayReport(
        String sagaId,
        String definition,
        Map<String, StepOutcome> originalOutcomesByStep,
        String originalFinalStatus,
        List<SimulatedStep> replayedTimeline,
        String replayedFinalStatus,
        ReplayDiff diff
) {
    /** One step's simulated execution under the new definition. */
    public record SimulatedStep(String stepName, String type, String action, StepOutcome appliedOutcome) {}

    /**
     * @param stepsAdded   steps in the new definition not present in the original run
     * @param stepsRemoved steps in the original run not present in the new definition
     * @param reordered    true if shared steps appear in a different relative order
     * @param divergencePoint step name where the simulated trajectory first diverged, or null if identical outcome
     * @param finalStatusChanged true if replayed final status differs from original
     */
    public record ReplayDiff(
            List<String> stepsAdded,
            List<String> stepsRemoved,
            boolean reordered,
            String divergencePoint,
            boolean finalStatusChanged
    ) {}
}
