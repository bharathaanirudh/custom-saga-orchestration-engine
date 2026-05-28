package com.anirudh.saga.core.diagnosis;

import com.anirudh.saga.core.audit.SagaExecutionLog;
import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Heuristic root-cause analyzer for stuck sagas (P2-038 / E2-25).
 *
 * <p>Pure function over {@link SagaInstance} state + the saga's
 * {@link SagaExecutionLog} timeline. No new collections, no extra queries
 * beyond what the controller already loads. Caller passes the timeline
 * ordered oldest → newest.
 *
 * <p><strong>What it doesn't do:</strong> ML, scoring, multi-signal fusion.
 * Just plain string-pattern matching against reason text. When the heuristic
 * can't classify, returns {@link RootCause#UNKNOWN} with {@link SuggestedAction#INVESTIGATE}
 * so the operator can still see the raw timeline. The goal is fast triage,
 * not a complete diagnostic engine.
 */
@Service
public class SagaDiagnosisService {

    // Pattern: "Step <name> exceeded <N>s timeout" (from StepTimeoutScheduler).
    private static final Pattern STEP_TIMEOUT_REASON =
            Pattern.compile(".*exceeded \\d+s timeout.*", Pattern.DOTALL);

    /**
     * Build a {@link Diagnosis} when the saga is in a status that requires
     * operator attention ({@code SUSPENDED} or {@code FAILED}). Returns
     * {@code Optional.empty()} for healthy or in-flight statuses — diagnosis
     * isn't meaningful mid-execution.
     *
     * @param instance the saga instance
     * @param timeline its execution log entries, ordered oldest → newest
     */
    public Optional<Diagnosis> diagnose(SagaInstance instance, List<SagaExecutionLog> timeline) {
        SagaStatus status = instance.getStatus();
        if (status != SagaStatus.SUSPENDED && status != SagaStatus.FAILED) {
            return Optional.empty();
        }

        String lastSuccessfulStep = findLastEvent(timeline, "STEP_COMPLETED");
        String failedStep = findLastEvent(timeline, "STEP_STARTED");

        if (status == SagaStatus.FAILED) {
            // FAILED implies compensation failure (see SagaStateMachine.failCompensationStep).
            // Pull the failed-compensation step name when available.
            String failedCompensation = instance.getFailedCompensations() != null
                    && !instance.getFailedCompensations().isEmpty()
                    ? instance.getFailedCompensations().get(0)
                    : findLastEvent(timeline, "COMPENSATION_STEP_FAILED");
            String reason = findLastEventData(timeline, "COMPENSATION_STEP_FAILED");
            return Optional.of(new Diagnosis(
                    RootCause.COMPENSATION_FAILED,
                    SuggestedAction.MANUAL_INTERVENTION,
                    reason,
                    lastSuccessfulStep,
                    failedCompensation));
        }

        // SUSPENDED — classify by the suspension reason string.
        String reason = findLastEventData(timeline, "SUSPENDED");
        RootCause cause = classifySuspensionReason(reason);
        SuggestedAction action = mapAction(cause);
        return Optional.of(new Diagnosis(cause, action, reason, lastSuccessfulStep, failedStep));
    }

    private RootCause classifySuspensionReason(String reason) {
        if (reason == null) return RootCause.UNKNOWN;
        if (STEP_TIMEOUT_REASON.matcher(reason).matches()) return RootCause.SAGA_TIMEOUT;
        if (reason.startsWith("Technical failure at HTTP step:")) return RootCause.TECHNICAL_FAILURE;
        if (reason.startsWith("Technical failure at step:")) return RootCause.DLT_EXHAUSTED;
        return RootCause.UNKNOWN;
    }

    private SuggestedAction mapAction(RootCause cause) {
        return switch (cause) {
            case DLT_EXHAUSTED, TECHNICAL_FAILURE -> SuggestedAction.RETRY;
            case SAGA_TIMEOUT -> SuggestedAction.COMPENSATE;
            case COMPENSATION_FAILED -> SuggestedAction.MANUAL_INTERVENTION;
            case UNKNOWN -> SuggestedAction.INVESTIGATE;
        };
    }

    /** Returns the stepName of the most recent log entry with the given event, or null. */
    private String findLastEvent(List<SagaExecutionLog> timeline, String event) {
        for (int i = timeline.size() - 1; i >= 0; i--) {
            SagaExecutionLog e = timeline.get(i);
            if (event.equals(e.getEvent())) return e.getStepName();
        }
        return null;
    }

    /** Returns the data field of the most recent log entry with the given event, or null. */
    private String findLastEventData(List<SagaExecutionLog> timeline, String event) {
        for (int i = timeline.size() - 1; i >= 0; i--) {
            SagaExecutionLog e = timeline.get(i);
            if (event.equals(e.getEvent())) return e.getData();
        }
        return null;
    }
}
