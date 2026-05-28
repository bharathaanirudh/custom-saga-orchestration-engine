package com.anirudh.saga.core.diagnosis;

/**
 * Structured triage record for a stuck saga (P2-038 / E2-25).
 *
 * <p>Returned as the {@code diagnosis} field of {@code GET /sagas/{sagaId}}
 * when the saga is in a status that requires operator attention
 * ({@code SUSPENDED} or {@code FAILED}). For healthy or in-flight sagas the
 * field is absent — diagnosis is meaningless mid-execution.
 *
 * @param rootCause          Heuristic classification of why the saga is stuck
 * @param suggestedAction    What the operator should do next
 * @param lastError          Raw reason text from the suspension/failure log entry, if any
 * @param lastSuccessfulStep Name of the last step that completed before things went wrong, or {@code null} if none
 * @param failedStep         Name of the step that triggered the suspension/failure, or {@code null} if not identifiable
 */
public record Diagnosis(
        RootCause rootCause,
        SuggestedAction suggestedAction,
        String lastError,
        String lastSuccessfulStep,
        String failedStep
) {}
