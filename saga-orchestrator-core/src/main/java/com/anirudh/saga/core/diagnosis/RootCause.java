package com.anirudh.saga.core.diagnosis;

/**
 * Heuristic classification of why a saga is stuck (SUSPENDED or FAILED).
 * Returned to operators via {@code GET /sagas/{sagaId}} (P2-038 / E2-25).
 *
 * <p>Values are deliberately few: 4 known causes + an UNKNOWN fallback.
 * No ML, no scoring — just string-pattern matching against the saga's
 * execution log. If the heuristic doesn't fire, operators see
 * {@link #UNKNOWN} with the raw reason text and a suggested
 * {@code INVESTIGATE} action.
 */
public enum RootCause {

    /** Kafka step retries exhausted → DLT → SUSPENDED. */
    DLT_EXHAUSTED,

    /**
     * A step exceeded its declared {@code timeoutSeconds} budget. Maps from
     * the v0.1 per-step-timeout suspension (P2-008); saga-level timeouts
     * currently auto-compensate so they never appear here.
     */
    SAGA_TIMEOUT,

    /** HTTP step returned a technical failure that wasn't retryable. */
    TECHNICAL_FAILURE,

    /** A compensation step itself threw — saga ended in FAILED, not SUSPENDED. */
    COMPENSATION_FAILED,

    /** Heuristic did not match a known cause; operator must investigate the timeline. */
    UNKNOWN
}
