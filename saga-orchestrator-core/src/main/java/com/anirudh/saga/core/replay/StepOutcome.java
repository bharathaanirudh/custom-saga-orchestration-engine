package com.anirudh.saga.core.replay;

/**
 * The outcome of a step in the *original* saga run, reconstructed from the event log
 * (P2-027). Drives the sandboxed BTFC simulation against a new definition.
 */
public enum StepOutcome {
    /** Step completed (STEP_COMPLETED in the original log). */
    SUCCESS,
    /** Step got a BUSINESS failure (original log shows compensation started for it). */
    BUSINESS_FAILURE,
    /** Step got a TECHNICAL failure (original log shows suspension for it). */
    TECHNICAL_FAILURE,
    /** Step exists in the new definition but not the original run — outcome can't be known. */
    UNKNOWN
}
