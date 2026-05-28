package com.anirudh.saga.core.diagnosis;

/**
 * What the on-call developer should do next, given the {@link RootCause}.
 * Returned alongside diagnosis (P2-038 / E2-25). Action mapping is fixed:
 *
 * <ul>
 *   <li>{@link RootCause#DLT_EXHAUSTED} → {@link #RETRY}</li>
 *   <li>{@link RootCause#SAGA_TIMEOUT} → {@link #COMPENSATE}</li>
 *   <li>{@link RootCause#TECHNICAL_FAILURE} → {@link #RETRY} (most similar to DLT)</li>
 *   <li>{@link RootCause#COMPENSATION_FAILED} → {@link #MANUAL_INTERVENTION}</li>
 *   <li>{@link RootCause#UNKNOWN} → {@link #INVESTIGATE}</li>
 * </ul>
 */
public enum SuggestedAction {

    /** {@code POST /sagas/{id}/retry} — re-dispatch the failed step. */
    RETRY,

    /** {@code POST /sagas/{id}/compensate} — give up forward progress, undo what shipped. */
    COMPENSATE,

    /** Operator must inspect compensation state and decide; engine cannot proceed safely. */
    MANUAL_INTERVENTION,

    /** Pattern matcher didn't fire — read the timeline + logs to classify by hand. */
    INVESTIGATE
}
