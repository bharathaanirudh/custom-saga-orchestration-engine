package com.anirudh.saga.core.replay;

import java.util.List;

/**
 * Aggregate result of bulk-replaying many historical sagas against a new definition (P2-027 AC-4).
 *
 * @param definition       target definition replayed against
 * @param sagaType         filter applied
 * @param replayed         number of sagas replayed (capped at {@code CAP})
 * @param sameFinalStatus  count whose simulated final status matched the original
 * @param diverged         count whose simulated final status differed
 * @param divergedSagaIds  the sagaIds that diverged (for drill-down)
 */
public record BulkReplayReport(
        String definition,
        String sagaType,
        int replayed,
        int sameFinalStatus,
        int diverged,
        List<String> divergedSagaIds
) {
    /** Max sagas processed per bulk call (AC-4). */
    public static final int CAP = 1000;
}
