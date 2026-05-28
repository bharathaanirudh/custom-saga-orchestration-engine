package com.anirudh.saga.core.audit;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Read-only result of replaying a saga's event history (P2-017 / E2-11).
 *
 * @param sagaId             the saga replayed
 * @param replayedUpTo       cutoff timestamp; null = full history
 * @param eventsApplied      number of events folded into the reconstruction
 * @param lastEventAt        timestamp of the last event applied (null if none)
 * @param reconstructedState saga state derived from event snapshots alone (independent of the live instance)
 * @param currentState       the live persisted SagaInstance state, or null if absent/corrupted
 * @param differences        human-readable field-level diffs between reconstructed and current; empty if they match
 */
public record ReplayResult(
        String sagaId,
        Instant replayedUpTo,
        int eventsApplied,
        Instant lastEventAt,
        Map<String, Object> reconstructedState,
        Map<String, Object> currentState,
        List<String> differences
) {}
