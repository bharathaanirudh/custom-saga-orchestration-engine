package com.anirudh.saga.core.audit;

import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.repository.SagaExecutionLogRepository;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Reconstructs a saga's state from its event history (P2-017 / E2-11).
 *
 * <p><b>Read-only.</b> Never touches the live {@link SagaInstance}. Reconstruction is
 * snapshot-based: P2-016 captures an {@code instanceSnapshot} on every state transition,
 * so the snapshot of the latest event (≤ the optional cutoff) IS the folded state at that
 * point — no re-derivation of business logic needed, and correct even if the live instance
 * is corrupted or deleted (AC-4).
 *
 * <p>Events without a snapshot (e.g. ones written via the no-instance {@code logEvent}
 * overload) contribute to the event count but not to state — the last snapshot-bearing
 * event wins.
 */
@Service
public class SagaReplayService {

    private final SagaExecutionLogRepository logRepository;
    private final SagaInstanceRepository instanceRepository;

    public SagaReplayService(SagaExecutionLogRepository logRepository,
                             SagaInstanceRepository instanceRepository) {
        this.logRepository = logRepository;
        this.instanceRepository = instanceRepository;
    }

    /**
     * @param sagaId saga to replay
     * @param upTo   optional cutoff — reconstruct state as of this instant; null = full history
     */
    public ReplayResult replay(String sagaId, Instant upTo) {
        List<SagaExecutionLog> events = logRepository.findBySagaIdOrderByTimestampAsc(sagaId).stream()
                .filter(e -> upTo == null || !e.getTimestamp().isAfter(upTo))
                .toList();

        Map<String, Object> reconstructed = null;
        Instant lastEventAt = null;
        for (SagaExecutionLog e : events) {
            lastEventAt = e.getTimestamp();
            if (e.getInstanceSnapshot() != null) {
                reconstructed = e.getInstanceSnapshot(); // last snapshot ≤ cutoff wins
            }
        }

        Map<String, Object> current = instanceRepository.findBySagaId(sagaId)
                .map(SagaReplayService::toStateMap)
                .orElse(null);

        List<String> diffs = diff(reconstructed, current);

        return new ReplayResult(sagaId, upTo, events.size(), lastEventAt, reconstructed, current, diffs);
    }

    /** Same projection shape used by {@code CheckpointStore.snapshot()} so reconstructed ↔ current compare cleanly. */
    static Map<String, Object> toStateMap(SagaInstance instance) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("status", instance.getStatus() != null ? instance.getStatus().name() : null);
        m.put("currentStep", instance.getCurrentStep());
        m.put("context", instance.getContext());
        m.put("failedCompensations", instance.getFailedCompensations());
        m.put("timeoutAt", instance.getTimeoutAt());
        m.put("currentStepStartedAt", instance.getCurrentStepStartedAt());
        m.put("version", instance.getVersion());
        return m;
    }

    private List<String> diff(Map<String, Object> reconstructed, Map<String, Object> current) {
        List<String> diffs = new ArrayList<>();
        if (reconstructed == null) {
            diffs.add("No snapshot-bearing events — cannot reconstruct state from history.");
            return diffs;
        }
        if (current == null) {
            diffs.add("Live SagaInstance is absent/corrupted — reconstructed state derives from events alone.");
            return diffs;
        }
        // Compare the keys reconstruction can speak to. `version` is excluded — it's a
        // Mongo optimistic-lock counter, not saga state, and drifts legitimately.
        for (String key : List.of("status", "currentStep", "context", "failedCompensations",
                "timeoutAt", "currentStepStartedAt")) {
            Object r = reconstructed.get(key);
            Object c = current.get(key);
            if (!Objects.equals(normalize(r), normalize(c))) {
                diffs.add(String.format("%s: reconstructed=%s, current=%s", key, r, c));
            }
        }
        return diffs;
    }

    /** Treat null and empty collection/map as equal so a fresh-vs-absent field isn't a false diff. */
    private Object normalize(Object v) {
        if (v instanceof Map<?, ?> m && m.isEmpty()) return null;
        if (v instanceof List<?> l && l.isEmpty()) return null;
        return v;
    }
}
