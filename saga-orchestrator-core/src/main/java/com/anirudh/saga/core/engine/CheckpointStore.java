package com.anirudh.saga.core.engine;

import com.anirudh.saga.core.audit.SagaExecutionLog;
import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.repository.SagaExecutionLogRepository;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Component
public class CheckpointStore {

    private static final Logger log = LoggerFactory.getLogger(CheckpointStore.class);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaExecutionLogRepository executionLogRepository;
    private final Clock clock;

    public CheckpointStore(SagaInstanceRepository sagaInstanceRepository,
                           SagaExecutionLogRepository executionLogRepository,
                           Clock clock) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.executionLogRepository = executionLogRepository;
        this.clock = clock;
    }

    public SagaInstance save(SagaInstance instance) {
        instance.touch(Instant.now(clock));
        return sagaInstanceRepository.save(instance);
    }

    /** Append an event with no state snapshot (actor=ENGINE). For call sites without a SagaInstance in scope. */
    public void logEvent(String sagaId, String stepName, String event, String data) {
        SagaExecutionLog entry = SagaExecutionLog.of(sagaId, stepName, event, data, Instant.now(clock));
        executionLogRepository.save(entry);
        log.info("[sagaId={}] Event logged: step={} event={}", sagaId, stepName, event);
    }

    /**
     * Append an event WITH a snapshot of the saga's state at this point (P2-016).
     * The snapshot is what makes the log replay-grade — P2-017 reconstructs state from it.
     */
    public void logEvent(SagaInstance instance, String stepName, String event, String data) {
        SagaExecutionLog entry = SagaExecutionLog.of(
                instance.getSagaId(), stepName, event, data, Instant.now(clock), "ENGINE", snapshot(instance));
        executionLogRepository.save(entry);
        log.info("[sagaId={}] Event logged: step={} event={}", instance.getSagaId(), stepName, event);
    }

    /** Capture the replay-relevant state of a saga as a plain map (stored as a nested Mongo document). */
    private Map<String, Object> snapshot(SagaInstance instance) {
        Map<String, Object> snap = new HashMap<>();
        snap.put("status", instance.getStatus() != null ? instance.getStatus().name() : null);
        snap.put("currentStep", instance.getCurrentStep());
        snap.put("context", new HashMap<>(instance.getContext()));
        snap.put("failedCompensations", instance.getFailedCompensations());
        snap.put("timeoutAt", instance.getTimeoutAt());
        snap.put("currentStepStartedAt", instance.getCurrentStepStartedAt());
        snap.put("version", instance.getVersion());
        return snap;
    }
}
