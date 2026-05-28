package com.anirudh.saga.core.scheduler;

import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.engine.SagaOrchestrator;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

@Component
public class SagaTimeoutScheduler {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutScheduler.class);

    private final SagaInstanceRepository repository;
    private final SagaOrchestrator orchestrator;
    private final Clock clock;

    public SagaTimeoutScheduler(SagaInstanceRepository repository,
                                 SagaOrchestrator orchestrator, Clock clock) {
        this.repository = repository;
        this.orchestrator = orchestrator;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${saga.timeout.poll-interval-ms:60000}")
    public void checkTimeouts() {
        Instant now = Instant.now(clock);
        List<SagaInstance> timedOut = repository.findTimedOut(now);
        if (timedOut.isEmpty()) return;

        log.warn("SagaTimeoutScheduler found {} timed-out sagas", timedOut.size());
        for (SagaInstance instance : timedOut) {
            try {
                log.warn("[sagaId={}] Saga timed out — triggering compensation", instance.getSagaId());
                orchestrator.triggerCompensation(instance.getSagaId());
            } catch (Exception e) {
                log.error("[sagaId={}] Failed to trigger timeout compensation: {}",
                        instance.getSagaId(), e.getMessage());
            }
        }
    }
}
