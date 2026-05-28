package com.anirudh.saga.core.scheduler;

import com.anirudh.saga.core.domain.SagaDefinition;
import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;
import com.anirudh.saga.core.domain.StepDefinition;
import com.anirudh.saga.core.engine.SagaStateMachine;
import com.anirudh.saga.core.loader.SagaDefinitionLoader;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

/**
 * Per-step timeout detection (P2-008).
 *
 * On each tick, finds IN_PROGRESS sagas whose current step has been running longer
 * than the step's {@code timeoutSeconds}. v0.1 scope: timed-out steps cause the
 * saga to transition to SUSPENDED — no automatic re-dispatch (deferred to v1.0
 * after participant-side compensation idempotency lands; see EXECUTION-PLAN §11).
 */
@Component
public class StepTimeoutScheduler {

    private static final Logger log = LoggerFactory.getLogger(StepTimeoutScheduler.class);

    private final SagaInstanceRepository repository;
    private final SagaDefinitionLoader definitions;
    private final SagaStateMachine stateMachine;
    private final Clock clock;

    public StepTimeoutScheduler(SagaInstanceRepository repository,
                                SagaDefinitionLoader definitions,
                                SagaStateMachine stateMachine,
                                Clock clock) {
        this.repository = repository;
        this.definitions = definitions;
        this.stateMachine = stateMachine;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${saga.timeout.poll-interval-ms:10000}")
    public void detectAndHandleTimeouts() {
        Instant now = Instant.now(clock);
        // 1-second floor on candidate query — anything younger than that can't have
        // exceeded any reasonable timeoutSeconds. Filtering in Java for the actual
        // per-step deadline since timeoutSeconds varies by definition.
        List<SagaInstance> candidates = repository.findStepCandidatesForTimeoutCheck(now.minusSeconds(1));
        if (candidates.isEmpty()) return;

        for (SagaInstance instance : candidates) {
            try {
                StepDefinition step = currentStep(instance);
                if (step == null) continue;
                if (isTimedOut(instance, step, now)) {
                    log.warn("[sagaId={}] Step {} exceeded {}s timeout — suspending",
                            instance.getSagaId(), step.name(), step.timeoutSeconds());
                    stateMachine.suspend(instance,
                            "Step '" + step.name() + "' exceeded " + step.timeoutSeconds() + "s timeout");
                }
            } catch (Exception e) {
                log.error("[sagaId={}] StepTimeoutScheduler error: {}", instance.getSagaId(), e.getMessage());
            }
        }
    }

    /** Visible for testing — pure function over instance + step + now. */
    public boolean isTimedOut(SagaInstance instance, StepDefinition step, Instant now) {
        if (instance.getStatus() != SagaStatus.IN_PROGRESS) return false;
        Instant startedAt = instance.getCurrentStepStartedAt();
        if (startedAt == null) return false;
        if (step.timeoutSeconds() <= 0) return false; // 0 means no per-step timeout
        return startedAt.plusSeconds(step.timeoutSeconds()).isBefore(now);
    }

    private StepDefinition currentStep(SagaInstance instance) {
        SagaDefinition def = definitions.getDefinition(instance.getSagaType());
        if (def == null) return null;
        int idx = instance.getCurrentStep();
        if (idx < 0 || idx >= def.steps().size()) return null;
        return def.steps().get(idx);
    }
}
