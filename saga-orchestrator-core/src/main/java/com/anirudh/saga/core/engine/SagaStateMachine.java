package com.anirudh.saga.core.engine;

import com.anirudh.saga.core.domain.*;
import com.anirudh.saga.core.exception.SagaInvalidStateException;
import com.anirudh.saga.core.metrics.SagaMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;

@Component
public class SagaStateMachine {

    private static final Logger log = LoggerFactory.getLogger(SagaStateMachine.class);

    private final CheckpointStore checkpointStore;
    private final SagaMetrics metrics;
    private final Clock clock;

    public SagaStateMachine(CheckpointStore checkpointStore, SagaMetrics metrics, Clock clock) {
        this.checkpointStore = checkpointStore;
        this.metrics = metrics;
        this.clock = clock;
    }

    public SagaInstance initialize(SagaInstance instance) {
        log.info("[sagaId={}] Transition CREATED -> STARTED", instance.getSagaId());
        instance.setStatus(SagaStatus.STARTED);
        checkpointStore.logEvent(instance,"SAGA", "STARTED", null);
        metrics.recordStarted(instance);
        return checkpointStore.save(instance);
    }

    public SagaInstance startStep(SagaInstance instance, StepDefinition step) {
        log.info("[sagaId={}] Transition {} -> IN_PROGRESS step={}", instance.getSagaId(), instance.getStatus(), step.name());
        instance.setStatus(SagaStatus.IN_PROGRESS);
        instance.setCurrentStepStartedAt(Instant.now(clock));
        checkpointStore.logEvent(instance,step.name(), "STEP_STARTED", null);
        metrics.recordStepExecuted(instance, step);
        return checkpointStore.save(instance);
    }

    public SagaInstance completeStep(SagaInstance instance, StepDefinition step, Object result, SagaDefinition definition) {
        log.info("[sagaId={}] Step {} completed", instance.getSagaId(), step.name());
        if (result != null) {
            instance.getContext().put(step.name(), result);
        }
        instance.setCurrentStep(instance.getCurrentStep() + 1);
        instance.setCurrentStepStartedAt(null);
        checkpointStore.logEvent(instance,step.name(), "STEP_COMPLETED", null);

        if (instance.getCurrentStep() >= definition.steps().size()) {
            instance.setStatus(SagaStatus.COMPLETED);
            log.info("[sagaId={}] Transition IN_PROGRESS -> COMPLETED", instance.getSagaId());
            checkpointStore.logEvent(instance,"SAGA", "COMPLETED", null);
            metrics.recordCompleted(instance);
        }
        return checkpointStore.save(instance);
    }

    public SagaInstance startCompensation(SagaInstance instance, String reason) {
        log.info("[sagaId={}] Transition {} -> COMPENSATING reason={}", instance.getSagaId(), instance.getStatus(), reason);
        instance.setStatus(SagaStatus.COMPENSATING);
        checkpointStore.logEvent(instance,"SAGA", "COMPENSATION_STARTED", reason);
        return checkpointStore.save(instance);
    }

    public SagaInstance completeCompensationStep(SagaInstance instance, StepDefinition step) {
        log.info("[sagaId={}] Compensation step {} completed", instance.getSagaId(), step.name());
        instance.setCurrentStep(instance.getCurrentStep() - 1);
        instance.setCurrentStepStartedAt(null);
        checkpointStore.logEvent(instance,step.name(), "COMPENSATION_STEP_COMPLETED", null);
        return checkpointStore.save(instance);
    }

    public SagaInstance failCompensationStep(SagaInstance instance, StepDefinition step, String reason) {
        log.warn("[sagaId={}] Compensation step {} failed: {}", instance.getSagaId(), step.name(), reason);
        instance.addFailedCompensation(step.name());
        checkpointStore.logEvent(instance,step.name(), "COMPENSATION_STEP_FAILED", reason);
        // Compensation-step failure is technical by definition (the participant's
        // compensation handler threw). Forward-step failure tagging is a P2-038
        // hardening concern — see EXECUTION-PLAN §11.
        metrics.recordStepFailed(instance, step, com.anirudh.saga.sdk.contract.FailureType.TECHNICAL);
        return checkpointStore.save(instance);
    }

    public SagaInstance finishCompensation(SagaInstance instance) {
        if (instance.getFailedCompensations().isEmpty()) {
            instance.setStatus(SagaStatus.COMPENSATED);
            log.info("[sagaId={}] Transition COMPENSATING -> COMPENSATED", instance.getSagaId());
            checkpointStore.logEvent(instance,"SAGA", "COMPENSATED", null);
            metrics.recordCompensated(instance);
        } else {
            instance.setStatus(SagaStatus.FAILED);
            log.error("[sagaId={}] Transition COMPENSATING -> FAILED failedSteps={}", instance.getSagaId(), instance.getFailedCompensations());
            checkpointStore.logEvent(instance,"SAGA", "FAILED", "Failed compensations: " + instance.getFailedCompensations());
            metrics.recordFailed(instance);
        }
        return checkpointStore.save(instance);
    }

    public SagaInstance suspend(SagaInstance instance, String reason) {
        log.warn("[sagaId={}] Transition {} -> SUSPENDED reason={}", instance.getSagaId(), instance.getStatus(), reason);
        instance.setStatus(SagaStatus.SUSPENDED);
        instance.setSuspendedAt(Instant.now(clock));
        checkpointStore.logEvent(instance,"SAGA", "SUSPENDED", reason);
        metrics.recordSuspended(instance);
        return checkpointStore.save(instance);
    }

    public void validateForRetry(SagaInstance instance) {
        if (instance.getStatus() != SagaStatus.SUSPENDED) {
            throw new SagaInvalidStateException(
                    "Cannot retry saga [" + instance.getSagaId() + "] in status: " + instance.getStatus());
        }
    }

    public void validateForCompensation(SagaInstance instance) {
        if (instance.getStatus() == SagaStatus.COMPLETED ||
                instance.getStatus() == SagaStatus.COMPENSATED ||
                instance.getStatus() == SagaStatus.FAILED) {
            throw new SagaInvalidStateException(
                    "Cannot compensate saga [" + instance.getSagaId() + "] in status: " + instance.getStatus());
        }
    }
}
