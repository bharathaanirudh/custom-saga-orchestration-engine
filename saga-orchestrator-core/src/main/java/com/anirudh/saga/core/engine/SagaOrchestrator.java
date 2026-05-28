package com.anirudh.saga.core.engine;

import com.anirudh.saga.core.domain.*;
import com.anirudh.saga.core.executor.StepExecutorRegistry;
import com.anirudh.saga.core.executor.StepResult;
import com.anirudh.saga.core.metrics.SagaMetrics;
import com.anirudh.saga.core.exception.IdempotencyPayloadMismatchException;
import com.anirudh.saga.core.exception.PayloadTooLargeException;
import com.anirudh.saga.core.exception.SagaExecutionException;
import com.anirudh.saga.core.exception.SagaNotFoundException;
import com.anirudh.saga.core.loader.SagaDefinitionLoader;
import com.anirudh.saga.core.lock.SagaLockManager;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import com.anirudh.saga.sdk.contract.SagaStartRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

@Component
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    private final SagaDefinitionLoader definitionLoader;
    private final SagaInstanceRepository repository;
    private final SagaStateMachine stateMachine;
    private final IdempotencyGuard idempotencyGuard;
    private final StepExecutorRegistry executorRegistry;
    private final CheckpointStore checkpointStore;
    private final SagaLockManager lockManager;
    private final Clock clock;
    private final ObjectMapper objectMapper;
    private final SagaMetrics metrics;
    private final long maxPayloadBytes;

    public SagaOrchestrator(SagaDefinitionLoader definitionLoader,
                             SagaInstanceRepository repository,
                             SagaStateMachine stateMachine,
                             IdempotencyGuard idempotencyGuard,
                             StepExecutorRegistry executorRegistry,
                             CheckpointStore checkpointStore,
                             SagaLockManager lockManager,
                             Clock clock,
                             ObjectMapper objectMapper,
                             SagaMetrics metrics,
                             @Value("${saga.payload.max-bytes:1048576}") long maxPayloadBytes) {
        this.definitionLoader = definitionLoader;
        this.repository = repository;
        this.stateMachine = stateMachine;
        this.idempotencyGuard = idempotencyGuard;
        this.executorRegistry = executorRegistry;
        this.checkpointStore = checkpointStore;
        this.lockManager = lockManager;
        this.clock = clock;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public SagaInstance start(SagaStartRequest request) {
        // P2-061 AC-2: payload size cap at ingress, before any DB write.
        enforcePayloadSize(request.payload());

        SagaDefinition definition = definitionLoader.getDefinition(request.sagaType());

        String idempotencyKey = request.idempotencyKey() != null
                ? request.idempotencyKey()
                : idempotencyGuard.computeKey(request.sagaType(), request.payload());

        // P2-061 AC-1: payload-mismatch detection on duplicate idempotency key.
        String requestPayloadHash = idempotencyGuard.computePayloadHash(request.payload());

        Optional<SagaInstance> existing = idempotencyGuard.findExisting(idempotencyKey);
        if (existing.isPresent()) {
            assertPayloadHashMatches(existing.get(), requestPayloadHash, idempotencyKey);
            log.info("[sagaId={}] Idempotency hit — returning existing saga", existing.get().getSagaId());
            return existing.get();
        }

        SagaInstance instance = SagaInstance.create(
                request.sagaType(), request.payload(), idempotencyKey,
                definition.timeoutMinutes(), Instant.now(clock));
        instance.setIdempotencyPayloadHash(requestPayloadHash);

        instance = idempotencyGuard.saveOrGetExisting(instance);

        // §11 fix: TOCTOU between findExisting() and saveOrGetExisting() can return the
        // *winning* concurrent saga rather than ours. Recheck payload hash so concurrent
        // requests with mismatched payloads still get a 409 (not a silent advance).
        assertPayloadHashMatches(instance, requestPayloadHash, idempotencyKey);

        if (instance.getStatus() != SagaStatus.STARTED || instance.getCurrentStep() > 0) {
            log.info("[sagaId={}] Concurrent creation detected — returning winner's saga", instance.getSagaId());
            return instance;
        }

        // Acquire aggregate lock if defined in saga YAML
        acquireLockIfNeeded(instance, definition);

        instance = stateMachine.initialize(instance);
        log.info("[sagaId={}] Starting saga type={}", instance.getSagaId(), request.sagaType());
        dispatchStep(instance, definition);
        return instance;
    }

    private static boolean isTerminal(SagaStatus status) {
        return status == SagaStatus.COMPLETED
                || status == SagaStatus.COMPENSATED
                || status == SagaStatus.FAILED;
    }

    private void enforcePayloadSize(Map<String, Object> payload) {
        if (payload == null) return;
        try {
            long bytes = objectMapper.writeValueAsBytes(payload).length;
            if (bytes > maxPayloadBytes) {
                throw new PayloadTooLargeException(bytes, maxPayloadBytes);
            }
        } catch (JsonProcessingException e) {
            throw new SagaExecutionException("Failed to serialize saga payload for size check", e);
        }
    }

    private void assertPayloadHashMatches(SagaInstance existing, String requestHash, String idempotencyKey) {
        String storedHash = existing.getIdempotencyPayloadHash();
        // null storedHash = saga from before P2-061 shipped → skip check (graceful migration).
        if (storedHash != null && !storedHash.equals(requestHash)) {
            throw new IdempotencyPayloadMismatchException(idempotencyKey, existing.getSagaId());
        }
    }

    public void handleReply(String sagaId, String stepId, boolean success, String failureType, Object data) {
        // P2-061 AC-3: handle missing-saga and terminal-state replies as observability
        // signals (metrics + WARN log) rather than exceptions or silent advances.
        Optional<SagaInstance> maybeInstance = repository.findBySagaId(sagaId);
        if (maybeInstance.isEmpty()) {
            log.warn("Reply received for unknown sagaId={} stepId={} — ignoring", sagaId, stepId);
            metrics.recordUnknownReply();
            return;
        }
        SagaInstance instance = maybeInstance.get();
        if (isTerminal(instance.getStatus())) {
            log.warn("[sagaId={}] Late reply received for terminal saga (status={}, stepId={}) — ignoring",
                    sagaId, instance.getStatus(), stepId);
            metrics.recordLateReply(instance.getStatus().name());
            return;
        }
        SagaDefinition definition = definitionLoader.getDefinition(instance.getSagaType());

        int stepIndex = instance.getCurrentStep();
        if (stepIndex >= definition.steps().size()) {
            log.warn("[sagaId={}] Reply for already completed saga — ignoring", sagaId);
            metrics.recordLateReply(instance.getStatus().name());
            return;
        }

        StepDefinition currentStep = definition.steps().get(stepIndex);
        if (!currentStep.name().equals(stepId)) {
            log.warn("[sagaId={}] Reply stepId={} != current step={} — ignoring",
                    sagaId, stepId, currentStep.name());
            return;
        }

        if (success) {
            instance = stateMachine.completeStep(instance, currentStep, data, definition);
            if (instance.getStatus() == SagaStatus.COMPLETED) {
                releaseLock(instance);
            } else {
                dispatchStep(instance, definition);
            }
        } else if ("BUSINESS".equals(failureType)) {
            instance = stateMachine.startCompensation(instance, "Business failure at step: " + stepId);
            compensate(instance, definition);
        } else {
            applyFallbackOrSuspend(instance, currentStep, definition,
                    "Technical failure at step: " + stepId);
        }
    }

    public void retryFromSuspended(String sagaId) {
        SagaInstance instance = repository.findBySagaId(sagaId)
                .orElseThrow(() -> new SagaNotFoundException(sagaId));
        stateMachine.validateForRetry(instance);
        SagaDefinition definition = definitionLoader.getDefinition(instance.getSagaType());

        instance.setStatus(SagaStatus.IN_PROGRESS);
        instance = checkpointStore.save(instance);
        checkpointStore.logEvent(instance, "SAGA", "RETRY_FROM_SUSPENDED", null);
        dispatchStep(instance, definition);
    }

    public void triggerCompensation(String sagaId) {
        SagaInstance instance = repository.findBySagaId(sagaId)
                .orElseThrow(() -> new SagaNotFoundException(sagaId));
        stateMachine.validateForCompensation(instance);
        SagaDefinition definition = definitionLoader.getDefinition(instance.getSagaType());
        instance = stateMachine.startCompensation(instance, "Manual trigger");
        compensate(instance, definition);
    }

    private void dispatchStep(SagaInstance instance, SagaDefinition definition) {
        int stepIndex = instance.getCurrentStep();
        if (stepIndex >= definition.steps().size()) return;
        StepDefinition step = definition.steps().get(stepIndex);
        stateMachine.startStep(instance, step);

        StepResult result = executorRegistry.getExecutor(step.type()).execute(instance, step);

        if (result.isDispatched()) return; // Kafka — async reply via ReplyCorrelator

        // HTTP — handle inline
        if (result.isSuccess()) {
            instance = stateMachine.completeStep(instance, step, result.data(), definition);
            if (instance.getStatus() == SagaStatus.COMPLETED) {
                releaseLock(instance);
            } else {
                dispatchStep(instance, definition);
            }
        } else if (result.isBusinessFailure()) {
            instance = stateMachine.startCompensation(instance, "Business failure at HTTP step: " + step.name());
            compensate(instance, definition);
        } else {
            applyFallbackOrSuspend(instance, step, definition,
                    "Technical failure at HTTP step: " + step.name() + " — " + result.error());
        }
    }

    /**
     * P2-067: technical retries exhausted. If the step declares a {@code fallback},
     * write it into context and continue; otherwise suspend (legacy behavior). NEVER
     * called for business failures — BTFC streams stay separate.
     */
    private void applyFallbackOrSuspend(SagaInstance instance, StepDefinition step,
                                        SagaDefinition definition, String suspendReason) {
        if (!step.hasFallback()) {
            stateMachine.suspend(instance, suspendReason);
            return;
        }
        log.warn("[sagaId={}] step={} fallback applied — original error: {}",
                instance.getSagaId(), step.name(), suspendReason);
        metrics.recordStepFallbackApplied(instance, step);
        checkpointStore.logEvent(instance.getSagaId(), step.name(), "STEP_FALLBACK_APPLIED",
                truncate(String.valueOf(step.fallback())));
        SagaInstance after = stateMachine.completeStep(instance, step, step.fallback(), definition);
        if (after.getStatus() == SagaStatus.COMPLETED) {
            releaseLock(after);
        } else {
            dispatchStep(after, definition);
        }
    }

    private static String truncate(String s) {
        if (s == null) return null;
        return s.length() <= 500 ? s : s.substring(0, 499) + "…";
    }

    private void compensate(SagaInstance instance, SagaDefinition definition) {
        int startFrom = instance.getCurrentStep() - 1;
        for (int i = startFrom; i >= 0; i--) {
            StepDefinition step = definition.steps().get(i);
            if (!step.hasCompensation()) continue;
            try {
                executorRegistry.getExecutor(step.type()).compensate(instance, step);
                instance = stateMachine.completeCompensationStep(instance, step);
            } catch (Exception e) {
                log.error("[sagaId={}] Compensation step {} failed: {}",
                        instance.getSagaId(), step.name(), e.getMessage());
                instance = stateMachine.failCompensationStep(instance, step, e.getMessage());
            }
        }
        stateMachine.finishCompensation(instance);
        releaseLock(instance); // Release on COMPENSATED or FAILED
    }

    private void acquireLockIfNeeded(SagaInstance instance, SagaDefinition definition) {
        if (!definition.hasLockTarget()) return;
        Map<String, Object> payload = instance.getPayload();
        Object targetId = payload.get(definition.lockTargetField());
        if (targetId == null) {
            log.warn("[sagaId={}] Lock target field '{}' not found in payload — skipping lock",
                    instance.getSagaId(), definition.lockTargetField());
            return;
        }
        lockManager.acquire(
                definition.lockTargetType(),
                targetId.toString(),
                instance.getSagaId(),
                instance.getSagaType(),
                Duration.ofMinutes(definition.timeoutMinutes()));
    }

    private void releaseLock(SagaInstance instance) {
        try {
            lockManager.releaseAll(instance.getSagaId());
        } catch (Exception e) {
            log.warn("[sagaId={}] Failed to release locks: {}", instance.getSagaId(), e.getMessage());
        }
    }
}
