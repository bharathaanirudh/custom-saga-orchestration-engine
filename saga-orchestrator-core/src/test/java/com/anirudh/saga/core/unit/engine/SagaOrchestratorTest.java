package com.anirudh.saga.core.unit.engine;

import com.anirudh.saga.core.domain.*;
import com.anirudh.saga.core.engine.*;
import com.anirudh.saga.core.exception.SagaDefinitionNotFoundException;
import com.anirudh.saga.core.exception.SagaNotFoundException;
import com.anirudh.saga.core.executor.StepExecutor;
import com.anirudh.saga.core.executor.StepExecutorRegistry;
import com.anirudh.saga.core.executor.StepResult;
import com.anirudh.saga.core.fixtures.SagaDefinitionFixture;
import com.anirudh.saga.core.fixtures.SagaInstanceFixture;
import com.anirudh.saga.core.loader.SagaDefinitionLoader;
import com.anirudh.saga.core.lock.SagaLockManager;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import com.anirudh.saga.sdk.contract.SagaStartRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SagaOrchestratorTest {

    @Mock SagaDefinitionLoader definitionLoader;
    @Mock SagaInstanceRepository repository;
    @Mock SagaStateMachine stateMachine;
    @Mock IdempotencyGuard idempotencyGuard;
    @Mock StepExecutorRegistry executorRegistry;
    @Mock CheckpointStore checkpointStore;
    @Mock SagaLockManager lockManager;
    @Mock StepExecutor kafkaExecutor;
    @Mock StepExecutor httpExecutor;

    SagaOrchestrator orchestrator;
    Clock fixedClock;

    @BeforeEach
    void setUp() {
        fixedClock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        orchestrator = new SagaOrchestrator(
                definitionLoader, repository, stateMachine, idempotencyGuard,
                executorRegistry, checkpointStore, lockManager, fixedClock,
                new com.fasterxml.jackson.databind.ObjectMapper(),
                new com.anirudh.saga.core.metrics.SagaMetrics(
                        new io.micrometer.core.instrument.simple.SimpleMeterRegistry(), 100),
                1024L * 1024L);

        when(executorRegistry.getExecutor(StepType.KAFKA)).thenReturn(kafkaExecutor);
        when(executorRegistry.getExecutor(StepType.HTTP)).thenReturn(httpExecutor);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SCENARIO 1: Full Success — all steps complete
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    class FullSuccess {

        @Test
        void start_newSaga_createsAndDispatchesFirstStep() {
            SagaDefinition def = SagaDefinitionFixture.twoStepKafkaSaga();
            SagaInstance instance = SagaInstanceFixture.started();

            when(definitionLoader.getDefinition("test-saga")).thenReturn(def);
            when(idempotencyGuard.computeKey(any(), any())).thenReturn("key-123");
            when(idempotencyGuard.findExisting("key-123")).thenReturn(Optional.empty());
            when(idempotencyGuard.saveOrGetExisting(any())).thenReturn(instance);
            when(stateMachine.initialize(any())).thenReturn(instance);
            when(stateMachine.startStep(any(), any())).thenReturn(instance);
            when(kafkaExecutor.execute(any(), any())).thenReturn(StepResult.dispatched());

            SagaInstance result = orchestrator.start(
                    new SagaStartRequest("test-saga", Map.of("orderId", "ORD-1"), null));

            assertThat(result).isNotNull();
            verify(stateMachine).initialize(any());
            verify(kafkaExecutor).execute(any(), eq(def.steps().get(0)));
        }

        @Test
        void handleReply_success_advancesToNextStep() {
            SagaDefinition def = SagaDefinitionFixture.twoStepKafkaSaga();
            SagaInstance instance = SagaInstanceFixture.inProgressAtStep(0);

            when(repository.findBySagaId("saga-1")).thenReturn(Optional.of(instance));
            when(definitionLoader.getDefinition("test-saga")).thenReturn(def);
            when(stateMachine.completeStep(any(), any(), any(), any())).thenReturn(instance);
            when(stateMachine.startStep(any(), any())).thenReturn(instance);
            when(kafkaExecutor.execute(any(), any())).thenReturn(StepResult.dispatched());

            orchestrator.handleReply("saga-1", "step-one", true, null, "data");

            verify(stateMachine).completeStep(any(), eq(def.steps().get(0)), eq("data"), eq(def));
        }

        @Test
        void handleReply_lastStepSuccess_completesAndReleasesLock() {
            SagaDefinition def = SagaDefinitionFixture.singleKafkaStep();
            SagaInstance instance = SagaInstanceFixture.inProgressAtStep(0);

            SagaInstance completed = SagaInstanceFixture.inProgress();
            completed.setStatus(SagaStatus.COMPLETED);

            when(repository.findBySagaId("saga-1")).thenReturn(Optional.of(instance));
            when(definitionLoader.getDefinition("test-saga")).thenReturn(def);
            when(stateMachine.completeStep(any(), any(), any(), any())).thenReturn(completed);

            orchestrator.handleReply("saga-1", "only-step", true, null, null);

            verify(lockManager).releaseAll(any());
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SCENARIO 2: Business Failure — triggers compensation
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    class BusinessFailure {

        @Test
        void handleReply_businessFailure_startsCompensation() {
            SagaDefinition def = SagaDefinitionFixture.threeStepSagaWithCompensation();
            SagaInstance instance = SagaInstanceFixture.inProgressAtStep(1);

            SagaInstance compensating = SagaInstanceFixture.inProgressAtStep(1);
            compensating.setStatus(SagaStatus.COMPENSATING);

            when(repository.findBySagaId("saga-1")).thenReturn(Optional.of(instance));
            when(definitionLoader.getDefinition("test-saga")).thenReturn(def);
            when(stateMachine.startCompensation(any(), anyString())).thenReturn(compensating);
            when(stateMachine.completeCompensationStep(any(), any())).thenReturn(compensating);
            when(stateMachine.finishCompensation(any())).thenReturn(compensating);

            orchestrator.handleReply("saga-1", "step-two", false, "BUSINESS", null);

            verify(stateMachine).startCompensation(any(), contains("Business failure"));
            // Should compensate step-one (reverse order from step 0)
            verify(kafkaExecutor).compensate(any(), eq(def.steps().get(0)));
            verify(stateMachine).finishCompensation(any());
            verify(lockManager).releaseAll(any());
        }

        @Test
        void handleReply_businessFailure_atStep3_compensatesStep2And1InReverse() {
            SagaDefinition def = SagaDefinitionFixture.threeStepSagaWithCompensation();
            SagaInstance instance = SagaInstanceFixture.inProgressAtStep(2);

            SagaInstance compensating = SagaInstanceFixture.inProgressAtStep(2);
            compensating.setStatus(SagaStatus.COMPENSATING);

            when(repository.findBySagaId("saga-1")).thenReturn(Optional.of(instance));
            when(definitionLoader.getDefinition("test-saga")).thenReturn(def);
            when(stateMachine.startCompensation(any(), anyString())).thenReturn(compensating);
            when(stateMachine.completeCompensationStep(any(), any())).thenReturn(compensating);
            when(stateMachine.finishCompensation(any())).thenReturn(compensating);

            orchestrator.handleReply("saga-1", "step-three", false, "BUSINESS", null);

            // Reverse order: step-two first, then step-one
            var inOrder = inOrder(kafkaExecutor);
            inOrder.verify(kafkaExecutor).compensate(any(), eq(def.steps().get(1)));
            inOrder.verify(kafkaExecutor).compensate(any(), eq(def.steps().get(0)));
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SCENARIO 3: Technical Failure — suspends saga
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    class TechnicalFailure {

        @Test
        void handleReply_technicalFailure_suspendsSaga() {
            SagaDefinition def = SagaDefinitionFixture.twoStepKafkaSaga();
            SagaInstance instance = SagaInstanceFixture.inProgressAtStep(0);

            SagaInstance suspended = SagaInstanceFixture.suspended();

            when(repository.findBySagaId("saga-1")).thenReturn(Optional.of(instance));
            when(definitionLoader.getDefinition("test-saga")).thenReturn(def);
            when(stateMachine.suspend(any(), anyString())).thenReturn(suspended);

            orchestrator.handleReply("saga-1", "step-one", false, "TECHNICAL", null);

            verify(stateMachine).suspend(any(), contains("Technical failure"));
            verify(kafkaExecutor, never()).compensate(any(), any());
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SCENARIO 4: HTTP step inline handling
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    class HttpStepHandling {

        SagaDefinition httpDef;

        @BeforeEach
        void setUp() {
            httpDef = new SagaDefinition("http-saga", 30, java.util.List.of(
                    SagaDefinitionFixture.httpStep("pay", "CHARGE", "http://pay/charge", "REFUND", "http://pay/refund")
            ));
        }

        @Test
        void dispatchStep_httpSuccess_completesInline() {
            SagaInstance instance = SagaInstanceFixture.started();
            SagaInstance completed = SagaInstanceFixture.inProgress();
            completed.setStatus(SagaStatus.COMPLETED);

            when(definitionLoader.getDefinition("http-saga")).thenReturn(httpDef);
            when(idempotencyGuard.computeKey(any(), any())).thenReturn("key");
            when(idempotencyGuard.findExisting(any())).thenReturn(Optional.empty());
            when(idempotencyGuard.saveOrGetExisting(any())).thenReturn(instance);
            when(stateMachine.initialize(any())).thenReturn(instance);
            when(stateMachine.startStep(any(), any())).thenReturn(instance);
            when(httpExecutor.execute(any(), any())).thenReturn(StepResult.success("paid"));
            when(stateMachine.completeStep(any(), any(), any(), any())).thenReturn(completed);

            orchestrator.start(new SagaStartRequest("http-saga", Map.of(), null));

            verify(httpExecutor).execute(any(), any());
            verify(stateMachine).completeStep(any(), any(), eq("paid"), any());
        }

        @Test
        void dispatchStep_httpBusinessFailure_compensatesInline() {
            SagaInstance instance = SagaInstanceFixture.started();
            SagaInstance compensating = SagaInstanceFixture.inProgress();
            compensating.setStatus(SagaStatus.COMPENSATING);

            when(definitionLoader.getDefinition("http-saga")).thenReturn(httpDef);
            when(idempotencyGuard.computeKey(any(), any())).thenReturn("key");
            when(idempotencyGuard.findExisting(any())).thenReturn(Optional.empty());
            when(idempotencyGuard.saveOrGetExisting(any())).thenReturn(instance);
            when(stateMachine.initialize(any())).thenReturn(instance);
            when(stateMachine.startStep(any(), any())).thenReturn(instance);
            when(httpExecutor.execute(any(), any())).thenReturn(StepResult.businessFailure("CARD_DECLINED"));
            when(stateMachine.startCompensation(any(), anyString())).thenReturn(compensating);
            when(stateMachine.finishCompensation(any())).thenReturn(compensating);

            orchestrator.start(new SagaStartRequest("http-saga", Map.of(), null));

            verify(stateMachine).startCompensation(any(), contains("Business failure"));
        }

        @Test
        void dispatchStep_httpTechnicalFailure_suspends() {
            SagaInstance instance = SagaInstanceFixture.started();
            SagaInstance suspended = SagaInstanceFixture.suspended();

            when(definitionLoader.getDefinition("http-saga")).thenReturn(httpDef);
            when(idempotencyGuard.computeKey(any(), any())).thenReturn("key");
            when(idempotencyGuard.findExisting(any())).thenReturn(Optional.empty());
            when(idempotencyGuard.saveOrGetExisting(any())).thenReturn(instance);
            when(stateMachine.initialize(any())).thenReturn(instance);
            when(stateMachine.startStep(any(), any())).thenReturn(instance);
            when(httpExecutor.execute(any(), any())).thenReturn(StepResult.technicalFailure("TIMEOUT"));
            when(stateMachine.suspend(any(), anyString())).thenReturn(suspended);

            orchestrator.start(new SagaStartRequest("http-saga", Map.of(), null));

            verify(stateMachine).suspend(any(), contains("Technical failure"));
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SCENARIO 5: Compensation failure — partial compensation → FAILED
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    class CompensationFailure {

        @Test
        void compensate_whenStepThrows_recordsFailureAndContinues() {
            SagaDefinition def = SagaDefinitionFixture.threeStepSagaWithCompensation();
            SagaInstance instance = SagaInstanceFixture.inProgressAtStep(2);

            SagaInstance compensating = SagaInstanceFixture.inProgressAtStep(2);
            compensating.setStatus(SagaStatus.COMPENSATING);

            when(repository.findBySagaId("saga-1")).thenReturn(Optional.of(instance));
            when(definitionLoader.getDefinition("test-saga")).thenReturn(def);
            when(stateMachine.startCompensation(any(), anyString())).thenReturn(compensating);
            // step-two compensation fails
            doThrow(new RuntimeException("DB down")).when(kafkaExecutor).compensate(any(), eq(def.steps().get(1)));
            when(stateMachine.failCompensationStep(any(), any(), anyString())).thenReturn(compensating);
            // step-one compensation succeeds
            when(stateMachine.completeCompensationStep(any(), any())).thenReturn(compensating);
            when(stateMachine.finishCompensation(any())).thenReturn(compensating);

            orchestrator.handleReply("saga-1", "step-three", false, "BUSINESS", null);

            // Both compensation attempts were made despite step-two failing
            verify(kafkaExecutor).compensate(any(), eq(def.steps().get(1))); // fails
            verify(kafkaExecutor).compensate(any(), eq(def.steps().get(0))); // succeeds
            verify(stateMachine).failCompensationStep(any(), eq(def.steps().get(1)), eq("DB down"));
            verify(stateMachine).completeCompensationStep(any(), eq(def.steps().get(0)));
            verify(stateMachine).finishCompensation(any());
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SCENARIO 6: Idempotency — duplicate request returns existing
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    class Idempotency {

        @Test
        void start_duplicateRequest_returnsExisting() {
            SagaInstance existing = SagaInstanceFixture.inProgress();
            when(definitionLoader.getDefinition("test-saga")).thenReturn(SagaDefinitionFixture.twoStepKafkaSaga());
            when(idempotencyGuard.computeKey(any(), any())).thenReturn("key-dup");
            when(idempotencyGuard.findExisting("key-dup")).thenReturn(Optional.of(existing));

            SagaInstance result = orchestrator.start(
                    new SagaStartRequest("test-saga", Map.of(), null));

            assertThat(result).isSameAs(existing);
            verify(stateMachine, never()).initialize(any());
            verify(kafkaExecutor, never()).execute(any(), any());
        }

        @Test
        void start_withExplicitIdempotencyKey_usesItDirectly() {
            SagaInstance existing = SagaInstanceFixture.inProgress();
            when(definitionLoader.getDefinition("test-saga")).thenReturn(SagaDefinitionFixture.twoStepKafkaSaga());
            when(idempotencyGuard.findExisting("my-custom-key")).thenReturn(Optional.of(existing));

            SagaInstance result = orchestrator.start(
                    new SagaStartRequest("test-saga", Map.of(), "my-custom-key"));

            assertThat(result).isSameAs(existing);
            verify(idempotencyGuard, never()).computeKey(any(), any());
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SCENARIO 7: Edge cases
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    class EdgeCases {

        @Test
        void handleReply_unknownSagaId_loggedAndMetered() {
            // P2-061 AC-3 changed contract: unknown sagaIds no longer throw — they're
            // logged at WARN and increment the saga.reply.unknown counter.
            when(repository.findBySagaId("unknown")).thenReturn(Optional.empty());
            // Should not throw.
            orchestrator.handleReply("unknown", "step", true, null, null);
            verifyNoInteractions(stateMachine);
        }

        @Test
        void handleReply_duplicateReply_ignored() {
            SagaDefinition def = SagaDefinitionFixture.twoStepKafkaSaga();
            SagaInstance instance = SagaInstanceFixture.inProgressAtStep(1); // already at step 1

            when(repository.findBySagaId("saga-1")).thenReturn(Optional.of(instance));
            when(definitionLoader.getDefinition("test-saga")).thenReturn(def);

            // Reply for step-one but we're already at step 1 (step-two)
            orchestrator.handleReply("saga-1", "step-one", true, null, null);

            // Should be ignored — no state machine calls
            verify(stateMachine, never()).completeStep(any(), any(), any(), any());
        }

        @Test
        void handleReply_alreadyCompleted_ignored() {
            SagaDefinition def = SagaDefinitionFixture.singleKafkaStep();
            SagaInstance instance = SagaInstanceFixture.inProgressAtStep(1); // past all steps

            when(repository.findBySagaId("saga-1")).thenReturn(Optional.of(instance));
            when(definitionLoader.getDefinition("test-saga")).thenReturn(def); // matches fixture sagaType

            orchestrator.handleReply("saga-1", "only-step", true, null, null);

            verify(stateMachine, never()).completeStep(any(), any(), any(), any());
        }

        @Test
        void start_unknownSagaType_throwsDefinitionNotFound() {
            when(definitionLoader.getDefinition("nonexistent"))
                    .thenThrow(new SagaDefinitionNotFoundException("nonexistent"));

            assertThatThrownBy(() -> orchestrator.start(
                    new SagaStartRequest("nonexistent", Map.of(), null)))
                    .isInstanceOf(SagaDefinitionNotFoundException.class);
        }

        @Test
        void retryFromSuspended_checkpointsBeforeDispatch() {
            SagaDefinition def = SagaDefinitionFixture.twoStepKafkaSaga();
            SagaInstance suspended = SagaInstanceFixture.suspended();

            when(repository.findBySagaId("saga-1")).thenReturn(Optional.of(suspended));
            when(definitionLoader.getDefinition("test-saga")).thenReturn(def);
            when(checkpointStore.save(any())).thenReturn(suspended);
            when(stateMachine.startStep(any(), any())).thenReturn(suspended);
            when(kafkaExecutor.execute(any(), any())).thenReturn(StepResult.dispatched());

            orchestrator.retryFromSuspended("saga-1");

            var inOrder = inOrder(checkpointStore, kafkaExecutor);
            inOrder.verify(checkpointStore).save(any()); // checkpoint FIRST
            inOrder.verify(kafkaExecutor).execute(any(), any()); // dispatch AFTER
        }

        @Test
        void triggerCompensation_onCompletedSaga_throws() {
            SagaInstance completed = SagaInstanceFixture.inProgress();
            completed.setStatus(SagaStatus.COMPLETED);

            when(repository.findBySagaId("saga-1")).thenReturn(Optional.of(completed));
            doThrow(new com.anirudh.saga.core.exception.SagaInvalidStateException("nope"))
                    .when(stateMachine).validateForCompensation(completed);

            assertThatThrownBy(() -> orchestrator.triggerCompensation("saga-1"))
                    .isInstanceOf(com.anirudh.saga.core.exception.SagaInvalidStateException.class);
        }

        @Test
        void compensate_skipsStepsWithoutCompensation() {
            // step-two has no compensation (null)
            SagaDefinition def = SagaDefinitionFixture.twoStepKafkaSaga();
            SagaInstance instance = SagaInstanceFixture.inProgressAtStep(1);

            SagaInstance compensating = SagaInstanceFixture.inProgressAtStep(1);
            compensating.setStatus(SagaStatus.COMPENSATING);

            when(repository.findBySagaId("saga-1")).thenReturn(Optional.of(instance));
            when(definitionLoader.getDefinition("test-saga")).thenReturn(def);
            when(stateMachine.startCompensation(any(), anyString())).thenReturn(compensating);
            when(stateMachine.completeCompensationStep(any(), any())).thenReturn(compensating);
            when(stateMachine.finishCompensation(any())).thenReturn(compensating);

            orchestrator.handleReply("saga-1", "step-two", false, "BUSINESS", null);

            // step-one has compensation, step-two doesn't (null) — only step-one compensated
            // startFrom = currentStep(1) - 1 = 0, so only step at index 0 is checked
            verify(kafkaExecutor).compensate(any(), eq(def.steps().get(0)));
            verify(stateMachine).finishCompensation(any());
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SCENARIO 8: Lock manager integration
    // ═══════════════════════════════════════════════════════════════════════

    @Nested
    class LockIntegration {

        @Test
        void start_withLockTarget_acquiresLock() {
            SagaDefinition def = new SagaDefinition("locked-saga", 30,
                    java.util.List.of(SagaDefinitionFixture.kafkaStep("s", "A", "t", null, null)),
                    "order", "orderId");
            SagaInstance instance = SagaInstanceFixture.started();

            when(definitionLoader.getDefinition("locked-saga")).thenReturn(def);
            when(idempotencyGuard.computeKey(any(), any())).thenReturn("key");
            when(idempotencyGuard.findExisting(any())).thenReturn(Optional.empty());
            when(idempotencyGuard.saveOrGetExisting(any())).thenReturn(instance);
            when(stateMachine.initialize(any())).thenReturn(instance);
            when(stateMachine.startStep(any(), any())).thenReturn(instance);
            when(kafkaExecutor.execute(any(), any())).thenReturn(StepResult.dispatched());

            orchestrator.start(new SagaStartRequest("locked-saga", Map.of("orderId", "ORD-123"), null));

            verify(lockManager).acquire(eq("order"), eq("ORD-001"), any(), eq("test-saga"), any());
        }

        @Test
        void start_withoutLockTarget_skipsLock() {
            SagaDefinition def = SagaDefinitionFixture.twoStepKafkaSaga(); // no lock config
            SagaInstance instance = SagaInstanceFixture.started();

            when(definitionLoader.getDefinition("test-saga")).thenReturn(def);
            when(idempotencyGuard.computeKey(any(), any())).thenReturn("key");
            when(idempotencyGuard.findExisting(any())).thenReturn(Optional.empty());
            when(idempotencyGuard.saveOrGetExisting(any())).thenReturn(instance);
            when(stateMachine.initialize(any())).thenReturn(instance);
            when(stateMachine.startStep(any(), any())).thenReturn(instance);
            when(kafkaExecutor.execute(any(), any())).thenReturn(StepResult.dispatched());

            orchestrator.start(new SagaStartRequest("test-saga", Map.of(), null));

            verify(lockManager, never()).acquire(any(), any(), any(), any(), any());
        }
    }
}
