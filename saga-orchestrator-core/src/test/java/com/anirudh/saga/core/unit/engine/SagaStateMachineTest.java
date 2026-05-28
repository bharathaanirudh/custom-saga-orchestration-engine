package com.anirudh.saga.core.unit.engine;

import com.anirudh.saga.core.audit.SagaExecutionLog;
import com.anirudh.saga.core.domain.*;
import com.anirudh.saga.core.engine.CheckpointStore;
import com.anirudh.saga.core.engine.SagaStateMachine;
import com.anirudh.saga.core.exception.SagaInvalidStateException;
import com.anirudh.saga.core.fixtures.SagaDefinitionFixture;
import com.anirudh.saga.core.fixtures.SagaInstanceFixture;
import com.anirudh.saga.core.metrics.SagaMetrics;
import com.anirudh.saga.core.repository.SagaExecutionLogRepository;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SagaStateMachineTest {

    @Mock SagaInstanceRepository instanceRepository;
    @Mock SagaExecutionLogRepository logRepository;
    @Mock SagaMetrics metrics;

    SagaStateMachine stateMachine;
    Clock fixedClock;

    @BeforeEach
    void setUp() {
        fixedClock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        when(instanceRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(logRepository.save(any(SagaExecutionLog.class))).thenAnswer(inv -> inv.getArgument(0));
        CheckpointStore checkpointStore = new CheckpointStore(instanceRepository, logRepository, fixedClock);
        stateMachine = new SagaStateMachine(checkpointStore, metrics, fixedClock);
    }

    @Test
    void initialize_setsStatusToStarted() {
        SagaInstance instance = SagaInstanceFixture.started();
        SagaInstance result = stateMachine.initialize(instance);
        assertThat(result.getStatus()).isEqualTo(SagaStatus.STARTED);
        verify(metrics).recordStarted(any());
    }

    @Test
    void startStep_setsStatusToInProgress() {
        SagaInstance instance = SagaInstanceFixture.started();
        StepDefinition step = SagaDefinitionFixture.kafkaStep("step-one", "ACTION", "topic", null, null);
        SagaInstance result = stateMachine.startStep(instance, step);
        assertThat(result.getStatus()).isEqualTo(SagaStatus.IN_PROGRESS);
        verify(metrics).recordStepExecuted(any(), any());
    }

    @Test
    void completeStep_advancesCurrentStep() {
        SagaInstance instance = SagaInstanceFixture.inProgress();
        SagaDefinition definition = SagaDefinitionFixture.twoStepKafkaSaga();
        StepDefinition step = definition.steps().get(0);
        SagaInstance result = stateMachine.completeStep(instance, step, "result-data", definition);
        assertThat(result.getCurrentStep()).isEqualTo(1);
        assertThat(result.getContext()).containsKey("step-one");
    }

    @Test
    void completeStep_lastStep_movesToCompleted() {
        SagaDefinition definition = SagaDefinitionFixture.singleKafkaStep();
        SagaInstance instance = SagaInstanceFixture.inProgress();
        StepDefinition step = definition.steps().get(0);
        SagaInstance result = stateMachine.completeStep(instance, step, null, definition);
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        verify(metrics).recordCompleted(any());
    }

    @Test
    void completeStep_notLastStep_staysInProgress() {
        SagaDefinition definition = SagaDefinitionFixture.twoStepKafkaSaga();
        SagaInstance instance = SagaInstanceFixture.inProgress();
        StepDefinition step = definition.steps().get(0);
        SagaInstance result = stateMachine.completeStep(instance, step, null, definition);
        assertThat(result.getStatus()).isEqualTo(SagaStatus.IN_PROGRESS);
    }

    @Test
    void startCompensation_setsStatusToCompensating() {
        SagaInstance instance = SagaInstanceFixture.inProgress();
        SagaInstance result = stateMachine.startCompensation(instance, "Business failure");
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
    }

    @Test
    void completeCompensationStep_decrementsCurrentStep() {
        SagaInstance instance = SagaInstanceFixture.inProgressAtStep(2);
        instance.setStatus(SagaStatus.COMPENSATING);
        StepDefinition step = SagaDefinitionFixture.kafkaStep("step-two", "A", "t", "COMP", "t");
        SagaInstance result = stateMachine.completeCompensationStep(instance, step);
        assertThat(result.getCurrentStep()).isEqualTo(1);
    }

    @Test
    void finishCompensation_noFailures_movesToCompensated() {
        SagaInstance instance = SagaInstanceFixture.inProgress();
        instance.setStatus(SagaStatus.COMPENSATING);
        SagaInstance result = stateMachine.finishCompensation(instance);
        assertThat(result.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
        verify(metrics).recordCompensated(any());
    }

    @Test
    void finishCompensation_withFailures_movesToFailed() {
        SagaInstance instance = SagaInstanceFixture.inProgress();
        instance.setStatus(SagaStatus.COMPENSATING);
        instance.addFailedCompensation("step-one");
        SagaInstance result = stateMachine.finishCompensation(instance);
        assertThat(result.getStatus()).isEqualTo(SagaStatus.FAILED);
        verify(metrics).recordFailed(any());
    }

    @Test
    void failCompensationStep_addsToFailedList() {
        SagaInstance instance = SagaInstanceFixture.inProgress();
        instance.setStatus(SagaStatus.COMPENSATING);
        StepDefinition step = SagaDefinitionFixture.kafkaStep("step-one", "A", "t", "C", "t");
        SagaInstance result = stateMachine.failCompensationStep(instance, step, "DB down");
        assertThat(result.getFailedCompensations()).containsExactly("step-one");
        verify(metrics).recordStepFailed(any(), any(), any());
    }

    @Test
    void suspend_setsStatusAndSuspendedAt() {
        SagaInstance instance = SagaInstanceFixture.inProgress();
        SagaInstance result = stateMachine.suspend(instance, "Technical failure");
        assertThat(result.getStatus()).isEqualTo(SagaStatus.SUSPENDED);
        assertThat(result.getSuspendedAt()).isNotNull();
        verify(metrics).recordSuspended(any());
    }

    @Test
    void validateForRetry_onSuspended_doesNotThrow() {
        SagaInstance instance = SagaInstanceFixture.suspended();
        assertThatCode(() -> stateMachine.validateForRetry(instance)).doesNotThrowAnyException();
    }

    @Test
    void validateForRetry_onCompleted_throws() {
        SagaInstance instance = SagaInstanceFixture.inProgress();
        instance.setStatus(SagaStatus.COMPLETED);
        assertThatThrownBy(() -> stateMachine.validateForRetry(instance))
                .isInstanceOf(SagaInvalidStateException.class);
    }

    @Test
    void validateForCompensation_onCompleted_throws() {
        SagaInstance instance = SagaInstanceFixture.inProgress();
        instance.setStatus(SagaStatus.COMPLETED);
        assertThatThrownBy(() -> stateMachine.validateForCompensation(instance))
                .isInstanceOf(SagaInvalidStateException.class);
    }

    @Test
    void validateForCompensation_onInProgress_doesNotThrow() {
        SagaInstance instance = SagaInstanceFixture.inProgress();
        assertThatCode(() -> stateMachine.validateForCompensation(instance)).doesNotThrowAnyException();
    }
}
