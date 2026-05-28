package com.anirudh.saga.core.unit.scheduler;

import com.anirudh.saga.core.domain.SagaDefinition;
import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;
import com.anirudh.saga.core.domain.StepDefinition;
import com.anirudh.saga.core.domain.StepType;
import com.anirudh.saga.core.engine.SagaStateMachine;
import com.anirudh.saga.core.fixtures.SagaInstanceFixture;
import com.anirudh.saga.core.loader.SagaDefinitionLoader;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import com.anirudh.saga.core.scheduler.StepTimeoutScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class StepTimeoutSchedulerTest {

    private static final Instant NOW = Instant.parse("2026-05-06T12:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Mock SagaInstanceRepository repository;
    @Mock SagaDefinitionLoader definitions;
    @Mock SagaStateMachine stateMachine;

    StepTimeoutScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new StepTimeoutScheduler(repository, definitions, stateMachine, FIXED_CLOCK);
    }

    @Test
    void isTimedOut_withinWindow_returnsFalse() {
        SagaInstance instance = inProgressStartedAt(NOW.minusSeconds(29));
        StepDefinition step = stepWithTimeout(30);

        assertThat(scheduler.isTimedOut(instance, step, NOW)).isFalse();
    }

    @Test
    void isTimedOut_pastWindow_returnsTrue() {
        SagaInstance instance = inProgressStartedAt(NOW.minusSeconds(31));
        StepDefinition step = stepWithTimeout(30);

        assertThat(scheduler.isTimedOut(instance, step, NOW)).isTrue();
    }

    @Test
    void isTimedOut_clearedStartTime_returnsFalse() {
        // Race window: reply received between query and processing → currentStepStartedAt cleared.
        SagaInstance instance = inProgressStartedAt(null);
        StepDefinition step = stepWithTimeout(30);

        assertThat(scheduler.isTimedOut(instance, step, NOW)).isFalse();
    }

    @Test
    void isTimedOut_nonInProgress_returnsFalse() {
        // Race window: status changed (e.g., to COMPENSATING) between query and processing.
        SagaInstance instance = inProgressStartedAt(NOW.minusSeconds(120));
        instance.setStatus(SagaStatus.COMPENSATING);
        StepDefinition step = stepWithTimeout(30);

        assertThat(scheduler.isTimedOut(instance, step, NOW)).isFalse();
    }

    @Test
    void isTimedOut_zeroTimeoutSeconds_returnsFalse() {
        // YAML default for unset int field is 0; treat as "no per-step timeout".
        SagaInstance instance = inProgressStartedAt(NOW.minusSeconds(99999));
        StepDefinition step = stepWithTimeout(0);

        assertThat(scheduler.isTimedOut(instance, step, NOW)).isFalse();
    }

    @Test
    void detectAndHandleTimeouts_pastWindow_suspends() {
        SagaInstance instance = inProgressStartedAt(NOW.minusSeconds(60));
        instance.setSagaType("test-saga");
        instance.setCurrentStep(0);
        SagaDefinition def = singleStepDefinition("test-saga", 30);
        when(repository.findStepCandidatesForTimeoutCheck(any())).thenReturn(List.of(instance));
        when(definitions.getDefinition("test-saga")).thenReturn(def);

        scheduler.detectAndHandleTimeouts();

        verify(stateMachine).suspend(eq(instance), contains("exceeded 30s timeout"));
    }

    @Test
    void detectAndHandleTimeouts_withinWindow_doesNothing() {
        SagaInstance instance = inProgressStartedAt(NOW.minusSeconds(10));
        instance.setSagaType("test-saga");
        instance.setCurrentStep(0);
        SagaDefinition def = singleStepDefinition("test-saga", 30);
        when(repository.findStepCandidatesForTimeoutCheck(any())).thenReturn(List.of(instance));
        when(definitions.getDefinition("test-saga")).thenReturn(def);

        scheduler.detectAndHandleTimeouts();

        verify(stateMachine, never()).suspend(any(), any());
    }

    @Test
    void detectAndHandleTimeouts_emptyCandidates_doesNothing() {
        when(repository.findStepCandidatesForTimeoutCheck(any())).thenReturn(List.of());

        scheduler.detectAndHandleTimeouts();

        verifyNoInteractions(definitions, stateMachine);
    }

    @Test
    void detectAndHandleTimeouts_unknownSagaType_skipped() {
        SagaInstance instance = inProgressStartedAt(NOW.minusSeconds(60));
        instance.setSagaType("unknown");
        instance.setCurrentStep(0);
        when(repository.findStepCandidatesForTimeoutCheck(any())).thenReturn(List.of(instance));
        when(definitions.getDefinition("unknown")).thenReturn(null);

        scheduler.detectAndHandleTimeouts();

        verify(stateMachine, never()).suspend(any(), any());
    }

    @Test
    void detectAndHandleTimeouts_currentStepOutOfRange_skipped() {
        SagaInstance instance = inProgressStartedAt(NOW.minusSeconds(60));
        instance.setSagaType("test-saga");
        instance.setCurrentStep(99); // out of range
        SagaDefinition def = singleStepDefinition("test-saga", 30);
        when(repository.findStepCandidatesForTimeoutCheck(any())).thenReturn(List.of(instance));
        when(definitions.getDefinition("test-saga")).thenReturn(def);

        scheduler.detectAndHandleTimeouts();

        verify(stateMachine, never()).suspend(any(), any());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private SagaInstance inProgressStartedAt(Instant startedAt) {
        SagaInstance instance = SagaInstanceFixture.inProgress();
        instance.setCurrentStepStartedAt(startedAt);
        return instance;
    }

    private StepDefinition stepWithTimeout(int timeoutSeconds) {
        return new StepDefinition("step", StepType.KAFKA, "ACT", "mod",
                "topic", null, null, null, null, 0, timeoutSeconds, null);
    }

    private SagaDefinition singleStepDefinition(String name, int timeoutSeconds) {
        return new SagaDefinition(name, 30, List.of(stepWithTimeout(timeoutSeconds)));
    }
}
