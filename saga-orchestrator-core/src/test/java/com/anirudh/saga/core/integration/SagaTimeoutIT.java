package com.anirudh.saga.core.integration;

import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;
import com.anirudh.saga.core.engine.SagaOrchestrator;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import com.anirudh.saga.core.repository.SagaExecutionLogRepository;
import com.anirudh.saga.core.scheduler.StepTimeoutScheduler;
import com.anirudh.saga.sdk.contract.SagaStartRequest;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * P2-004 (timeout half) Integration Test.
 *
 * Asserts:
 *  - AC-3: clock advanced past step's timeoutSeconds → scheduler suspends the saga.
 *
 * Uses a {@link MutableClock} bean override so the test can advance time without
 * sleeping. The IT saga ({@code it-timeout-saga}) is a single-step Kafka saga whose
 * topic has NO listener — the step hangs indefinitely, allowing the clock-advance
 * + scheduler-tick path to be exercised deterministically.
 */
@Import(SagaTimeoutIT.TimeoutTestConfig.class)
class SagaTimeoutIT extends AbstractIntegrationTest {

    static final String SAGA_TYPE = "it-timeout-saga";
    static final String STEP_TOPIC = "it-timeout-noop-commands";

    @Autowired private SagaOrchestrator orchestrator;
    @Autowired private SagaInstanceRepository instances;
    @Autowired private SagaExecutionLogRepository logs;
    @Autowired private StepTimeoutScheduler scheduler;
    @Autowired private MutableClock mutableClock;

    @BeforeEach
    void resetState() {
        instances.deleteAll();
        logs.deleteAll();
        // Anchor to wall-clock, NOT a fixed past time. Multiple test contexts share
        // the saga-orchestrator consumer group; if this clock falls behind real time,
        // any cross-context reply routed to this context stamps a past timestamp on
        // currentStepStartedAt — which then trips the step-timeout scheduler in OTHER
        // contexts running on systemUTC. (Cross-context routing is itself a v1.5 fix;
        // see EXECUTION-PLAN §11.)
        mutableClock.setTo(Instant.now());
    }

    @AfterEach
    void cleanCollections() {
        instances.deleteAll();
        logs.deleteAll();
    }

    @Test
    void stepTimeout_suspendsSaga() {
        SagaInstance started = orchestrator.start(new SagaStartRequest(
                SAGA_TYPE, Map.of("orderId", "ORD-TIMEOUT-1"), "key-timeout-1"));
        String sagaId = started.getSagaId();

        // Wait until the saga has its currentStepStartedAt set (step dispatched).
        await().atMost(Duration.ofSeconds(15))
                .until(() -> instances.findBySagaId(sagaId)
                        .map(s -> s.getCurrentStepStartedAt() != null
                                && s.getStatus() == SagaStatus.IN_PROGRESS)
                        .orElse(false));

        // YAML's timeoutSeconds=10. Advance clock 11s past start time.
        mutableClock.advance(Duration.ofSeconds(11));

        // Drive the scheduler synchronously rather than waiting for @Scheduled tick.
        scheduler.detectAndHandleTimeouts();

        SagaInstance suspended = instances.findBySagaId(sagaId).orElseThrow();
        assertThat(suspended.getStatus()).isEqualTo(SagaStatus.SUSPENDED);
        assertThat(suspended.getSuspendedAt()).isNotNull();
    }

    // ── Test wiring ──────────────────────────────────────────────────────────

    @Configuration
    static class TimeoutTestConfig {
        @Bean @Primary
        MutableClock mutableClock() { return new MutableClock(Instant.now()); }

        @Bean NewTopic timeoutStepTopic() { return new NewTopic(STEP_TOPIC, 1, (short) 1); }
    }

    /**
     * A {@link Clock} subclass that can be advanced manually. Marked {@code @Primary}
     * so it shadows {@link com.anirudh.saga.core.config.ClockConfig#clock()} — the
     * engine's scheduler, state machine, and other clock-driven components all see
     * this clock.
     */
    static class MutableClock extends Clock {
        private final AtomicReference<Instant> now;
        private final ZoneId zone = ZoneId.of("UTC");

        MutableClock(Instant initial) { this.now = new AtomicReference<>(initial); }

        @Override public ZoneId getZone() { return zone; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now.get(); }

        void advance(Duration d) { now.updateAndGet(t -> t.plus(d)); }
        void setTo(Instant t) { now.set(t); }
    }
}
