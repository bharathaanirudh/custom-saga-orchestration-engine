package com.anirudh.saga.core.integration;

import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;
import com.anirudh.saga.core.engine.SagaOrchestrator;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import com.anirudh.saga.sdk.contract.SagaStartRequest;
import io.micrometer.core.instrument.MeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * P2-067 / E2-38 — step-level fallback.
 *
 * <p>Both sagas use Kafka steps that no participant subscribes to (the topic
 * exists but nobody replies). The test simulates the post-DLT outcome by
 * calling {@code orchestrator.handleReply(..., failureType="TECHNICAL")}
 * directly — same call path that {@link com.anirudh.saga.core.dlt.DltHandler}
 * uses after the broker exhausts the DLT pipeline.
 */
class StepFallbackIT extends AbstractIntegrationTest {

    @Autowired private SagaOrchestrator orchestrator;
    @Autowired private SagaInstanceRepository instanceRepo;
    @Autowired private MeterRegistry meterRegistry;
    @Autowired private MongoTemplate mongoTemplate;

    @AfterEach
    void cleanup() {
        instanceRepo.deleteAll();
        mongoTemplate.remove(new Query(), "saga_execution_log");
        mongoTemplate.remove(new Query(), "saga_outbox");
    }

    @Test
    void technicalFailureWithFallback_completesSagaWithFallbackInContext() {
        SagaInstance started = orchestrator.start(new SagaStartRequest(
                "it-fallback-saga",
                Map.of(),
                "fb-" + UUID.randomUUID()));
        String sagaId = started.getSagaId();
        double counterBefore = fallbackCounterValue();

        // Simulate the post-DLT technical-failure dispatch the broker would
        // have made after retryMaxAttempts was exhausted.
        orchestrator.handleReply(sagaId, "it-fallback-step", false, "TECHNICAL", null);

        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            SagaInstance reloaded = instanceRepo.findBySagaId(sagaId).orElseThrow();
            assertThat(reloaded.getStatus())
                    .as("fallback path advances saga past technical failure to COMPLETED")
                    .isEqualTo(SagaStatus.COMPLETED);
            assertThat(reloaded.getContext())
                    .as("fallback value written into context under step name")
                    .containsKey("it-fallback-step");
            Object stepResult = reloaded.getContext().get("it-fallback-step");
            assertThat(stepResult).isInstanceOf(Map.class);
            @SuppressWarnings("unchecked")
            Map<String, Object> resultMap = (Map<String, Object>) stepResult;
            assertThat(resultMap)
                    .containsEntry("addressId", "default-billing")
                    .containsEntry("source", "fallback");
        });

        assertThat(fallbackCounterValue() - counterBefore)
                .as("saga.step.fallback.applied counter incremented exactly once")
                .isEqualTo(1.0);

        long logEntries = mongoTemplate.count(
                new Query(Criteria.where("sagaId").is(sagaId)
                        .and("event").is("STEP_FALLBACK_APPLIED")),
                "saga_execution_log");
        assertThat(logEntries)
                .as("STEP_FALLBACK_APPLIED audit event written")
                .isEqualTo(1L);
    }

    @Test
    void technicalFailureWithoutFallback_suspendsSaga_noRegression() {
        SagaInstance started = orchestrator.start(new SagaStartRequest(
                "it-no-fallback-saga",
                Map.of(),
                "nofb-" + UUID.randomUUID()));
        String sagaId = started.getSagaId();

        orchestrator.handleReply(sagaId, "it-no-fallback-step", false, "TECHNICAL", null);

        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            SagaInstance reloaded = instanceRepo.findBySagaId(sagaId).orElseThrow();
            assertThat(reloaded.getStatus())
                    .as("no fallback declared → legacy SUSPEND behavior preserved")
                    .isEqualTo(SagaStatus.SUSPENDED);
        });
    }

    private double fallbackCounterValue() {
        return Optional.ofNullable(
                        meterRegistry.find("saga.step.fallback.applied").counter())
                .map(c -> c.count())
                .orElse(0.0);
    }
}
