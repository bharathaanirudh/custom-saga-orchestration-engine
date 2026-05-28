package com.anirudh.saga.core.integration;

import com.anirudh.saga.core.api.SagaController;
import com.anirudh.saga.core.api.SagaResponse;
import com.anirudh.saga.core.audit.ReplayResult;
import com.anirudh.saga.core.audit.SagaExecutionLog;
import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;
import com.anirudh.saga.core.repository.SagaExecutionLogRepository;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * P2-017 / E2-11 — saga replay (read-only state reconstruction from events).
 *
 * <p>Reconstruction is snapshot-based: the latest snapshot-bearing event ≤ cutoff is the
 * folded state at that point. Covers AC-1 (reconstruct), AC-2 (`upTo` cutoff), AC-3 (diff
 * vs current), AC-4 (works without a live SagaInstance).
 */
class SagaReplayIT extends AbstractIntegrationTest {

    @Autowired private SagaController controller;
    @Autowired private SagaInstanceRepository instanceRepo;
    @Autowired private SagaExecutionLogRepository logRepo;
    @Autowired private MongoTemplate mongoTemplate;

    private static final Instant T0 = Instant.parse("2026-03-27T10:00:00Z");

    @AfterEach
    void cleanup() {
        instanceRepo.deleteAll();
        mongoTemplate.remove(new Query(), "saga_execution_log");
    }

    @Test
    void ac1_reconstructsStateFromEventSnapshots() {
        String sagaId = seedSagaWithHistory();

        ReplayResult r = controller.replay(sagaId, null).data();

        assertThat(r.eventsApplied()).isEqualTo(3);
        assertThat(r.reconstructedState())
                .as("AC-1: reconstructed from the last snapshot-bearing event")
                .containsEntry("status", "COMPLETED")
                .containsEntry("currentStep", 2);
    }

    @Test
    void ac2_reconstructsStateUpToCutoff() {
        String sagaId = seedSagaWithHistory();

        // Cut off after the second event (STEP_COMPLETED at +1s, currentStep=1) but before COMPLETED (+2s).
        ReplayResult r = controller.replay(sagaId, T0.plusSeconds(1).toString()).data();

        assertThat(r.eventsApplied()).isEqualTo(2);
        assertThat(r.reconstructedState())
                .as("AC-2: state as of the cutoff, not the final state")
                .containsEntry("status", "IN_PROGRESS")
                .containsEntry("currentStep", 1);
    }

    @Test
    void ac3_highlightsDiffWhenLiveStateDisagrees() {
        String sagaId = seedSagaWithHistory();
        // Tamper the live instance so it disagrees with the event history.
        SagaInstance live = instanceRepo.findBySagaId(sagaId).orElseThrow();
        live.setStatus(SagaStatus.FAILED);
        live.setCurrentStep(99);
        instanceRepo.save(live);

        ReplayResult r = controller.replay(sagaId, null).data();

        assertThat(r.differences())
                .as("AC-3: reconstructed (COMPLETED/2) vs current (FAILED/99) differences surfaced")
                .anyMatch(d -> d.startsWith("status:"))
                .anyMatch(d -> d.startsWith("currentStep:"));
    }

    @Test
    void ac4_reconstructsEvenWhenLiveInstanceDeleted() {
        String sagaId = seedSagaWithHistory();
        instanceRepo.deleteAll(); // simulate corrupted / lost SagaInstance — events remain

        ReplayResult r = controller.replay(sagaId, null).data();

        assertThat(r.reconstructedState())
                .as("AC-4: state derivable from events alone")
                .containsEntry("status", "COMPLETED");
        assertThat(r.currentState()).isNull();
        assertThat(r.differences()).anyMatch(d -> d.contains("absent/corrupted"));
    }

    // --- helpers ---

    /** Seed a saga with 3 events carrying increasing snapshots: STARTED(0), STEP_COMPLETED(1s), COMPLETED(2s). */
    private String seedSagaWithHistory() {
        String sagaId = UUID.randomUUID().toString();
        SagaInstance i = new SagaInstance();
        i.setSagaId(sagaId);
        i.setSagaType("REPLAY_TEST");
        i.setStatus(SagaStatus.COMPLETED);
        i.setCurrentStep(2);
        i.setIdempotencyKey("idem-" + sagaId);
        instanceRepo.save(i);

        logRepo.save(SagaExecutionLog.of(sagaId, "SAGA", "STARTED", null, T0, "ENGINE",
                Map.of("status", "STARTED", "currentStep", 0)));
        logRepo.save(SagaExecutionLog.of(sagaId, "step-1", "STEP_COMPLETED", null, T0.plusSeconds(1), "ENGINE",
                Map.of("status", "IN_PROGRESS", "currentStep", 1)));
        logRepo.save(SagaExecutionLog.of(sagaId, "SAGA", "COMPLETED", null, T0.plusSeconds(2), "ENGINE",
                Map.of("status", "COMPLETED", "currentStep", 2)));
        return sagaId;
    }
}
