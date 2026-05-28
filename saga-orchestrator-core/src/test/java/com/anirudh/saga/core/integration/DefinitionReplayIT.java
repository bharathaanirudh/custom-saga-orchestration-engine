package com.anirudh.saga.core.integration;

import com.anirudh.saga.core.audit.SagaExecutionLog;
import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;
import com.anirudh.saga.core.exception.SagaDefinitionNotFoundException;
import com.anirudh.saga.core.replay.BulkReplayReport;
import com.anirudh.saga.core.replay.DefinitionReplayService;
import com.anirudh.saga.core.replay.ReplayReport;
import com.anirudh.saga.core.replay.StepOutcome;
import com.anirudh.saga.core.repository.SagaExecutionLogRepository;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * P2-027 / E2-11 — time machine: replay a historical saga against a *new* definition.
 *
 * <p>Original recorded run: step-a (SUCCESS) → step-b (BUSINESS_FAILURE) → COMPENSATED.
 * Target definition `it-replay-target`: step-a + step-c-new (step-b removed, step-c-new added).
 */
class DefinitionReplayIT extends AbstractIntegrationTest {

    @Autowired private DefinitionReplayService replayService;
    @Autowired private SagaExecutionLogRepository logRepo;
    @Autowired private SagaInstanceRepository instanceRepo;
    @Autowired private MongoTemplate mongoTemplate;

    private static final Instant T0 = Instant.parse("2026-03-27T10:00:00Z");

    @AfterEach
    void cleanup() {
        instanceRepo.deleteAll();
        mongoTemplate.remove(new Query(), "saga_execution_log");
        mongoTemplate.remove(new Query(), "saga_outbox");
    }

    @Test
    void ac1_ac3_ac6_replayReconstructsOutcomesAndDiffsAgainstNewDefinition() {
        String sagaId = seedOriginalRun(UUID.randomUUID().toString());

        ReplayReport r = replayService.replay(sagaId, "it-replay-target");

        // AC-1: outcomes reconstructed from the original event log
        assertThat(r.originalOutcomesByStep())
                .containsEntry("step-a", StepOutcome.SUCCESS)
                .containsEntry("step-b", StepOutcome.BUSINESS_FAILURE);
        assertThat(r.originalFinalStatus()).isEqualTo("COMPENSATED");

        // Replayed against the new definition: step-a succeeds, step-c-new (new) assumed success → COMPLETED
        assertThat(r.replayedFinalStatus()).isEqualTo("COMPLETED");

        // AC-3: structural + behavioral diff
        assertThat(r.diff().stepsAdded()).contains("step-c-new");
        assertThat(r.diff().stepsRemoved()).contains("step-b");
        assertThat(r.diff().finalStatusChanged())
                .as("AC-3: original COMPENSATED vs replayed COMPLETED")
                .isTrue();

        // AC-6: the new step has no recorded outcome → flagged UNKNOWN in the timeline
        assertThat(r.replayedTimeline())
                .filteredOn(s -> s.stepName().equals("step-c-new"))
                .singleElement()
                .extracting(ReplayReport.SimulatedStep::appliedOutcome)
                .isEqualTo(StepOutcome.UNKNOWN);
    }

    @Test
    void ac2_replayPerformsNoWrites() {
        String sagaId = seedOriginalRun(UUID.randomUUID().toString());
        long instancesBefore = mongoTemplate.getCollection("saga_instances").countDocuments();
        long outboxBefore = mongoTemplate.getCollection("saga_outbox").countDocuments();
        long eventsBefore = mongoTemplate.getCollection("saga_execution_log").countDocuments();

        replayService.replay(sagaId, "it-replay-target");

        assertThat(mongoTemplate.getCollection("saga_instances").countDocuments())
                .as("AC-2: no SagaInstance writes").isEqualTo(instancesBefore);
        assertThat(mongoTemplate.getCollection("saga_outbox").countDocuments())
                .as("AC-2: no outbox publishes").isEqualTo(outboxBefore);
        assertThat(mongoTemplate.getCollection("saga_execution_log").countDocuments())
                .as("AC-2: no new events").isEqualTo(eventsBefore);
    }

    @Test
    void ac5_unknownDefinitionReturns404() {
        String sagaId = seedOriginalRun(UUID.randomUUID().toString());
        assertThatThrownBy(() -> replayService.replay(sagaId, "no-such-definition"))
                .as("AC-5: unknown target definition → SagaDefinitionNotFoundException (HTTP 404)")
                .isInstanceOf(SagaDefinitionNotFoundException.class);
    }

    @Test
    void ac4_bulkReplayAggregatesDivergence() {
        // Two sagas of one type; both originally COMPENSATED, both replay to COMPLETED → both diverge.
        String type = "it-replay-bulk";
        for (int i = 0; i < 2; i++) {
            String sagaId = seedOriginalRun(UUID.randomUUID().toString());
            SagaInstance inst = new SagaInstance();
            inst.setSagaId(sagaId);
            inst.setSagaType(type);
            inst.setStatus(SagaStatus.COMPENSATED);
            inst.setIdempotencyKey("idem-" + sagaId);
            instanceRepo.save(inst);
        }

        BulkReplayReport report = replayService.bulkReplay(type, "it-replay-target");

        assertThat(report.replayed()).isEqualTo(2);
        assertThat(report.diverged()).isEqualTo(2);
        assertThat(report.sameFinalStatus()).isZero();
    }

    // --- helpers ---

    /** Seed the original run: step-a SUCCESS, step-b BUSINESS_FAILURE, saga COMPENSATED. */
    private String seedOriginalRun(String sagaId) {
        appendEvent(sagaId, "SAGA", "STARTED", null, 0);
        appendEvent(sagaId, "step-a", "STEP_STARTED", null, 1);
        appendEvent(sagaId, "step-a", "STEP_COMPLETED", null, 2);
        appendEvent(sagaId, "step-b", "STEP_STARTED", null, 3);
        appendEvent(sagaId, "SAGA", "COMPENSATION_STARTED", "Business failure at step: step-b", 4);
        appendEvent(sagaId, "step-a", "COMPENSATION_STEP_COMPLETED", null, 5);
        appendEvent(sagaId, "SAGA", "COMPENSATED", null, 6);
        return sagaId;
    }

    private void appendEvent(String sagaId, String step, String event, String data, int offsetMillis) {
        logRepo.save(SagaExecutionLog.of(sagaId, step, event, data,
                T0.plusMillis(offsetMillis), "ENGINE", null));
    }
}
