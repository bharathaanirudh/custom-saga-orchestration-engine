package com.anirudh.saga.core.integration;

import com.anirudh.saga.core.api.SagaController;
import com.anirudh.saga.core.api.SagaResponse;
import com.anirudh.saga.core.audit.SagaExecutionLog;
import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;
import com.anirudh.saga.core.engine.CheckpointStore;
import com.anirudh.saga.core.repository.SagaExecutionLogRepository;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import org.bson.Document;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * P2-016 / E2-11 — immutable, replay-grade event store on `saga_execution_log`.
 *
 * <p>Covers AC-1 (events appended), AC-2 (rich fields + snapshot), AC-3 (ordered
 * reconstruction), AC-4 (schema validator rejects malformed inserts), AC-5
 * (`GET /sagas/{id}/events` returns ordered list).
 */
class SagaEventStoreIT extends AbstractIntegrationTest {

    @Autowired private CheckpointStore checkpointStore;
    @Autowired private SagaExecutionLogRepository logRepo;
    @Autowired private SagaInstanceRepository instanceRepo;
    @Autowired private SagaController controller;
    @Autowired private MongoTemplate mongoTemplate;

    @AfterEach
    void cleanup() {
        instanceRepo.deleteAll();
        mongoTemplate.remove(new Query(), "saga_execution_log");
    }

    @Test
    void ac1_ac2_eventAppendedWithRichFieldsAndSnapshot() {
        SagaInstance instance = seedInstance(SagaStatus.IN_PROGRESS, 2);

        checkpointStore.logEvent(instance, "charge-payment", "STEP_COMPLETED", null);

        List<SagaExecutionLog> events = logRepo.findBySagaIdOrderByTimestampAsc(instance.getSagaId());
        assertThat(events).hasSize(1);
        SagaExecutionLog e = events.get(0);
        assertThat(e.getSagaId()).isEqualTo(instance.getSagaId());
        assertThat(e.getEvent()).isEqualTo("STEP_COMPLETED");
        assertThat(e.getStepName()).isEqualTo("charge-payment");
        assertThat(e.getActor()).isEqualTo("ENGINE");
        assertThat(e.getTimestamp()).isNotNull();
        assertThat(e.getInstanceSnapshot())
                .as("AC-2: snapshot captures saga state at event time")
                .isNotNull()
                .containsEntry("status", "IN_PROGRESS")
                .containsEntry("currentStep", 2);
    }

    @Test
    void ac3_ac5_orderedReconstructionViaEndpoint() {
        SagaInstance instance = seedInstance(SagaStatus.COMPLETED, 0);
        // Append a realistic ordered sequence (timestamps stagger via the engine clock;
        // insert with explicit increasing timestamps to be deterministic).
        appendAt(instance.getSagaId(), "SAGA", "STARTED", 0);
        appendAt(instance.getSagaId(), "reserve-inventory", "STEP_STARTED", 1);
        appendAt(instance.getSagaId(), "reserve-inventory", "STEP_COMPLETED", 2);
        appendAt(instance.getSagaId(), "SAGA", "COMPLETED", 3);

        SagaResponse<List<SagaExecutionLog>> resp = controller.events(instance.getSagaId());

        assertThat(resp.success()).isTrue();
        assertThat(resp.data()).extracting(SagaExecutionLog::getEvent)
                .as("AC-3/AC-5: full timeline reconstructable in order")
                .containsExactly("STARTED", "STEP_STARTED", "STEP_COMPLETED", "COMPLETED");
    }

    @Test
    void ac4_schemaValidatorRejectsMalformedInsert() {
        // Insert a document missing the required `event` + `timestamp` fields.
        // V004's $jsonSchema validator (validationAction=error) must reject it.
        assertThatThrownBy(() ->
                mongoTemplate.getCollection("saga_execution_log")
                        .insertOne(new Document("_id", UUID.randomUUID().toString())
                                .append("sagaId", "tamper-test")))
                .as("AC-4: append-only schema validator blocks malformed (non-event) inserts")
                .isInstanceOf(Exception.class);
    }

    // --- helpers ---

    private SagaInstance seedInstance(SagaStatus status, int currentStep) {
        SagaInstance i = new SagaInstance();
        i.setSagaId(UUID.randomUUID().toString());
        i.setSagaType("TEST_SAGA");
        i.setStatus(status);
        i.setCurrentStep(currentStep);
        i.setIdempotencyKey("idem-" + i.getSagaId());
        i.setContext(Map.of("seeded", true));
        return instanceRepo.save(i);
    }

    private void appendAt(String sagaId, String step, String event, int offsetMillis) {
        logRepo.save(SagaExecutionLog.of(sagaId, step, event, null,
                Instant.parse("2026-01-01T00:00:00Z").plusMillis(offsetMillis), "ENGINE", null));
    }
}
