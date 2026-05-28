package com.anirudh.saga.core.integration;

import com.anirudh.saga.core.api.SagaController;
import com.anirudh.saga.core.api.SagaResponse;
import com.anirudh.saga.core.audit.SagaExecutionLog;
import com.anirudh.saga.core.diagnosis.Diagnosis;
import com.anirudh.saga.core.diagnosis.RootCause;
import com.anirudh.saga.core.diagnosis.SuggestedAction;
import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;
import com.anirudh.saga.core.repository.SagaExecutionLogRepository;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@code GET /sagas/{sagaId}} embeds a {@link Diagnosis} when the
 * saga is SUSPENDED or FAILED, and omits it for healthy statuses
 * (P2-038 / E2-25 AC-1, AC-6).
 *
 * <p>Calls the controller bean directly — bypasses HTTP layer but exercises
 * full DI wiring (Spring would fail to start if {@code SagaDiagnosisService}
 * isn't on the component-scan path).
 */
class SagaDiagnosisIT extends AbstractIntegrationTest {

    @Autowired private SagaController controller;
    @Autowired private SagaInstanceRepository instanceRepo;
    @Autowired private SagaExecutionLogRepository logRepo;

    @AfterEach
    void cleanup() {
        instanceRepo.deleteAll();
        logRepo.deleteAll();
    }

    @Test
    void inProgressSaga_responseHasNoDiagnosisField() {
        String sagaId = UUID.randomUUID().toString();
        seedInstance(sagaId, SagaStatus.IN_PROGRESS);
        seedLog(sagaId, "step-1", "STEP_STARTED", null);

        SagaResponse<Map<String, Object>> response = controller.get(sagaId);

        assertThat(response.success()).isTrue();
        assertThat(response.data())
                .as("diagnosis must be ABSENT for in-progress sagas")
                .doesNotContainKey("diagnosis");
    }

    @Test
    void suspendedSaga_responseEmbedsDiagnosisWithRootCause() {
        String sagaId = UUID.randomUUID().toString();
        seedInstance(sagaId, SagaStatus.SUSPENDED);
        seedLog(sagaId, "step-1", "STEP_COMPLETED", null);
        seedLog(sagaId, "step-2", "STEP_STARTED", null);
        seedLog(sagaId, "SAGA", "SUSPENDED", "Technical failure at step: step-2");

        SagaResponse<Map<String, Object>> response = controller.get(sagaId);

        assertThat(response.data()).containsKey("diagnosis");
        Diagnosis d = (Diagnosis) response.data().get("diagnosis");
        assertThat(d.rootCause()).isEqualTo(RootCause.DLT_EXHAUSTED);
        assertThat(d.suggestedAction()).isEqualTo(SuggestedAction.RETRY);
        assertThat(d.failedStep()).isEqualTo("step-2");
        assertThat(d.lastSuccessfulStep()).isEqualTo("step-1");
    }

    private void seedInstance(String sagaId, SagaStatus status) {
        SagaInstance instance = new SagaInstance();
        instance.setSagaId(sagaId);
        instance.setSagaType("TEST_SAGA");
        instance.setStatus(status);
        instance.setCreatedAt(Instant.now());
        instance.setUpdatedAt(Instant.now());
        instance.setIdempotencyKey("idem-" + sagaId);
        instanceRepo.save(instance);
    }

    private void seedLog(String sagaId, String stepName, String event, String data) {
        // Stagger timestamps so timeline ordering is deterministic
        Instant t = Instant.now().plusMillis(logRepo.findBySagaIdOrderByTimestampAsc(sagaId).size());
        logRepo.save(SagaExecutionLog.of(sagaId, stepName, event, data, t));
    }
}
