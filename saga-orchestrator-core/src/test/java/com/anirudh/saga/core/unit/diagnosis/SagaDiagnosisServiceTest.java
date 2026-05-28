package com.anirudh.saga.core.unit.diagnosis;

import com.anirudh.saga.core.audit.SagaExecutionLog;
import com.anirudh.saga.core.diagnosis.Diagnosis;
import com.anirudh.saga.core.diagnosis.RootCause;
import com.anirudh.saga.core.diagnosis.SagaDiagnosisService;
import com.anirudh.saga.core.diagnosis.SuggestedAction;
import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Heuristic coverage for {@link SagaDiagnosisService} — one test per
 * {@link RootCause} path plus the absence case for healthy sagas.
 */
class SagaDiagnosisServiceTest {

    private final SagaDiagnosisService service = new SagaDiagnosisService();

    @Test
    void healthySaga_returnsEmpty() {
        SagaInstance instance = newInstance(SagaStatus.IN_PROGRESS);
        assertThat(service.diagnose(instance, List.of())).isEmpty();
    }

    @Test
    void completedSaga_returnsEmpty() {
        SagaInstance instance = newInstance(SagaStatus.COMPLETED);
        assertThat(service.diagnose(instance, List.of())).isEmpty();
    }

    @Test
    void compensatedSaga_returnsEmpty() {
        SagaInstance instance = newInstance(SagaStatus.COMPENSATED);
        assertThat(service.diagnose(instance, List.of())).isEmpty();
    }

    @Test
    void suspended_dltExhausted_recommendsRetry() {
        SagaInstance instance = newInstance(SagaStatus.SUSPENDED);
        List<SagaExecutionLog> timeline = List.of(
                logOf("step-1", "STEP_STARTED", null),
                logOf("step-1", "STEP_COMPLETED", null),
                logOf("step-2", "STEP_STARTED", null),
                logOf("SAGA", "SUSPENDED", "Technical failure at step: step-2")
        );

        Diagnosis d = service.diagnose(instance, timeline).orElseThrow();

        assertThat(d.rootCause()).isEqualTo(RootCause.DLT_EXHAUSTED);
        assertThat(d.suggestedAction()).isEqualTo(SuggestedAction.RETRY);
        assertThat(d.lastError()).isEqualTo("Technical failure at step: step-2");
        assertThat(d.lastSuccessfulStep()).isEqualTo("step-1");
        assertThat(d.failedStep()).isEqualTo("step-2");
    }

    @Test
    void suspended_stepTimeout_recommendsCompensate() {
        SagaInstance instance = newInstance(SagaStatus.SUSPENDED);
        List<SagaExecutionLog> timeline = List.of(
                logOf("charge-payment", "STEP_STARTED", null),
                logOf("SAGA", "SUSPENDED", "Step charge-payment exceeded 30s timeout")
        );

        Diagnosis d = service.diagnose(instance, timeline).orElseThrow();

        assertThat(d.rootCause()).isEqualTo(RootCause.SAGA_TIMEOUT);
        assertThat(d.suggestedAction()).isEqualTo(SuggestedAction.COMPENSATE);
        assertThat(d.lastError()).contains("exceeded 30s timeout");
        assertThat(d.failedStep()).isEqualTo("charge-payment");
        assertThat(d.lastSuccessfulStep()).isNull();
    }

    @Test
    void suspended_httpTechnicalFailure_recommendsRetry() {
        SagaInstance instance = newInstance(SagaStatus.SUSPENDED);
        List<SagaExecutionLog> timeline = List.of(
                logOf("step-1", "STEP_STARTED", null),
                logOf("step-1", "STEP_COMPLETED", null),
                logOf("ship-order", "STEP_STARTED", null),
                logOf("SAGA", "SUSPENDED",
                        "Technical failure at HTTP step: ship-order — Connection refused")
        );

        Diagnosis d = service.diagnose(instance, timeline).orElseThrow();

        assertThat(d.rootCause()).isEqualTo(RootCause.TECHNICAL_FAILURE);
        assertThat(d.suggestedAction()).isEqualTo(SuggestedAction.RETRY);
        assertThat(d.lastError()).contains("Connection refused");
        assertThat(d.failedStep()).isEqualTo("ship-order");
        assertThat(d.lastSuccessfulStep()).isEqualTo("step-1");
    }

    @Test
    void suspended_unknownReason_recommendsInvestigate() {
        SagaInstance instance = newInstance(SagaStatus.SUSPENDED);
        List<SagaExecutionLog> timeline = List.of(
                logOf("step-1", "STEP_STARTED", null),
                logOf("SAGA", "SUSPENDED", "Something we didn't predict happened")
        );

        Diagnosis d = service.diagnose(instance, timeline).orElseThrow();

        assertThat(d.rootCause()).isEqualTo(RootCause.UNKNOWN);
        assertThat(d.suggestedAction()).isEqualTo(SuggestedAction.INVESTIGATE);
        assertThat(d.lastError()).contains("Something we didn't predict happened");
    }

    @Test
    void suspended_noReasonInLog_returnsUnknown() {
        SagaInstance instance = newInstance(SagaStatus.SUSPENDED);
        // Timeline has no SUSPENDED entry — defensive case
        List<SagaExecutionLog> timeline = List.of(
                logOf("step-1", "STEP_STARTED", null)
        );

        Diagnosis d = service.diagnose(instance, timeline).orElseThrow();

        assertThat(d.rootCause()).isEqualTo(RootCause.UNKNOWN);
        assertThat(d.suggestedAction()).isEqualTo(SuggestedAction.INVESTIGATE);
        assertThat(d.lastError()).isNull();
    }

    @Test
    void failed_compensationFailed_recommendsManualIntervention() {
        SagaInstance instance = newInstance(SagaStatus.FAILED);
        instance.setFailedCompensations(new ArrayList<>(List.of("refund-payment")));

        List<SagaExecutionLog> timeline = List.of(
                logOf("reserve-inventory", "STEP_COMPLETED", null),
                logOf("charge-payment", "STEP_COMPLETED", null),
                logOf("ship-order", "STEP_STARTED", null),
                logOf("SAGA", "COMPENSATION_STARTED", "Business failure at step: ship-order"),
                logOf("refund-payment", "COMPENSATION_STEP_FAILED",
                        "Refund gateway returned 503")
        );

        Diagnosis d = service.diagnose(instance, timeline).orElseThrow();

        assertThat(d.rootCause()).isEqualTo(RootCause.COMPENSATION_FAILED);
        assertThat(d.suggestedAction()).isEqualTo(SuggestedAction.MANUAL_INTERVENTION);
        assertThat(d.lastError()).contains("503");
        assertThat(d.failedStep()).isEqualTo("refund-payment");
        // Last STEP_COMPLETED in the timeline was charge-payment
        assertThat(d.lastSuccessfulStep()).isEqualTo("charge-payment");
    }

    // --- helpers ---

    private SagaInstance newInstance(SagaStatus status) {
        SagaInstance instance = new SagaInstance();
        instance.setSagaId("test-saga-id");
        instance.setSagaType("TEST_SAGA");
        instance.setStatus(status);
        return instance;
    }

    private SagaExecutionLog logOf(String stepName, String event, String data) {
        return SagaExecutionLog.of("test-saga-id", stepName, event, data, Instant.now());
    }
}
