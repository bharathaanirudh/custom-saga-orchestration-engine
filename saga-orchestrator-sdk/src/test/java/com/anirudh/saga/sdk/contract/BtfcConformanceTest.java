package com.anirudh.saga.sdk.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates that the SDK's {@link SagaReply} factory methods produce payloads
 * that conform to the BTFC v1 protocol — see {@code docs/btfc-protocol-v1.md}.
 *
 * <p>This test is the executable backstop for §2 (Reply Format) of the spec.
 * If the SDK ever drifts from the protocol, this test must fail before the PR
 * merges. CI runs it on every build.
 */
class BtfcConformanceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final SagaCommand CMD =
            new SagaCommand("saga-abc", "step-1", "DO_THING", Map.of("key", "value"));

    // ── §2 Reply Format — sagaId / stepId echo (P-1) ────────────────────────

    @Test
    void success_echoesSagaIdAndStepIdFromCommand() {
        SagaReply r = SagaReply.success(CMD, Map.of("result", "ok"));
        assertThat(r.sagaId()).isEqualTo("saga-abc");
        assertThat(r.stepId()).isEqualTo("step-1");
    }

    @Test
    void businessFailure_echoesSagaIdAndStepIdFromCommand() {
        SagaReply r = SagaReply.businessFailure(CMD, "CARD_DECLINED");
        assertThat(r.sagaId()).isEqualTo("saga-abc");
        assertThat(r.stepId()).isEqualTo("step-1");
    }

    @Test
    void technicalFailure_echoesSagaIdAndStepIdFromCommand() {
        SagaReply r = SagaReply.technicalFailure(CMD, "DB_TIMEOUT");
        assertThat(r.sagaId()).isEqualTo("saga-abc");
        assertThat(r.stepId()).isEqualTo("step-1");
    }

    // ── §2 Reply Format — status field allowed values ───────────────────────

    @Test
    void successReply_statusIsSUCCESS() {
        assertThat(SagaReply.success(CMD, null).status()).isEqualTo("SUCCESS");
    }

    @Test
    void businessFailureReply_statusIsBUSINESS_FAILURE() {
        assertThat(SagaReply.businessFailure(CMD, "x").status()).isEqualTo("BUSINESS_FAILURE");
    }

    @Test
    void technicalFailureReply_statusIsTECHNICAL_FAILURE() {
        assertThat(SagaReply.technicalFailure(CMD, "x").status()).isEqualTo("TECHNICAL_FAILURE");
    }

    // ── §2 Reply Format — failureType field per status (P-2) ────────────────

    @Test
    void successReply_failureTypeIsNull() {
        assertThat(SagaReply.success(CMD, null).failureType())
                .as("§2: failureType MUST be absent or null when status == SUCCESS")
                .isNull();
    }

    @Test
    void businessFailureReply_failureTypeIsBUSINESS() {
        assertThat(SagaReply.businessFailure(CMD, "x").failureType())
                .as("§2: BUSINESS_FAILURE MUST carry failureType=BUSINESS")
                .isEqualTo(FailureType.BUSINESS);
    }

    @Test
    void technicalFailureReply_failureTypeIsTECHNICAL() {
        assertThat(SagaReply.technicalFailure(CMD, "x").failureType())
                .as("§2: TECHNICAL_FAILURE MUST carry failureType=TECHNICAL")
                .isEqualTo(FailureType.TECHNICAL);
    }

    // ── §2 Reply Format — error field on failure (P-2 + SHOULD per spec) ───

    @Test
    void businessFailureReply_carriesErrorReason() {
        assertThat(SagaReply.businessFailure(CMD, "CARD_DECLINED").error())
                .as("§2: error SHOULD be present when status != SUCCESS")
                .isEqualTo("CARD_DECLINED");
    }

    @Test
    void technicalFailureReply_carriesErrorReason() {
        assertThat(SagaReply.technicalFailure(CMD, "Downstream HTTP 503").error())
                .as("§2: error SHOULD be present when status != SUCCESS")
                .isEqualTo("Downstream HTTP 503");
    }

    // ── §2 Reply Format — JSON serialization preserves the contract ─────────

    @Test
    void successReply_jsonRoundTrip_preservesFields() throws Exception {
        SagaReply original = SagaReply.success(CMD, Map.of("result", "ok"));
        String json = MAPPER.writeValueAsString(original);
        JsonNode node = MAPPER.readTree(json);

        assertThat(node.get("sagaId").asText()).isEqualTo("saga-abc");
        assertThat(node.get("stepId").asText()).isEqualTo("step-1");
        assertThat(node.get("status").asText()).isEqualTo("SUCCESS");
        assertThat(node.get("failureType").isNull())
                .as("§2: SUCCESS reply's failureType serialized as JSON null")
                .isTrue();
        assertThat(node.get("data").get("result").asText()).isEqualTo("ok");
    }

    @Test
    void businessFailureReply_jsonRoundTrip_preservesFailureType() throws Exception {
        SagaReply original = SagaReply.businessFailure(CMD, "CARD_DECLINED");
        String json = MAPPER.writeValueAsString(original);
        JsonNode node = MAPPER.readTree(json);

        assertThat(node.get("status").asText()).isEqualTo("BUSINESS_FAILURE");
        assertThat(node.get("failureType").asText())
                .as("§2: BUSINESS_FAILURE failureType serialized as the string 'BUSINESS'")
                .isEqualTo("BUSINESS");
        assertThat(node.get("error").asText()).isEqualTo("CARD_DECLINED");
    }

    @Test
    void technicalFailureReply_jsonRoundTrip_preservesFailureType() throws Exception {
        SagaReply original = SagaReply.technicalFailure(CMD, "DB_TIMEOUT");
        String json = MAPPER.writeValueAsString(original);
        JsonNode node = MAPPER.readTree(json);

        assertThat(node.get("status").asText()).isEqualTo("TECHNICAL_FAILURE");
        assertThat(node.get("failureType").asText())
                .as("§2: TECHNICAL_FAILURE failureType serialized as the string 'TECHNICAL'")
                .isEqualTo("TECHNICAL");
        assertThat(node.get("error").asText()).isEqualTo("DB_TIMEOUT");
    }

    // ── §2 + §4 — convenience predicates align with status values ───────────

    @Test
    void isSuccess_trueOnlyForSuccessStatus() {
        assertThat(SagaReply.success(CMD, null).isSuccess()).isTrue();
        assertThat(SagaReply.businessFailure(CMD, "x").isSuccess()).isFalse();
        assertThat(SagaReply.technicalFailure(CMD, "x").isSuccess()).isFalse();
    }

    @Test
    void isBusinessFailure_trueOnlyForBusinessFailureReply() {
        assertThat(SagaReply.businessFailure(CMD, "x").isBusinessFailure()).isTrue();
        assertThat(SagaReply.technicalFailure(CMD, "x").isBusinessFailure()).isFalse();
        assertThat(SagaReply.success(CMD, null).isBusinessFailure()).isFalse();
    }

    @Test
    void isTechnicalFailure_trueOnlyForTechnicalFailureReply() {
        assertThat(SagaReply.technicalFailure(CMD, "x").isTechnicalFailure()).isTrue();
        assertThat(SagaReply.businessFailure(CMD, "x").isTechnicalFailure()).isFalse();
        assertThat(SagaReply.success(CMD, null).isTechnicalFailure()).isFalse();
    }

    // ── §1 — FailureType enum closed to exactly two values ──────────────────

    @Test
    void failureTypeEnum_hasExactlyTwoValues() {
        assertThat(FailureType.values())
                .as("§1: BTFC defines exactly two failure types — BUSINESS and TECHNICAL. "
                        + "Adding a third value here is a v2 breaking change.")
                .containsExactlyInAnyOrder(FailureType.BUSINESS, FailureType.TECHNICAL);
    }
}
