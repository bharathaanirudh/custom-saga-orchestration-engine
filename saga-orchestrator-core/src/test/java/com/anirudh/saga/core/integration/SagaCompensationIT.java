package com.anirudh.saga.core.integration;

import com.anirudh.saga.core.audit.SagaExecutionLog;
import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;
import com.anirudh.saga.core.engine.SagaOrchestrator;
import com.anirudh.saga.core.lock.SagaLockRepository;
import com.anirudh.saga.core.repository.SagaExecutionLogRepository;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import com.anirudh.saga.sdk.contract.SagaCommand;
import com.anirudh.saga.sdk.contract.SagaHeaders;
import com.anirudh.saga.sdk.contract.SagaReply;
import com.anirudh.saga.sdk.contract.SagaStartRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * P2-003 Compensation Integration Test.
 *
 * Asserts:
 *  - AC-1: 3-step saga, steps 1+2 succeed, step 3 → BUSINESS failure → compensation starts
 *  - AC-2: Compensation runs in reverse order (step 2 then step 1)
 *  - AC-3: Both compensations succeed → status = COMPENSATED
 *  - AC-5: No lingering locks after compensation completes
 *
 * AC-4 (compensation-step-fails → FAILED) is out of v0.1 scope — see EXECUTION-PLAN §11.
 */
@Import(SagaCompensationIT.CompTestConfig.class)
class SagaCompensationIT extends AbstractIntegrationTest {

    static final String SAGA_TYPE = "it-compensation-saga";
    static final String STEP_ONE_TOPIC = "it-comp-one-commands";
    static final String STEP_TWO_TOPIC = "it-comp-two-commands";
    static final String STEP_THREE_TOPIC = "it-comp-three-commands";

    @Autowired private SagaOrchestrator orchestrator;
    @Autowired private SagaInstanceRepository instances;
    @Autowired private SagaExecutionLogRepository logs;
    @Autowired private SagaLockRepository locks;

    @BeforeEach
    void resetState() {
        instances.deleteAll();
        logs.deleteAll();
        locks.deleteAll();
    }

    @AfterEach
    void cleanCollections() {
        instances.deleteAll();
        logs.deleteAll();
        locks.deleteAll();
    }

    @Test
    void businessFailureMidSaga_compensatesInReverseOrder() {
        SagaInstance started = orchestrator.start(new SagaStartRequest(
                SAGA_TYPE, Map.of("orderId", "ORD-COMP-1"), "key-comp-1"));
        String sagaId = started.getSagaId();

        await().atMost(Duration.ofSeconds(60))
                .until(() -> instances.findBySagaId(sagaId)
                        .map(s -> s.getStatus() == SagaStatus.COMPENSATED)
                        .orElse(false));

        // AC-1: compensation actually started — execution log records COMPENSATION_STARTED
        List<SagaExecutionLog> timeline = logs.findBySagaIdOrderByTimestampAsc(sagaId);
        List<String> events = timeline.stream().map(SagaExecutionLog::getEvent).toList();
        assertThat(events).contains("COMPENSATION_STARTED", "COMPENSATED");

        // AC-2: reverse order — step 2's COMPENSATION_STEP_COMPLETED must precede step 1's
        int step2CompIdx = indexOf(timeline, "it-comp-step-two", "COMPENSATION_STEP_COMPLETED");
        int step1CompIdx = indexOf(timeline, "it-comp-step-one", "COMPENSATION_STEP_COMPLETED");
        assertThat(step2CompIdx).isGreaterThanOrEqualTo(0);
        assertThat(step1CompIdx).isGreaterThanOrEqualTo(0);
        assertThat(step2CompIdx).isLessThan(step1CompIdx);
    }

    @Test
    void afterCompensation_noLockHeld() {
        SagaInstance started = orchestrator.start(new SagaStartRequest(
                SAGA_TYPE, Map.of("orderId", "ORD-COMP-2"), "key-comp-2"));
        String sagaId = started.getSagaId();

        await().atMost(Duration.ofSeconds(60))
                .until(() -> instances.findBySagaId(sagaId)
                        .map(s -> s.getStatus() == SagaStatus.COMPENSATED)
                        .orElse(false));

        // AC-5: no locks anywhere (saga has no lockTargetType, so this is the trivial case)
        assertThat(locks.findAll()).isEmpty();
    }

    @Test
    void compensatedSaga_failedCompensationsListEmpty() {
        SagaInstance started = orchestrator.start(new SagaStartRequest(
                SAGA_TYPE, Map.of("orderId", "ORD-COMP-3"), "key-comp-3"));
        String sagaId = started.getSagaId();

        await().atMost(Duration.ofSeconds(60))
                .until(() -> instances.findBySagaId(sagaId)
                        .map(s -> s.getStatus() == SagaStatus.COMPENSATED)
                        .orElse(false));

        // AC-3: when all compensations succeeded, failedCompensations stays empty
        SagaInstance finalState = instances.findBySagaId(sagaId).orElseThrow();
        assertThat(finalState.getFailedCompensations()).isEmpty();
    }

    private static int indexOf(List<SagaExecutionLog> timeline, String stepName, String event) {
        for (int i = 0; i < timeline.size(); i++) {
            SagaExecutionLog log = timeline.get(i);
            if (stepName.equals(log.getStepName()) && event.equals(log.getEvent())) return i;
        }
        return -1;
    }

    // ── Test wiring ──────────────────────────────────────────────────────────

    @Configuration
    static class CompTestConfig {
        @Bean CompTestParticipant compTestParticipant(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
            return new CompTestParticipant(kafkaTemplate, objectMapper);
        }

        @Bean NewTopic compStepOneTopic()   { return new NewTopic(STEP_ONE_TOPIC, 1, (short) 1); }
        @Bean NewTopic compStepTwoTopic()   { return new NewTopic(STEP_TWO_TOPIC, 1, (short) 1); }
        @Bean NewTopic compStepThreeTopic() { return new NewTopic(STEP_THREE_TOPIC, 1, (short) 1); }
        @Bean NewTopic compReplyTopic()     { return new NewTopic(SagaHeaders.REPLY_TOPIC, 1, (short) 1); }
    }

    /**
     * Synthetic participant for the 3-step compensation saga.
     *
     * Forward actions:  ACT_ONE → SUCCESS, ACT_TWO → SUCCESS, ACT_THREE → BUSINESS failure.
     * Compensation actions: UNDO_ONE → SUCCESS, UNDO_TWO → SUCCESS.
     */
    static class CompTestParticipant {
        private final KafkaTemplate<String, String> kafkaTemplate;
        private final ObjectMapper objectMapper;

        CompTestParticipant(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
            this.kafkaTemplate = kafkaTemplate;
            this.objectMapper = objectMapper;
        }

        @KafkaListener(topics = STEP_ONE_TOPIC, groupId = "it-comp-participant-step-one",
                containerFactory = "sagaReplyListenerContainerFactory")
        public void onStepOne(ConsumerRecord<String, String> record) { handle(record); }

        @KafkaListener(topics = STEP_TWO_TOPIC, groupId = "it-comp-participant-step-two",
                containerFactory = "sagaReplyListenerContainerFactory")
        public void onStepTwo(ConsumerRecord<String, String> record) { handle(record); }

        @KafkaListener(topics = STEP_THREE_TOPIC, groupId = "it-comp-participant-step-three",
                containerFactory = "sagaReplyListenerContainerFactory")
        public void onStepThree(ConsumerRecord<String, String> record) { handle(record); }

        private void handle(ConsumerRecord<String, String> record) {
            try {
                SagaCommand cmd = objectMapper.readValue(record.value(), SagaCommand.class);
                SagaReply reply = "ACT_THREE".equals(cmd.action())
                        ? SagaReply.businessFailure(cmd, "simulated business failure at step 3")
                        : SagaReply.success(cmd, Map.of("acked", true, "action", cmd.action()));
                ProducerRecord<String, String> out = new ProducerRecord<>(
                        SagaHeaders.REPLY_TOPIC, cmd.sagaId(),
                        objectMapper.writeValueAsString(reply));
                SagaHeaders.setReplyHeaders(out, reply);
                kafkaTemplate.send(out);
            } catch (Exception e) {
                throw new RuntimeException("CompTestParticipant failed to reply", e);
            }
        }
    }
}
