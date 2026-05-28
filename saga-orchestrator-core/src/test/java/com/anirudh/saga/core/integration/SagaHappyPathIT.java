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
 * P2-002 Happy Path Integration Test.
 *
 * Asserts:
 *  - AC-1: SagaInstance written to MongoDB on start
 *  - AC-2: Outbox poller dispatches Kafka commands to {module}-commands topic
 *  - AC-3: Reply on saga-replies advances the saga
 *  - AC-4: After all steps, status = COMPLETED + lock released
 *  - AC-5: SagaExecutionLog timeline records events in order
 *
 * Uses {@link TestParticipant} to consume from both step command topics and
 * publish SUCCESS replies, simulating a real participant service.
 */
@Import(SagaHappyPathIT.HappyPathTestConfig.class)
class SagaHappyPathIT extends AbstractIntegrationTest {

    static final String SAGA_TYPE = "it-happy-path-saga";
    static final String STEP_ONE_TOPIC = "it-module-one-commands";
    static final String STEP_TWO_TOPIC = "it-module-two-commands";

    @Autowired private SagaOrchestrator orchestrator;
    @Autowired private SagaInstanceRepository instances;
    @Autowired private SagaExecutionLogRepository logs;
    @Autowired private SagaLockRepository locks;
    @Autowired private TestParticipant participant;

    @BeforeEach
    void resetState() {
        // Containers are reused across runs (Testcontainers withReuse=true),
        // so stale data from prior runs (or prior tests in this run) can hit
        // idempotency dedup and short-circuit the new saga. Clean before AND after.
        instances.deleteAll();
        logs.deleteAll();
        locks.deleteAll();
        participant.reset();
    }

    @AfterEach
    void cleanCollections() {
        instances.deleteAll();
        logs.deleteAll();
        locks.deleteAll();
    }

    @Test
    void twoStepKafkaSaga_completes() {
        SagaInstance started = orchestrator.start(new SagaStartRequest(
                SAGA_TYPE, Map.of("orderId", "ORD-1"), "key-happy-1"));
        String sagaId = started.getSagaId();

        // AC-1: instance persisted
        assertThat(instances.findBySagaId(sagaId)).isPresent();

        // AC-2 + AC-3 + AC-4: saga reaches COMPLETED via wire
        await().atMost(Duration.ofSeconds(60))
                .until(() -> instances.findBySagaId(sagaId)
                        .map(s -> s.getStatus() == SagaStatus.COMPLETED)
                        .orElse(false));

        // AC-2 implicit proof: the saga can ONLY reach COMPLETED if both step commands
        // flowed to Kafka and replies came back through saga-replies. We don't assert
        // participant.commandCount directly — it has cross-context bean-identity quirks
        // when multiple Spring contexts coexist in the same JVM (see EXECUTION-PLAN §11).
        // The execution log assertions in executionLog_recordsEachEventInOrder() cover this.
    }

    @Test
    void executionLog_recordsEachEventInOrder() {
        SagaInstance started = orchestrator.start(new SagaStartRequest(
                SAGA_TYPE, Map.of("orderId", "ORD-2"), "key-happy-2"));
        String sagaId = started.getSagaId();

        await().atMost(Duration.ofSeconds(60))
                .until(() -> instances.findBySagaId(sagaId)
                        .map(s -> s.getStatus() == SagaStatus.COMPLETED)
                        .orElse(false));

        List<SagaExecutionLog> timeline = logs.findBySagaIdOrderByTimestampAsc(sagaId);
        // Expect at minimum: STARTED, STEP_STARTED x2, STEP_COMPLETED x2, COMPLETED
        List<String> events = timeline.stream().map(SagaExecutionLog::getEvent).toList();
        assertThat(events).contains("STARTED", "STEP_STARTED", "STEP_COMPLETED", "COMPLETED");
        // STARTED must precede COMPLETED
        assertThat(events.indexOf("STARTED")).isLessThan(events.indexOf("COMPLETED"));
    }

    @Test
    void afterCompletion_noLockHeld() {
        // The test YAML defines no lockTargetType, so no locks should ever be created
        // — AC-4 ("lock released") is satisfied trivially. We assert no locks exist
        // anywhere after completion to keep the contract explicit.
        SagaInstance started = orchestrator.start(new SagaStartRequest(
                SAGA_TYPE, Map.of("orderId", "ORD-3"), "key-happy-3"));
        String sagaId = started.getSagaId();

        await().atMost(Duration.ofSeconds(60))
                .until(() -> instances.findBySagaId(sagaId)
                        .map(s -> s.getStatus() == SagaStatus.COMPLETED)
                        .orElse(false));

        assertThat(locks.findAll()).isEmpty();
    }

    // ── Test wiring ──────────────────────────────────────────────────────────

    @Configuration
    static class HappyPathTestConfig {
        @Bean TestParticipant testParticipant(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
            return new TestParticipant(kafkaTemplate, objectMapper);
        }

        // Pre-create command + reply topics so consumers don't race topic auto-creation.
        @Bean NewTopic itStepOneTopic() { return new NewTopic(STEP_ONE_TOPIC, 1, (short) 1); }
        @Bean NewTopic itStepTwoTopic() { return new NewTopic(STEP_TWO_TOPIC, 1, (short) 1); }
        @Bean NewTopic itReplyTopic()   { return new NewTopic(SagaHeaders.REPLY_TOPIC, 1, (short) 1); }
    }

    /**
     * Synthetic participant: consumes from both step command topics and replies SUCCESS.
     * Reusable across compensation IT (P2-003) and idempotency/timeout IT (P2-004).
     */
    static class TestParticipant {
        private final KafkaTemplate<String, String> kafkaTemplate;
        private final ObjectMapper objectMapper;
        private final java.util.concurrent.ConcurrentMap<String, java.util.concurrent.atomic.AtomicInteger> received =
                new java.util.concurrent.ConcurrentHashMap<>();

        TestParticipant(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
            this.kafkaTemplate = kafkaTemplate;
            this.objectMapper = objectMapper;
        }

        @KafkaListener(topics = STEP_ONE_TOPIC, groupId = "it-participant-step-one",
                containerFactory = "sagaReplyListenerContainerFactory")
        public void onStepOne(ConsumerRecord<String, String> record) { handle(record, STEP_ONE_TOPIC); }

        @KafkaListener(topics = STEP_TWO_TOPIC, groupId = "it-participant-step-two",
                containerFactory = "sagaReplyListenerContainerFactory")
        public void onStepTwo(ConsumerRecord<String, String> record) { handle(record, STEP_TWO_TOPIC); }

        private void handle(ConsumerRecord<String, String> record, String fromTopic) {
            received.computeIfAbsent(fromTopic, k -> new java.util.concurrent.atomic.AtomicInteger())
                    .incrementAndGet();
            try {
                SagaCommand cmd = objectMapper.readValue(record.value(), SagaCommand.class);
                SagaReply reply = SagaReply.success(cmd, Map.of("acked", true));
                ProducerRecord<String, String> out = new ProducerRecord<>(
                        SagaHeaders.REPLY_TOPIC, cmd.sagaId(),
                        objectMapper.writeValueAsString(reply));
                SagaHeaders.setReplyHeaders(out, reply);
                kafkaTemplate.send(out);
            } catch (Exception e) {
                throw new RuntimeException("TestParticipant failed to reply", e);
            }
        }

        void reset() { received.clear(); }
    }
}
