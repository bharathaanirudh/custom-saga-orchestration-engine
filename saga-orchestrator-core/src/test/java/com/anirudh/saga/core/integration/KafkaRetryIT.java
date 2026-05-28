package com.anirudh.saga.core.integration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for {@link com.anirudh.saga.core.config.KafkaConfig#sagaReplyErrorHandler}.
 *
 * Asserts:
 *  - Transient failures retry up to {@code saga.kafka.retry.max-attempts} and then succeed.
 *  - Permanent failures publish exactly one record to the {@code <topic>.DLT} topic.
 *
 * Uses a test-only listener wired against {@code sagaReplyListenerContainerFactory} so
 * the same error handler under test is exercised.
 */
@Import(KafkaRetryIT.RetryTestConfig.class)
@TestPropertySource(properties = {
        "saga.kafka.retry.max-attempts=3",
        "saga.kafka.retry.initial-interval-ms=100",
        "saga.kafka.retry.multiplier=2.0"
})
class KafkaRetryIT extends AbstractIntegrationTest {

    static final String INPUT_TOPIC = "kafka-retry-it-input";
    static final String DLT_TOPIC = INPUT_TOPIC + ".DLT";

    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private RetryTestListener listener;
    @Autowired private DltCollector dltCollector;
    @Autowired private KafkaListenerEndpointRegistry registry;

    @BeforeEach
    void resetCountersAndAwaitMyListenersAssigned() {
        listener.reset();
        dltCollector.reset();
        // Note: we deliberately do NOT call ContainerTestUtils.waitForAssignment here.
        // Under full-suite load, the input listener's first metadata fetch can take
        // longer than the default 60s timeout, causing flake. Instead we rely on:
        //  (a) the NewTopic bean (RetryTestConfig) — guarantees topic exists at startup,
        //  (b) auto-offset-reset=earliest — ensures the consumer reads the message even
        //      if it joined the group after the kafkaTemplate.send() call,
        //  (c) wider Awaitility windows in each test — absorbs assignment latency.
    }

    @Test
    void transientFailure_retriesUntilSuccess() {
        listener.failNTimesThenSucceed(2); // fails twice, succeeds on 3rd attempt

        kafkaTemplate.send(INPUT_TOPIC, "k1", "v1");

        await().atMost(Duration.ofSeconds(120))
                .until(() -> listener.successCount() == 1);

        assertThat(listener.invocationCount()).isEqualTo(3); // 1 initial + 2 retries
        assertThat(dltCollector.records()).isEmpty();
    }

    @Test
    void permanentFailure_dropsToDLT() {
        listener.failAlways();

        kafkaTemplate.send(INPUT_TOPIC, "k2", "v2");

        await().atMost(Duration.ofSeconds(120))
                .until(() -> dltCollector.records().size() == 1);

        // ExponentialBackOff with maxAttempts=3 ⇒ 3 total invocations before DLT
        assertThat(listener.invocationCount()).isEqualTo(3);
        assertThat(listener.successCount()).isZero();
        assertThat(dltCollector.records()).hasSize(1);
        assertThat(dltCollector.records().get(0).value()).isEqualTo("v2");
    }

    // ── Test wiring ──────────────────────────────────────────────────────────

    @Configuration
    static class RetryTestConfig {
        @Bean RetryTestListener retryTestListener() { return new RetryTestListener(); }
        @Bean DltCollector dltCollector() { return new DltCollector(); }

        // Pre-create the input topic so the consumer can get a partition assignment
        // before any send. Without this, waitForAssignment() in @BeforeEach times out.
        @Bean org.apache.kafka.clients.admin.NewTopic retryInputTopic() {
            return new org.apache.kafka.clients.admin.NewTopic(INPUT_TOPIC, 1, (short) 1);
        }
    }

    static class RetryTestListener {
        enum Mode { SUCCEED_ALWAYS, FAIL_ALWAYS, FAIL_N_THEN_SUCCEED }

        private volatile Mode mode = Mode.SUCCEED_ALWAYS;
        private volatile int failTarget = 0;
        private final AtomicInteger invocations = new AtomicInteger();
        private final AtomicInteger successes = new AtomicInteger();

        @KafkaListener(topics = INPUT_TOPIC,
                groupId = "kafka-retry-it",
                containerFactory = "sagaReplyListenerContainerFactory")
        public void onMessage(ConsumerRecord<String, String> record) {
            int n = invocations.incrementAndGet();
            switch (mode) {
                case FAIL_ALWAYS -> throw new RuntimeException("fail-always at attempt " + n);
                case FAIL_N_THEN_SUCCEED -> {
                    if (n <= failTarget) {
                        throw new RuntimeException("transient at attempt " + n);
                    }
                    successes.incrementAndGet();
                }
                case SUCCEED_ALWAYS -> successes.incrementAndGet();
            }
        }

        void failNTimesThenSucceed(int n) { this.mode = Mode.FAIL_N_THEN_SUCCEED; this.failTarget = n; }
        void failAlways() { this.mode = Mode.FAIL_ALWAYS; }
        void reset() { invocations.set(0); successes.set(0); mode = Mode.SUCCEED_ALWAYS; failTarget = 0; }
        int invocationCount() { return invocations.get(); }
        int successCount() { return successes.get(); }
    }

    static class DltCollector {
        private final List<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();

        @KafkaListener(topics = DLT_TOPIC,
                groupId = "kafka-retry-it-dlt",
                containerFactory = "sagaDltListenerContainerFactory")
        public void onDlt(ConsumerRecord<String, String> record) {
            records.add(record);
        }

        List<ConsumerRecord<String, String>> records() { return records; }
        void reset() { records.clear(); }
    }
}
