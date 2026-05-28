package com.anirudh.saga.core.integration;

import com.anirudh.saga.core.outbox.OutboxEntry;
import com.anirudh.saga.core.outbox.OutboxPoller;
import com.anirudh.saga.core.outbox.OutboxStatus;
import com.anirudh.saga.core.repository.SagaOutboxRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

/**
 * P2-007 / E2-22 — outbox poller retry counter, last-error visibility,
 * publish-failed counter, and max-retry-count gauge.
 *
 * <p>No real Kafka broker stop — {@link KafkaTemplate} is mocked at the
 * Spring level so we control success/failure precisely. The poller is
 * invoked from the test thread; the scheduled cadence is pushed to ~10
 * minutes to prevent the background scheduler racing with explicit invocations
 * (would otherwise double-increment counters and retryCount).
 */
@TestPropertySource(properties = "saga.outbox.poll-interval-ms=600000")
class OutboxPollerRetryIT extends AbstractIntegrationTest {

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired private OutboxPoller outboxPoller;
    @Autowired private SagaOutboxRepository outboxRepository;
    @Autowired private MeterRegistry meterRegistry;
    @Autowired private Clock clock;

    @AfterEach
    void cleanupOutbox() {
        outboxRepository.deleteAll();
    }

    @Test
    void ac2_publishFailure_incrementsRetryCount_persistsLastError_incrementsCounter() {
        given(kafkaTemplate.send(any(ProducerRecord.class)))
                .willThrow(new RuntimeException("broker unreachable: simulated"));

        OutboxEntry entry = seedPending();
        double counterBefore = failedCounterValue();

        outboxPoller.poll();

        OutboxEntry reloaded = outboxRepository.findById(entry.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).as("entry stays PENDING on failure").isEqualTo(OutboxStatus.PENDING);
        assertThat(reloaded.getRetryCount()).as("retryCount incremented").isEqualTo(1);
        assertThat(reloaded.getLastError()).as("lastError populated").contains("broker unreachable: simulated");
        assertThat(reloaded.getLastErrorAt()).as("lastErrorAt populated").isNotNull();
        assertThat(failedCounterValue() - counterBefore)
                .as("saga.outbox.publish.failed incremented").isEqualTo(1.0);
    }

    @Test
    void ac3_retriedEntryPublishesEventually_preservesRetryCount() {
        // First poll → fail
        given(kafkaTemplate.send(any(ProducerRecord.class)))
                .willThrow(new RuntimeException("transient: simulated"));
        OutboxEntry entry = seedPending();
        outboxPoller.poll();

        OutboxEntry afterFailure = outboxRepository.findById(entry.getId()).orElseThrow();
        assertThat(afterFailure.getRetryCount()).isEqualTo(1);
        assertThat(afterFailure.getStatus()).isEqualTo(OutboxStatus.PENDING);

        // Second poll → succeed (un-stub by re-stubbing default behavior — return null,
        // which KafkaTemplate.send() treats as fire-and-forget)
        Mockito.reset(kafkaTemplate);
        given(kafkaTemplate.send(any(ProducerRecord.class))).willReturn(null);

        outboxPoller.poll();

        OutboxEntry afterSuccess = outboxRepository.findById(entry.getId()).orElseThrow();
        assertThat(afterSuccess.getStatus())
                .as("entry moves to PUBLISHED after successful retry")
                .isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(afterSuccess.getRetryCount())
                .as("retryCount retained for audit, NOT reset on success")
                .isEqualTo(1);
        assertThat(afterSuccess.getLastError())
                .as("lastError retained for audit, NOT cleared on success")
                .contains("transient: simulated");
    }

    @Test
    void ac4_maxRetryCountGauge_reflectsHighestPendingRetryCount() {
        // Seed three PENDING entries with retryCounts 0 / 3 / 7
        seedPendingWithRetryCount(0);
        seedPendingWithRetryCount(3);
        seedPendingWithRetryCount(7);

        // Gauge is registered with a Supplier — every read calls the repository
        double gaugeValue = meterRegistry.find("saga.outbox.max.retry.count").gauge().value();
        assertThat((int) gaugeValue)
                .as("gauge reports max retryCount across PENDING entries")
                .isEqualTo(7);

        // Also: an entry that's already PUBLISHED with high retryCount must NOT
        // influence the gauge — the metric is about *currently stuck* entries.
        OutboxEntry published = seedPendingWithRetryCount(99);
        published.setStatus(OutboxStatus.PUBLISHED);
        outboxRepository.save(published);

        assertThat((int) meterRegistry.find("saga.outbox.max.retry.count").gauge().value())
                .as("PUBLISHED entries ignored — only PENDING contribute to gauge")
                .isEqualTo(7);
    }

    // --- helpers ---

    private OutboxEntry seedPending() {
        OutboxEntry entry = OutboxEntry.create(
                UUID.randomUUID().toString(),
                "step-1",
                "test-commands",
                "key-" + System.nanoTime(),
                "{\"action\":\"TEST\"}",
                Instant.now(clock).minusSeconds(60)  // older than fallback threshold (0 in IT)
        );
        return outboxRepository.save(entry);
    }

    private OutboxEntry seedPendingWithRetryCount(int retryCount) {
        OutboxEntry entry = seedPending();
        entry.setRetryCount(retryCount);
        return outboxRepository.save(entry);
    }

    private double failedCounterValue() {
        return Optional.ofNullable(meterRegistry.find("saga.outbox.publish.failed").counter())
                .map(c -> c.count())
                .orElse(0.0);
    }
}
