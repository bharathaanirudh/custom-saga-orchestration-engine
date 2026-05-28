package com.anirudh.saga.core.outbox;

import com.anirudh.saga.core.repository.SagaOutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;

@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final SagaOutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Clock clock;
    private final long fallbackThresholdMinutes;
    private final PublisherMode publisherMode;
    private final MeterRegistry meterRegistry;
    private final int maxErrorLength;
    // Memoized counter — looked up once at @PostConstruct (P2-007 AC-4).
    private Counter publishFailedCounter;

    public OutboxPoller(SagaOutboxRepository outboxRepository,
                        KafkaTemplate<String, String> kafkaTemplate,
                        Clock clock,
                        @Value("${saga.outbox.fallback-threshold-minutes:5}") long fallbackThresholdMinutes,
                        @Value("${saga.outbox.publisher.mode:DEBEZIUM_PRIMARY}") PublisherMode publisherMode,
                        @Value("${saga.outbox.max-error-length:500}") int maxErrorLength,
                        MeterRegistry meterRegistry) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.clock = clock;
        this.fallbackThresholdMinutes = fallbackThresholdMinutes;
        this.publisherMode = publisherMode;
        this.maxErrorLength = maxErrorLength;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    void registerMeters() {
        // Mode gauge — value = 1 (constant), the meaningful signal is the {mode=...} label.
        Gauge.builder("saga.outbox.publisher.mode", () -> 1.0)
                .description("Outbox publisher strategy active on this instance")
                .tags(Tags.of("mode", publisherMode.name()))
                .register(meterRegistry);

        // P2-007 AC-4: publish-failure counter, tagged by publisher mode only
        // (no topic / sagaId tags — cardinality discipline).
        publishFailedCounter = Counter.builder("saga.outbox.publish.failed")
                .description("OutboxPoller.send() failures since process start")
                .tags(Tags.of("publisherMode", publisherMode.name()))
                .register(meterRegistry);

        // P2-007 AC-4: max retry count across all PENDING entries, sampled on
        // every Prometheus scrape via an independent Mongo aggregation query.
        // Independent of the mode threshold so freshly-failing entries are visible
        // even in DEBEZIUM_PRIMARY mode (where the poll list only includes >5min old).
        // Supplier closure (not the (T, ToDoubleFunction<T>) overload) — that overload
        // holds T as a WeakReference and can return 0 if GC clears the ref mid-test.
        Gauge.builder("saga.outbox.max.retry.count",
                        () -> (double) outboxRepository.findMaxRetryCountAmongPending())
                .description("Highest retryCount across PENDING outbox entries (0 when none)")
                .register(meterRegistry);

        log.info("OutboxPoller publisher mode = {}", publisherMode);
    }

    private String truncate(String message) {
        if (message == null) return null;
        if (message.length() <= maxErrorLength) return message;
        return message.substring(0, maxErrorLength - 1) + "…";
    }

    @Scheduled(fixedDelayString = "${saga.outbox.poll-interval-ms:30000}")
    public void poll() {
        // Mode-driven threshold:
        //   DEBEZIUM_PRIMARY: only entries older than fallback-threshold (Debezium failed)
        //   POLLER_ONLY: every PENDING entry — poll-interval bounds latency
        Instant threshold = (publisherMode == PublisherMode.POLLER_ONLY)
                ? Instant.now(clock)                                                  // any PENDING entry
                : Instant.now(clock).minusSeconds(fallbackThresholdMinutes * 60);    // fallback only
        List<OutboxEntry> pending = outboxRepository.findPendingOlderThan(threshold);

        if (pending.isEmpty()) return;

        if (publisherMode == PublisherMode.POLLER_ONLY) {
            log.debug("OutboxPoller publishing {} PENDING entries (POLLER_ONLY)", pending.size());
        } else {
            log.warn("OutboxPoller found {} PENDING entries older than {}min — Debezium fallback",
                    pending.size(), fallbackThresholdMinutes);
        }

        for (OutboxEntry entry : pending) {
            try {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        entry.getTopic(), entry.getMessageKey(), entry.getPayload());
                // Attach saga headers — ReplyCorrelator reads these
                record.headers().add(new RecordHeader("X-Saga-Id",
                        entry.getSagaId().getBytes(StandardCharsets.UTF_8)));
                record.headers().add(new RecordHeader("X-Saga-Step-Id",
                        entry.getStepId().getBytes(StandardCharsets.UTF_8)));

                kafkaTemplate.send(record);
                entry.setStatus(OutboxStatus.PUBLISHED);
                outboxRepository.save(entry);
                log.info("[sagaId={}] Outbox poller published stepId={} (mode={})",
                        entry.getSagaId(), entry.getStepId(), publisherMode);
            } catch (Exception e) {
                // P2-007 AC-2: record failure on the entry + persist so operators
                // see retry history without grepping logs. Entry stays PENDING
                // so the next poll cycle re-attempts.
                entry.setRetryCount(entry.getRetryCount() + 1);
                entry.setLastError(truncate(e.getMessage()));
                entry.setLastErrorAt(Instant.now(clock));
                outboxRepository.save(entry);
                publishFailedCounter.increment();
                log.error("[sagaId={}] Outbox poller publish failed stepId={} retryCount={}: {}",
                        entry.getSagaId(), entry.getStepId(), entry.getRetryCount(), e.getMessage());
            }
        }
    }
}
