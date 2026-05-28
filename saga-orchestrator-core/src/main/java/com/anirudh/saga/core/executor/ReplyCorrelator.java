package com.anirudh.saga.core.executor;

import com.anirudh.saga.core.engine.SagaOrchestrator;
import com.anirudh.saga.core.metrics.SagaMetrics;
import com.anirudh.saga.sdk.contract.SagaHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class ReplyCorrelator {

    private static final Logger log = LoggerFactory.getLogger(ReplyCorrelator.class);

    private final SagaOrchestrator orchestrator;
    private final ReplyRateLimiter rateLimiter;
    private final SagaMetrics metrics;

    public ReplyCorrelator(SagaOrchestrator orchestrator, ReplyRateLimiter rateLimiter, SagaMetrics metrics) {
        this.orchestrator = orchestrator;
        this.rateLimiter = rateLimiter;
        this.metrics = metrics;
    }

    @KafkaListener(topics = SagaHeaders.REPLY_TOPIC,
            groupId = "${saga.kafka.consumer.group-id:saga-orchestrator}",
            containerFactory = "sagaReplyListenerContainerFactory")
    public void onReply(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String sagaId  = SagaHeaders.get(record.headers(), SagaHeaders.SAGA_ID);
        String stepId  = SagaHeaders.get(record.headers(), SagaHeaders.STEP_ID);
        String status  = SagaHeaders.get(record.headers(), SagaHeaders.STATUS);
        String failure = SagaHeaders.get(record.headers(), SagaHeaders.FAILURE_TYPE);

        if (sagaId == null || stepId == null) {
            log.warn("Reply with missing saga headers — skipping. key={}", record.key());
            ack.acknowledge();
            return;
        }

        // P2-061 AC-5: throttle floods of replies for the same sagaId.
        if (!rateLimiter.allow(sagaId)) {
            log.warn("[sagaId={}] Reply rate-limited (>{}/sec); dropping stepId={}", sagaId,
                    "configured cap", stepId);
            metrics.recordThrottledReply();
            ack.acknowledge();
            return;
        }

        log.info("[sagaId={}] Reply received stepId={} status={}", sagaId, stepId, status);

        boolean success = "SUCCESS".equalsIgnoreCase(status);
        try {
            orchestrator.handleReply(sagaId, stepId, success, failure, record.value());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("[sagaId={}] Failed to handle reply: {}", sagaId, e.getMessage(), e);
        }
    }
}
