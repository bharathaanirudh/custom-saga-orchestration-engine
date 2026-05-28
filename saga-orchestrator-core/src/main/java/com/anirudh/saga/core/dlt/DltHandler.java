package com.anirudh.saga.core.dlt;

import com.anirudh.saga.core.engine.SagaOrchestrator;
import com.anirudh.saga.sdk.contract.SagaHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DltHandler {

    private static final Logger log = LoggerFactory.getLogger(DltHandler.class);

    private final SagaOrchestrator orchestrator;

    public DltHandler(SagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @KafkaListener(topicPattern = ".*\\.DLT",
            groupId = "${saga.kafka.consumer.dlt-group-id:saga-orchestrator-dlt}",
            containerFactory = "sagaDltListenerContainerFactory")
    public void onDltMessage(ConsumerRecord<String, String> record) {
        String sagaId  = SagaHeaders.get(record.headers(), SagaHeaders.SAGA_ID);
        String stepId  = SagaHeaders.get(record.headers(), SagaHeaders.STEP_ID);
        String failure = SagaHeaders.get(record.headers(), SagaHeaders.FAILURE_TYPE);

        if (sagaId == null) {
            log.warn("DLT message with no sagaId on topic={} — skipping", record.topic());
            return;
        }

        log.warn("[sagaId={}] DLT message topic={} stepId={} failureType={}", sagaId, record.topic(), stepId, failure);

        if ("BUSINESS".equalsIgnoreCase(failure)) {
            orchestrator.handleReply(sagaId, stepId, false, "BUSINESS", null);
        } else {
            orchestrator.handleReply(sagaId, stepId, false, "TECHNICAL", null);
        }
    }
}
