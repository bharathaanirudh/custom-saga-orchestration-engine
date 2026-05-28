package com.anirudh.saga.core.outbox;

import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import com.anirudh.saga.core.repository.SagaOutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Component
public class OutboxWriter {

    private static final Logger log = LoggerFactory.getLogger(OutboxWriter.class);

    private final SagaOutboxRepository outboxRepository;
    private final SagaInstanceRepository sagaInstanceRepository;

    public OutboxWriter(SagaOutboxRepository outboxRepository, SagaInstanceRepository sagaInstanceRepository) {
        this.outboxRepository = outboxRepository;
        this.sagaInstanceRepository = sagaInstanceRepository;
    }

    /**
     * Writes OutboxEntry only — used by KafkaStepExecutor for command dispatch.
     * SagaInstance is already checkpointed by CheckpointStore before this call.
     */
    @Transactional
    public OutboxEntry write(String sagaId, String stepId, String topic, String messageKey, String payload, Instant now) {
        OutboxEntry entry = OutboxEntry.create(sagaId, stepId, topic, messageKey, payload, now);
        OutboxEntry saved = outboxRepository.save(entry);
        log.info("[sagaId={}] Outbox entry written stepId={} topic={}", sagaId, stepId, topic);
        return saved;
    }

    /**
     * Atomic write — SagaInstance checkpoint + OutboxEntry in one MongoDB transaction.
     * Guarantees: if either fails, both are rolled back.
     */
    @Transactional
    public OutboxEntry writeAtomic(SagaInstance instance, String stepId, String topic, String messageKey, String payload, Instant now) {
        // Save saga checkpoint
        instance.setUpdatedAt(now);
        sagaInstanceRepository.save(instance);

        // Save outbox entry — same transaction
        OutboxEntry entry = OutboxEntry.create(instance.getSagaId(), stepId, topic, messageKey, payload, now);
        OutboxEntry saved = outboxRepository.save(entry);

        log.info("[sagaId={}] Atomic write: checkpoint + outbox stepId={} topic={}", instance.getSagaId(), stepId, topic);
        return saved;
    }
}
