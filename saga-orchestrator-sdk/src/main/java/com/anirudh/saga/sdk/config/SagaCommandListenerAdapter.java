package com.anirudh.saga.sdk.config;

import com.anirudh.saga.sdk.contract.*;
import com.anirudh.saga.sdk.exception.SagaBusinessException;
import com.anirudh.saga.sdk.exception.SagaTechnicalException;
import com.anirudh.saga.sdk.idempotency.ProcessedCommand;
import com.anirudh.saga.sdk.idempotency.ProcessedCommandRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class SagaCommandListenerAdapter {

    private static final Logger log = LoggerFactory.getLogger(SagaCommandListenerAdapter.class);

    private final Object handlerBean;
    private final List<SagaCommandHandlerRegistrar.ActionHandler> handlers;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final ProcessedCommandRepository processedCommandRepository;

    public SagaCommandListenerAdapter(
            Object handlerBean,
            List<SagaCommandHandlerRegistrar.ActionHandler> handlers,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            ProcessedCommandRepository processedCommandRepository) {
        this.handlerBean = handlerBean;
        this.handlers = handlers;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.processedCommandRepository = processedCommandRepository;
    }

    public void onMessage(ConsumerRecord<String, String> record) {
        SagaCommand command = null;
        try {
            command = objectMapper.readValue(record.value(), SagaCommand.class);
            log.info("[sagaId={}] Received action={}", command.sagaId(), command.action());

            SagaCommandHandlerRegistrar.ActionHandler handler = resolveHandler(command.action());
            if (handler == null) {
                log.warn("[sagaId={}] No handler for action={}", command.sagaId(), command.action());
                sendReply(command, SagaReply.technicalFailure(command, "No handler for action: " + command.action()));
                return;
            }

            // Idempotency check
            if (handler.idempotent() && processedCommandRepository != null) {
                String cmdKey = ProcessedCommand.buildKey(command.sagaId(), command.stepId(), command.action());
                Optional<ProcessedCommand> cached = processedCommandRepository.findById(cmdKey);
                if (cached.isPresent()) {
                    log.info("[sagaId={}] Idempotent hit — returning cached reply", command.sagaId());
                    sendRawReply(command.sagaId(), cached.get().getReplyJson());
                    return;
                }
            }

            SagaReply reply = (SagaReply) handler.method().invoke(handlerBean, command);

            // Store for idempotency
            if (handler.idempotent() && processedCommandRepository != null) {
                String cmdKey = ProcessedCommand.buildKey(command.sagaId(), command.stepId(), command.action());
                String replyJson = objectMapper.writeValueAsString(reply);
                processedCommandRepository.save(new ProcessedCommand(cmdKey, replyJson, Instant.now()));
            }

            sendReply(command, reply);

        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            log.error("[sagaId={}] Handler failed: {} ({})",
                    command != null ? command.sagaId() : "unknown",
                    cause.getMessage(), cause.getClass().getSimpleName());

            if (command != null) {
                SagaReply reply = classifyException(command, cause);
                sendReply(command, reply);
            }
        }
    }

    /**
     * Intelligent exception classification:
     * - SagaBusinessException → businessFailure (compensate)
     * - SagaTechnicalException → technicalFailure (retry → suspend)
     * - IllegalArgumentException, IllegalStateException → businessFailure (bad input = business problem)
     * - NullPointerException, ClassCastException → technicalFailure (code bug)
     * - IOException, TimeoutException, ConnectException → technicalFailure (infra problem)
     * - Everything else → technicalFailure (safe default)
     */
    private SagaReply classifyException(SagaCommand command, Throwable cause) {
        if (cause instanceof SagaBusinessException) {
            log.info("[sagaId={}] Classified as BUSINESS failure: {}", command.sagaId(), cause.getMessage());
            return SagaReply.businessFailure(command, cause.getMessage());
        }

        if (cause instanceof SagaTechnicalException) {
            log.warn("[sagaId={}] Classified as TECHNICAL failure: {}", command.sagaId(), cause.getMessage());
            return SagaReply.technicalFailure(command, cause.getMessage());
        }

        if (cause instanceof IllegalArgumentException || cause instanceof IllegalStateException) {
            log.info("[sagaId={}] Classified as BUSINESS failure (bad input): {}", command.sagaId(), cause.getMessage());
            return SagaReply.businessFailure(command, cause.getMessage());
        }

        log.warn("[sagaId={}] Classified as TECHNICAL failure (default): {} — {}",
                command.sagaId(), cause.getClass().getSimpleName(), cause.getMessage());
        return SagaReply.technicalFailure(command, cause.getClass().getSimpleName() + ": " + cause.getMessage());
    }

    private SagaCommandHandlerRegistrar.ActionHandler resolveHandler(String action) {
        for (var h : handlers) {
            if (!h.action().isEmpty() && h.action().equals(action)) return h;
        }
        for (var h : handlers) {
            if (h.action().isEmpty()) return h;
        }
        return null;
    }

    private void sendReply(SagaCommand command, SagaReply reply) {
        try {
            String replyJson = objectMapper.writeValueAsString(reply);
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(SagaHeaders.REPLY_TOPIC, command.sagaId(), replyJson);
            SagaHeaders.setReplyHeaders(producerRecord, reply);
            kafkaTemplate.send(producerRecord);
            log.info("[sagaId={}] Reply sent status={}", command.sagaId(), reply.status());
        } catch (Exception e) {
            log.error("[sagaId={}] Failed to send reply: {}", command.sagaId(), e.getMessage(), e);
        }
    }

    private void sendRawReply(String sagaId, String replyJson) {
        try {
            SagaReply reply = objectMapper.readValue(replyJson, SagaReply.class);
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(SagaHeaders.REPLY_TOPIC, sagaId, replyJson);
            SagaHeaders.setReplyHeaders(producerRecord, reply);
            kafkaTemplate.send(producerRecord);
        } catch (Exception e) {
            log.error("[sagaId={}] Failed to send cached reply: {}", sagaId, e.getMessage(), e);
        }
    }
}
