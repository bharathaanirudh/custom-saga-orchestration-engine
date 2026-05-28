package com.anirudh.saga.sdk.contract;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;

/**
 * Shared Kafka header constants and utilities used by both core and SDK.
 */
public final class SagaHeaders {

    public static final String SAGA_ID     = "X-Saga-Id";
    public static final String STEP_ID     = "X-Saga-Step-Id";
    public static final String STATUS      = "X-Saga-Status";
    public static final String FAILURE_TYPE = "X-Failure-Type";
    public static final String REPLY_TOPIC = "saga-replies";

    private SagaHeaders() {}

    public static String get(Headers headers, String name) {
        Header h = headers.lastHeader(name);
        return h != null ? new String(h.value(), StandardCharsets.UTF_8) : null;
    }

    public static void set(ProducerRecord<?, ?> record, String name, String value) {
        if (value != null) {
            record.headers().add(new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8)));
        }
    }

    public static void setReplyHeaders(ProducerRecord<?, ?> record, SagaReply reply) {
        set(record, SAGA_ID, reply.sagaId());
        set(record, STEP_ID, reply.stepId());
        set(record, STATUS, reply.status());
        if (reply.failureType() != null) {
            set(record, FAILURE_TYPE, reply.failureType().name());
        }
    }
}
