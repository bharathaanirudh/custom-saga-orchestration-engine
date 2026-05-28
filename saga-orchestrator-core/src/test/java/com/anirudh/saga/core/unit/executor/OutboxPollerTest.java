package com.anirudh.saga.core.unit.executor;

import com.anirudh.saga.core.outbox.OutboxEntry;
import com.anirudh.saga.core.outbox.OutboxPoller;
import com.anirudh.saga.core.outbox.OutboxStatus;
import com.anirudh.saga.core.outbox.PublisherMode;
import com.anirudh.saga.core.repository.SagaOutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxPollerTest {

    @Mock SagaOutboxRepository outboxRepository;
    @Mock KafkaTemplate<String, String> kafkaTemplate;

    OutboxPoller poller;
    Clock fixedClock;

    @BeforeEach
    void setUp() {
        fixedClock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        poller = new OutboxPoller(outboxRepository, kafkaTemplate, fixedClock, 5L,
                PublisherMode.DEBEZIUM_PRIMARY, 500, new SimpleMeterRegistry());
    }

    @Test
    void poll_noPending_doesNothing() {
        when(outboxRepository.findPendingOlderThan(any())).thenReturn(List.of());
        poller.poll();
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }

    @Test
    void poll_pendingEntry_publishesWithHeaders() {
        OutboxEntry entry = OutboxEntry.create("saga-1", "step-1", "topic-a", "key", "payload", Instant.now());
        when(outboxRepository.findPendingOlderThan(any())).thenReturn(List.of(entry));

        poller.poll();

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());

        ProducerRecord<String, String> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("topic-a");
        assertThat(record.headers().lastHeader("X-Saga-Id")).isNotNull();
        assertThat(record.headers().lastHeader("X-Saga-Step-Id")).isNotNull();
    }

    @Test
    void poll_afterPublish_marksPublished() {
        OutboxEntry entry = OutboxEntry.create("saga-1", "step-1", "topic-a", "key", "payload", Instant.now());
        when(outboxRepository.findPendingOlderThan(any())).thenReturn(List.of(entry));

        poller.poll();

        assertThat(entry.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        verify(outboxRepository).save(entry);
    }
}
