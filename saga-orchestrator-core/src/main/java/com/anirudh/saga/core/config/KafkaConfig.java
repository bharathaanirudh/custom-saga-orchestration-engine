package com.anirudh.saga.core.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

// @EnableKafka — provides KafkaListenerEndpointRegistry, which the SDK's
// SagaCommandHandlerRegistrar requires. Spring Boot's KafkaAutoConfiguration
// would normally supply it, but the engine excludes that to wire its own
// producer/consumer beans. So we re-enable Kafka annotation processing here.
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${saga.kafka.bootstrap-servers:localhost:9192}")
    private String bootstrapServers;

    @Value("${saga.kafka.consumer.group-id:saga-orchestrator}")
    private String consumerGroupId;

    @Value("${saga.kafka.listener.concurrency:1}")
    private int listenerConcurrency;

    @Value("${saga.kafka.retry.max-attempts:3}")
    private int retryMaxAttempts;

    @Value("${saga.kafka.retry.initial-interval-ms:1000}")
    private long retryInitialIntervalMs;

    @Value("${saga.kafka.retry.multiplier:2.0}")
    private double retryMultiplier;

    // ── Producer ─────────────────────────────────────────────────────────────

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ── Consumer ─────────────────────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ── Reply Listener Error Handler — exponential backoff → DLT ─────────────
    //
    // After `retryMaxAttempts` retries with exponentially increasing intervals,
    // the record is published to `<originalTopic>.DLT` and the offset is
    // committed so the consumer advances. DltHandler is the downstream consumer
    // of the .DLT topic pattern (see DltHandler).

    @Bean
    public DefaultErrorHandler sagaReplyErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));

        ExponentialBackOff backOff = new ExponentialBackOff(retryInitialIntervalMs, retryMultiplier);
        // saga.kafka.retry.max-attempts is total attempts including the initial delivery
        // (industry convention). ExponentialBackOff.setMaxAttempts counts retries only —
        // the initial delivery isn't a back-off step — so subtract one.
        if (retryMaxAttempts < 1) {
            throw new IllegalArgumentException(
                    "saga.kafka.retry.max-attempts must be >= 1; got " + retryMaxAttempts);
        }
        backOff.setMaxAttempts(retryMaxAttempts - 1);

        return new DefaultErrorHandler(recoverer, backOff);
    }

    // ── Reply Listener — MANUAL_IMMEDIATE ack ────────────────────────────────

    // §11 fix: marked @Primary so the SDK's SagaCommandHandlerRegistrar can autowire
    // ConcurrentKafkaListenerContainerFactory unambiguously (engine has two factories).
    @Bean("sagaReplyListenerContainerFactory")
    @org.springframework.context.annotation.Primary
    public ConcurrentKafkaListenerContainerFactory<String, String> sagaReplyListenerContainerFactory(
            DefaultErrorHandler sagaReplyErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(listenerConcurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(sagaReplyErrorHandler);
        return factory;
    }

    // ── DLT Listener — auto ack ──────────────────────────────────────────────

    @Bean("sagaDltListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> sagaDltListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
