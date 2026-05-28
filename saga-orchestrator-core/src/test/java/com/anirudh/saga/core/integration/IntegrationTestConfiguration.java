package com.anirudh.saga.core.integration;

import com.anirudh.saga.core.SagaOrchestratorAutoConfiguration;
import com.anirudh.saga.sdk.config.SagaSdkAutoConfiguration;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Map;

/**
 * Minimal Spring Boot configuration root for integration tests.
 *
 * The engine ships only an @AutoConfiguration class — no @SpringBootApplication
 * — so tests need an explicit @SpringBootConfiguration to bootstrap the context.
 *
 * @EnableKafka is required because SagaOrchestratorAutoConfiguration excludes
 * KafkaAutoConfiguration (which would normally enable the listener registry).
 * Without it, SDK's SagaCommandHandlerRegistrar cannot wire
 * KafkaListenerEndpointRegistry.
 */
@SpringBootConfiguration
@EnableAutoConfiguration(exclude = SagaSdkAutoConfiguration.class)
@EnableKafka
@EnableScheduling
@Import(SagaOrchestratorAutoConfiguration.class)
public class IntegrationTestConfiguration {

    /**
     * KafkaAutoConfiguration is excluded by the engine, so {@link KafkaAdmin}
     * (which materializes {@link org.apache.kafka.clients.admin.NewTopic} beans
     * at startup) must be declared explicitly. Required for ITs that pre-create
     * topics to avoid consumer-assignment races.
     */
    @Bean
    public KafkaAdmin kafkaAdmin(@Value("${saga.kafka.bootstrap-servers}") String bootstrapServers) {
        return new KafkaAdmin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }
}
