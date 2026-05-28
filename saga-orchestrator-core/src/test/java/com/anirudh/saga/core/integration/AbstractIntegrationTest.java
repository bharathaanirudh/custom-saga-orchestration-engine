package com.anirudh.saga.core.integration;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared base for saga engine integration tests.
 *
 * Singleton-container pattern: containers are static and started once per JVM,
 * reused across every test class that extends this base. This is the
 * Testcontainers-recommended approach for fast suite times.
 *
 * Provides:
 *  - MongoDB 7.0 single-node replica set (multi-doc transactions enabled)
 *  - Kafka (Confluent cp-kafka 7.5.0)
 *  - Spring properties wired via @DynamicPropertySource
 */
@SpringBootTest(classes = IntegrationTestConfiguration.class)
@Testcontainers
@TestPropertySource(properties = {
        // Production runs Debezium as primary publisher; in tests the poller IS the
        // publisher, so we drop the fallback threshold to 0 across ALL ITs. This is
        // critical because multiple test contexts share the saga-orchestrator consumer
        // group — a reply may be routed to a sibling context whose poller would otherwise
        // wait 5 minutes before publishing the next step's outbox entry.
        "saga.outbox.fallback-threshold-minutes=0",
        // Faster outbox + step-timeout polls so any anomaly surfaces in test timeframe.
        "saga.outbox.poll-interval-ms=200",
        "saga.timeout.poll-interval-ms=2000",
        // RetentionCheck (P2-035) requires the saga-replies topic to exist at boot;
        // some ITs don't pre-create it, and broker retention is irrelevant in tests.
        "saga.outbox.publisher.retention-check=skip"
})
public abstract class AbstractIntegrationTest {

    protected static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                    .withReuse(true);

    @SuppressWarnings("resource")
    protected static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
                    .withReuse(true);

    static {
        MONGO.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("saga.mongodb.uri", MONGO::getReplicaSetUrl);
        registry.add("saga.mongodb.database", () -> "saga-test");
        registry.add("saga.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        // Unique consumer group per Spring context. Multiple IT contexts coexist in
        // the same JVM and would otherwise compete for the saga-replies and DLT
        // partitions — a reply for context A's saga could be routed to context B's
        // ReplyCorrelator, mutating the saga via B's clock/metrics/etc.
        String suffix = java.util.UUID.randomUUID().toString().substring(0, 8);
        registry.add("saga.kafka.consumer.group-id", () -> "saga-orchestrator-it-" + suffix);
        registry.add("saga.kafka.consumer.dlt-group-id", () -> "saga-orchestrator-dlt-it-" + suffix);
    }
}
