package com.anirudh.saga.core.integration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke test for {@link AbstractIntegrationTest}.
 *
 * Asserts the two non-trivial Testcontainers invariants:
 *  - AC-2: MongoDB replica set supports multi-document transactions
 *  - AC-3: Kafka producer can publish (proves bootstrap wiring + topic creation)
 */
class InfrastructureSmokeIT extends AbstractIntegrationTest {

    @Autowired private MongoTemplate mongoTemplate;
    @Autowired private MongoTransactionManager txManager;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void mongoReplicaSet_supportsMultiDocumentTransactions() {
        TransactionTemplate tx = new TransactionTemplate(txManager);
        String collection = "smoke-tx-" + UUID.randomUUID();

        tx.executeWithoutResult(status -> {
            mongoTemplate.insert(new SmokeDoc("a", 1), collection);
            mongoTemplate.insert(new SmokeDoc("b", 2), collection);
        });

        assertThat(mongoTemplate.getCollection(collection).countDocuments()).isEqualTo(2);
    }

    @Test
    void kafka_producerCanPublish() throws Exception {
        kafkaTemplate.send("smoke-topic", "key", "value")
                .get(10, TimeUnit.SECONDS);
    }

    record SmokeDoc(String name, int value) {}
}
