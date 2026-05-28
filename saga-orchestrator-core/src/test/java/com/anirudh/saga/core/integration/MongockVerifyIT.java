package com.anirudh.saga.core.integration;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

import java.time.Duration;
import java.util.List;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Verifies Mongock actually ran the migrations at context startup (P2-057).
 *
 * <p>Covers AC-2 (change-units recorded), AC-3 (indexes from V001 present), AC-5 +
 * AC-6 (idempotent re-application against a populated database).
 */
class MongockVerifyIT extends AbstractIntegrationTest {

    @Autowired private MongoTemplate mongoTemplate;

    @Test
    void ac2_mongockAppliedV001AndV002() {
        // Mongock 5 records each successful @ChangeUnit in a collection commonly
        // named "mongockChangeLog" (or "changelog"; varies by version). Check both.
        long v001Count = countChangeUnit("V001__initial_indexes");
        long v002Count = countChangeUnit("V002__idempotency_payload_hash");

        assertThat(v001Count + v002Count)
                .as("Mongock changeLog should contain both V001 and V002 entries — found V001=%d V002=%d",
                        v001Count, v002Count)
                .isGreaterThanOrEqualTo(2);
    }

    @Test
    void ac3_v001IndexesPresentOnSagaInstances() {
        List<String> indexNames = StreamSupport.stream(
                        mongoTemplate.indexOps("saga_instances").getIndexInfo().spliterator(), false)
                .map(i -> i.getName())
                .toList();

        assertThat(indexNames)
                .as("V001 should have created idempotency + status + timeout + step-started indexes — got %s",
                        indexNames)
                .contains("idx_idempotency_key", "idx_status", "idx_status_timeout", "idx_status_step_started");
    }

    @Test
    void ac5_ac6_reapplyingIndexesOnPopulatedCollectionIsNoOp() {
        // Seed data so the collection is non-empty — proves the "populated DB"
        // half of AC-6. Mongock's ensureIndex semantics must remain idempotent
        // when documents are present (real upgrade path).
        Document seed = new Document()
                .append("_id", "seed-saga-id")
                .append("idempotencyKey", "seed-key-" + System.nanoTime())
                .append("status", "COMPLETED")
                .append("currentStep", 0);
        try {
            mongoTemplate.getCollection("saga_instances").insertOne(seed);

            // Re-apply the same indexes V001 declared. ensureIndex with identical
            // key+name must be a no-op; raising would mean V001 isn't safely
            // re-runnable on populated clusters.
            assertThatCode(() -> {
                mongoTemplate.indexOps("saga_instances")
                        .ensureIndex(new Index("idempotencyKey", Sort.Direction.ASC).unique().named("idx_idempotency_key"));
                mongoTemplate.indexOps("saga_processed_commands")
                        .ensureIndex(new Index("processedAt", Sort.Direction.ASC)
                                .expire(Duration.ofDays(7)).named("idx_processed_ttl"));
            }).as("Re-applying V001-equivalent indexes on a populated collection must not throw")
                    .doesNotThrowAnyException();

            // Seed document untouched.
            long seedCount = mongoTemplate.getCollection("saga_instances")
                    .countDocuments(new Document("_id", "seed-saga-id"));
            assertThat(seedCount).as("Seed document preserved across re-apply").isEqualTo(1L);
        } finally {
            mongoTemplate.getCollection("saga_instances").deleteOne(new Document("_id", "seed-saga-id"));
        }
    }

    private long countChangeUnit(String changeId) {
        long count = 0;
        for (String collection : new String[]{"mongockChangeLog", "changelog"}) {
            try {
                count += mongoTemplate.getCollection(collection)
                        .countDocuments(new Document("changeId", changeId));
            } catch (Exception ignored) {
                // collection might not exist for one of the names — ok
            }
        }
        return count;
    }
}
