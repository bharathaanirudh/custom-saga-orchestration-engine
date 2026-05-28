package com.anirudh.saga.core.migration;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ValidationOptions;
import io.mongock.api.annotations.ChangeUnit;
import io.mongock.api.annotations.Execution;
import io.mongock.api.annotations.RollbackExecution;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.concurrent.TimeUnit;

/**
 * V004: harden `saga_execution_log` into an immutable, replay-grade event store
 * (P2-016 / E2-11).
 *
 * <p>Two changes:
 * <ol>
 *   <li><b>Schema validator</b> — every document must carry the immutable core fields
 *       (sagaId, event, timestamp). `validationAction: error` rejects malformed inserts.
 *       Combined with the fact that nothing in the engine ever updates this collection
 *       (only {@code CheckpointStore.logEvent} inserts), this gives append-only behavior
 *       at the application + schema level. <i>True tamper-proofing</i> (blocking updates
 *       even from a rogue client) needs DB-level role restrictions — out of engine scope.</li>
 *   <li><b>TTL index</b> — events expire after 90 days to bound storage growth. For a
 *       permanent audit trail, drop {@code idx_event_ttl}.</li>
 * </ol>
 */
@ChangeUnit(id = "V004__saga_event_store", order = "004", author = "engine")
public class V004__SagaEventStore {

    private static final Logger log = LoggerFactory.getLogger(V004__SagaEventStore.class);
    private static final String COLLECTION = "saga_execution_log";
    private static final long TTL_DAYS = 90;

    @Execution
    public void apply(MongoTemplate mongoTemplate) {
        MongoDatabase db = mongoTemplate.getDb();

        // 1) Schema validator — enforce the immutable core fields on every write.
        Document schema = new Document("$jsonSchema", new Document()
                .append("bsonType", "object")
                .append("required", java.util.List.of("sagaId", "event", "timestamp"))
                .append("properties", new Document()
                        .append("sagaId", new Document("bsonType", "string"))
                        .append("event", new Document("bsonType", "string"))
                        .append("timestamp", new Document("bsonType", "date"))));

        if (collectionExists(db, COLLECTION)) {
            db.runCommand(new Document("collMod", COLLECTION)
                    .append("validator", schema)
                    .append("validationAction", "error"));
            log.info("V004: applied schema validator to {} (append-only enforcement)", COLLECTION);
        } else {
            db.createCollection(COLLECTION, new com.mongodb.client.model.CreateCollectionOptions()
                    .validationOptions(new ValidationOptions().validator(schema)));
            log.info("V004: created {} with schema validator", COLLECTION);
        }

        // 2) TTL index — bound storage; 90-day retention.
        db.getCollection(COLLECTION).createIndex(
                new Document("timestamp", 1),
                new IndexOptions().name("idx_event_ttl").expireAfter(TTL_DAYS, TimeUnit.DAYS));
        log.info("V004: TTL index idx_event_ttl ({}d) on {}.timestamp", TTL_DAYS, COLLECTION);
    }

    @RollbackExecution
    public void rollback(MongoTemplate mongoTemplate) {
        MongoDatabase db = mongoTemplate.getDb();
        if (collectionExists(db, COLLECTION)) {
            db.runCommand(new Document("collMod", COLLECTION)
                    .append("validator", new Document())
                    .append("validationAction", "warn"));
        }
    }

    private boolean collectionExists(MongoDatabase db, String name) {
        for (String existing : db.listCollectionNames()) {
            if (existing.equals(name)) return true;
        }
        return false;
    }
}
