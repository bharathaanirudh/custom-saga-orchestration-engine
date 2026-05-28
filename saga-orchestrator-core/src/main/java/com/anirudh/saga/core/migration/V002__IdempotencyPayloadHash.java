package com.anirudh.saga.core.migration;

import io.mongock.api.annotations.ChangeUnit;
import io.mongock.api.annotations.Execution;
import io.mongock.api.annotations.RollbackExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * V002: records the introduction of {@code SagaInstance.idempotencyPayloadHash}
 * (added by P2-061 AC-1).
 *
 * <p>MongoDB is schemaless, so existing documents simply have the field absent
 * (which the engine treats as "skip the hash check" for graceful upgrade).
 * This change unit is recorded for two reasons:
 * <ol>
 *   <li>Audit trail — operators can confirm at a glance that the schema is at v0.2+
 *       by inspecting {@code mongockChangeLog}.</li>
 *   <li>If a future downgrade ever needs to remove the field, the rollback is here.</li>
 * </ol>
 */
@ChangeUnit(id = "V002__idempotency_payload_hash", order = "002", author = "engine")
public class V002__IdempotencyPayloadHash {

    private static final Logger log = LoggerFactory.getLogger(V002__IdempotencyPayloadHash.class);

    @Execution
    public void apply(MongoTemplate mongoTemplate) {
        long count = mongoTemplate.getCollection("saga_instances").countDocuments();
        log.info("V002 applied: idempotencyPayloadHash field is now valid on saga_instances ({} existing docs left untouched)", count);
    }

    @RollbackExecution
    public void rollback(MongoTemplate mongoTemplate) {
        // No-op — field is null-tolerant by design; nothing to undo.
    }
}
