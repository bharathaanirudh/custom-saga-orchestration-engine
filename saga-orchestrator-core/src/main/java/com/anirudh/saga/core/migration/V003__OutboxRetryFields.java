package com.anirudh.saga.core.migration;

import io.mongock.api.annotations.ChangeUnit;
import io.mongock.api.annotations.Execution;
import io.mongock.api.annotations.RollbackExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * V003: records the introduction of {@code OutboxEntry.retryCount},
 * {@code lastError}, {@code lastErrorAt} fields (P2-007 / E2-22).
 *
 * <p>MongoDB is schemaless — existing PENDING entries written before this
 * change simply lack the new fields. The engine treats absent values as
 * {@code retryCount = 0} (primitive default) and {@code lastError = null},
 * which is the correct "never failed yet" semantics.
 *
 * <p>This change unit is a marker, not a data migration: no backfill is
 * required. Recorded in {@code mongockChangeLog} for audit, so operators
 * can confirm at a glance that the schema is at v0.3+.
 *
 * <p>Note: the V003 slot was originally pencilled in for outbox competitive
 * claiming (E2-12, v1.5). That work was always going to need a real data
 * migration once it lands, so it is reclaimed as V004+ when E2-12 ships.
 */
@ChangeUnit(id = "V003__outbox_retry_fields", order = "003", author = "engine")
public class V003__OutboxRetryFields {

    private static final Logger log = LoggerFactory.getLogger(V003__OutboxRetryFields.class);

    @Execution
    public void apply(MongoTemplate mongoTemplate) {
        long count = mongoTemplate.getCollection("saga_outbox").countDocuments();
        log.info("V003 applied: outbox retry fields are now valid on saga_outbox "
                + "({} existing docs left untouched)", count);
    }

    @RollbackExecution
    public void rollback(MongoTemplate mongoTemplate) {
        // No-op — fields are null-tolerant by design; nothing to undo.
    }
}
