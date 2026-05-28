package com.anirudh.saga.core.migration;

import io.mongock.api.annotations.ChangeUnit;
import io.mongock.api.annotations.Execution;
import io.mongock.api.annotations.RollbackExecution;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.Index;

import java.time.Duration;

/**
 * V001: initial set of indexes for the engine's collections (P2-057 / E2-34).
 *
 * <p>Originally created in {@code MongoIndexConfig.afterPropertiesSet()}. Moved here so
 * index changes are versioned alongside schema additions. {@code ensureIndex} is
 * idempotent — re-runs on populated clusters are no-ops.
 */
@ChangeUnit(id = "V001__initial_indexes", order = "001", author = "engine")
public class V001__InitialIndexes {

    @Execution
    public void apply(MongoTemplate mongoTemplate) {
        // saga_instances
        mongoTemplate.indexOps("saga_instances")
                .ensureIndex(new Index("idempotencyKey", Sort.Direction.ASC).unique().named("idx_idempotency_key"));
        mongoTemplate.indexOps("saga_instances")
                .ensureIndex(new Index("status", Sort.Direction.ASC).named("idx_status"));
        mongoTemplate.indexOps("saga_instances")
                .ensureIndex(new CompoundIndexDefinition(
                        new Document("status", 1).append("timeoutAt", 1)
                ).named("idx_status_timeout"));
        mongoTemplate.indexOps("saga_instances")
                .ensureIndex(new CompoundIndexDefinition(
                        new Document("status", 1).append("currentStepStartedAt", 1)
                ).named("idx_status_step_started"));

        // saga_execution_log
        mongoTemplate.indexOps("saga_execution_log")
                .ensureIndex(new Index("sagaId", Sort.Direction.ASC).named("idx_execution_log_sagaid"));

        // saga_locks
        mongoTemplate.indexOps("saga_locks")
                .ensureIndex(new Index("expiresAt", Sort.Direction.ASC).named("idx_lock_expires"));
        mongoTemplate.indexOps("saga_locks")
                .ensureIndex(new Index("sagaId", Sort.Direction.ASC).named("idx_lock_sagaid"));

        // saga_processed_commands — TTL
        mongoTemplate.indexOps("saga_processed_commands")
                .ensureIndex(new Index("processedAt", Sort.Direction.ASC)
                        .expire(Duration.ofDays(7)).named("idx_processed_ttl"));
    }

    @RollbackExecution
    public void rollback(MongoTemplate mongoTemplate) {
        // Index rollback is intentionally a no-op. Dropping indexes is destructive and
        // only relevant for operator-driven downgrades; not a Mongock concern.
    }
}
