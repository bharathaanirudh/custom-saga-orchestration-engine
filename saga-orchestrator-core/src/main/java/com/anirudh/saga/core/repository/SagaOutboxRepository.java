package com.anirudh.saga.core.repository;

import com.anirudh.saga.core.outbox.OutboxEntry;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import java.time.Instant;
import java.util.List;

public interface SagaOutboxRepository
        extends MongoRepository<OutboxEntry, String>, SagaOutboxRepositoryCustom {

    @Query("{ 'status': 'PENDING', 'createdAt': { $lt: ?0 } }")
    List<OutboxEntry> findPendingOlderThan(Instant threshold);
}
