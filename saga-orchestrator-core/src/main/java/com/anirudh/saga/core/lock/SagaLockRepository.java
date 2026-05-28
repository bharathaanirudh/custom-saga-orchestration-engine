package com.anirudh.saga.core.lock;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import java.time.Instant;
import java.util.List;

public interface SagaLockRepository extends MongoRepository<SagaLock, String> {

    @Query("{ 'expiresAt': { $lt: ?0 } }")
    List<SagaLock> findExpiredLocks(Instant now);

    void deleteBySagaId(String sagaId);
}
