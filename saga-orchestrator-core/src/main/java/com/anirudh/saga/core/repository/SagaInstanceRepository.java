package com.anirudh.saga.core.repository;

import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface SagaInstanceRepository extends MongoRepository<SagaInstance, String> {

    Optional<SagaInstance> findBySagaId(String sagaId);

    Optional<SagaInstance> findByIdempotencyKey(String idempotencyKey);

    List<SagaInstance> findByStatus(SagaStatus status);

    /** All sagas of a type — bulk replay candidates (P2-027). */
    List<SagaInstance> findBySagaType(String sagaType);

    @Query("{ 'status': { $in: ['STARTED', 'IN_PROGRESS'] }, 'timeoutAt': { $lt: ?0 } }")
    List<SagaInstance> findTimedOut(Instant now);

    /**
     * Sagas in IN_PROGRESS with a step that started before {@code earliestStartedAt}.
     * Step-level timeout filtering happens in Java (per-step timeoutSeconds varies by definition).
     * Pass an aggressive cutoff (e.g., now - 1s) to return all viable candidates.
     */
    @Query("{ 'status': 'IN_PROGRESS', 'currentStepStartedAt': { $ne: null, $lt: ?0 } }")
    List<SagaInstance> findStepCandidatesForTimeoutCheck(Instant earliestStartedAt);
}
