package com.anirudh.saga.core.repository;

import com.anirudh.saga.core.audit.SagaExecutionLog;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface SagaExecutionLogRepository extends MongoRepository<SagaExecutionLog, String> {

    List<SagaExecutionLog> findBySagaIdOrderByTimestampAsc(String sagaId);
}
