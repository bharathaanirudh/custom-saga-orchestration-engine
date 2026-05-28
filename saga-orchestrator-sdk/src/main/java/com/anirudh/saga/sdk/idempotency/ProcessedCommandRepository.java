package com.anirudh.saga.sdk.idempotency;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface ProcessedCommandRepository extends MongoRepository<ProcessedCommand, String> {}
