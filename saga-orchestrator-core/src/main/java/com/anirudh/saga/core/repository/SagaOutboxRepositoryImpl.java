package com.anirudh.saga.core.repository;

import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;

/**
 * Custom repository fragment — Spring Data discovers this by
 * the {@code Impl} suffix convention on
 * {@link SagaOutboxRepositoryCustom}.
 */
public class SagaOutboxRepositoryImpl implements SagaOutboxRepositoryCustom {

    private final MongoTemplate mongoTemplate;

    public SagaOutboxRepositoryImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public int findMaxRetryCountAmongPending() {
        Aggregation agg = Aggregation.newAggregation(
                Aggregation.match(org.springframework.data.mongodb.core.query.Criteria
                        .where("status").is("PENDING")),
                Aggregation.group().max("retryCount").as("max")
        );
        AggregationResults<Document> result =
                mongoTemplate.aggregate(agg, "saga_outbox", Document.class);
        Document doc = result.getUniqueMappedResult();
        if (doc == null) return 0;
        Number max = (Number) doc.get("max");
        return max == null ? 0 : max.intValue();
    }
}
