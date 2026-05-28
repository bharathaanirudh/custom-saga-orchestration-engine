package com.anirudh.saga.core;

import io.mongock.runner.springboot.EnableMongock;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.scheduling.annotation.EnableScheduling;

// Mongo/Kafka Spring Boot auto-configurations are excluded via
// `spring.autoconfigure.exclude` in application.yml — the engine wires its
// own MongoConfig and KafkaConfig. We cannot put `@EnableAutoConfiguration(exclude=…)`
// on this class because it IS an @AutoConfiguration; that re-triggers
// auto-config discovery and Spring sees this class in its own import chain,
// raising "circular @Import" at boot.
// @EnableMongock — fires migration runner before the engine accepts traffic
// (P2-057 / E2-34, AC-1). Annotation-based wiring is required because the engine's
// bundled application.yml is NOT loaded by downstream consumers (only the topmost
// application.yml wins), so the `mongock.enabled=true` property alone is not
// reliable for library users.
@AutoConfiguration
@EnableMongock
@EnableScheduling
@ComponentScan(
        // Enumerate sub-packages explicitly rather than scanning the root
        // `com.anirudh.saga.core`. The auto-configuration itself lives at the
        // root; scanning the root re-discovers this class and Spring raises
        // "circular @Import" on real bootRun. (Tests previously missed this
        // because they wired config classes directly.) Excluding via filter
        // does not work — Spring records the import before filters apply.
        basePackages = {
                "com.anirudh.saga.core.api",
                "com.anirudh.saga.core.audit",
                "com.anirudh.saga.core.config",
                "com.anirudh.saga.core.diagnosis",
                "com.anirudh.saga.core.dlt",
                "com.anirudh.saga.core.domain",
                "com.anirudh.saga.core.engine",
                "com.anirudh.saga.core.exception",
                "com.anirudh.saga.core.executor",
                "com.anirudh.saga.core.infrastructure",
                "com.anirudh.saga.core.loader",
                "com.anirudh.saga.core.lock",
                "com.anirudh.saga.core.metrics",
                "com.anirudh.saga.core.migration",
                "com.anirudh.saga.core.outbox",
                "com.anirudh.saga.core.replay",
                "com.anirudh.saga.core.repository",
                "com.anirudh.saga.core.scheduler"
        },
        excludeFilters = @ComponentScan.Filter(
                type = FilterType.REGEX,
                // Test-only integration package — present on test classpath under
                // com.anirudh.saga.core.integration, never in main. Filter kept for
                // when tests share the same auto-config import chain.
                pattern = "com\\.anirudh\\.saga\\.core\\.integration\\..*"))
public class SagaOrchestratorAutoConfiguration {
    // All beans now in dedicated config classes:
    // MongoConfig — MongoDB client, transaction manager, repository scanning
    // KafkaConfig — producer, consumer, listener container factories
    // ClockConfig — injectable Clock bean
}
