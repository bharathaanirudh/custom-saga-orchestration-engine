package com.anirudh.saga.core.outbox;

import com.anirudh.saga.sdk.contract.SagaHeaders;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Startup check for {@code saga-replies} topic retention (P2-035 AC-7/8).
 *
 * <p>If retention.ms is shorter than the longest saga's max age, late replies will
 * be dropped before the orchestrator can read them — sagas hang in IN_PROGRESS forever.
 * This check fails fast at boot so the misconfiguration is caught before traffic.
 *
 * <p>Behavior is governed by {@code saga.outbox.publisher.retention-check}:
 * <ul>
 *   <li>{@code fail} (default) — throw on insufficient retention</li>
 *   <li>{@code warn} — log a warning and proceed</li>
 *   <li>{@code skip} — no check at all</li>
 * </ul>
 */
@Component
@ConditionalOnBean(KafkaAdmin.class)
public class RetentionCheck implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(RetentionCheck.class);
    private static final String RETENTION_MS = "retention.ms";

    public enum Mode { FAIL, WARN, SKIP }

    private final KafkaAdmin kafkaAdmin;
    private final Mode mode;
    private final long maxSagaTimeoutMinutes;

    public RetentionCheck(
            KafkaAdmin kafkaAdmin,
            @Value("${saga.outbox.publisher.retention-check:fail}") String modeStr,
            @Value("${saga.outbox.publisher.max-saga-timeout-minutes:60}") long maxSagaTimeoutMinutes) {
        this.kafkaAdmin = kafkaAdmin;
        this.mode = Mode.valueOf(modeStr.toUpperCase());
        this.maxSagaTimeoutMinutes = maxSagaTimeoutMinutes;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (mode == Mode.SKIP) {
            log.info("Kafka retention check skipped (saga.outbox.publisher.retention-check=skip)");
            return;
        }

        long requiredRetentionMs = Duration.ofMinutes(maxSagaTimeoutMinutes).toMillis();

        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, SagaHeaders.REPLY_TOPIC);
            Map<ConfigResource, Config> configs = client.describeConfigs(Collections.singleton(resource))
                    .all().get(10, TimeUnit.SECONDS);

            Config config = configs.get(resource);
            if (config == null) {
                handleProblem("Cannot describe topic " + SagaHeaders.REPLY_TOPIC + " — does it exist?");
                return;
            }

            ConfigEntry retention = config.get(RETENTION_MS);
            if (retention == null) {
                handleProblem("Topic " + SagaHeaders.REPLY_TOPIC + " has no retention.ms config");
                return;
            }

            long actualRetentionMs = Long.parseLong(retention.value());
            // -1 means infinite retention — always sufficient.
            if (actualRetentionMs > 0 && actualRetentionMs < requiredRetentionMs) {
                handleProblem(String.format(
                        "RETENTION_TOO_SHORT: %s retention.ms=%d but engine needs >= %d (max saga timeout %d min)",
                        SagaHeaders.REPLY_TOPIC, actualRetentionMs, requiredRetentionMs, maxSagaTimeoutMinutes));
                return;
            }

            log.info("Kafka retention check OK: {} retention.ms={} (>= required {})",
                    SagaHeaders.REPLY_TOPIC, actualRetentionMs, requiredRetentionMs);
        } catch (Exception e) {
            // Fail-fast on connection errors only when mode=FAIL.
            handleProblem("Kafka retention check failed: " + e.getMessage());
        }
    }

    private void handleProblem(String message) {
        if (mode == Mode.FAIL) {
            throw new IllegalStateException(message);
        }
        log.warn("Retention check (mode=warn): {}", message);
    }
}
