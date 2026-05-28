package com.anirudh.saga.core.migration;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reports Mongock migration outcomes once the application has started (P2-057 AC-4).
 *
 * <p>Mongock writes one entry per change-unit to {@code mongockChangeLog} with a state
 * field. This reporter scans that collection on {@code ApplicationReadyEvent} and:
 * <ol>
 *   <li>Registers per-state gauges so operators can alert on
 *       {@code saga_migration_state_total{state="FAILED"} > 0}.</li>
 *   <li>Emits a structured log line (INFO on all-EXECUTED, ERROR on any non-EXECUTED)
 *       so log scrapers can route alerts.</li>
 * </ol>
 *
 * <p><strong>Note on the "engine refuses to start" half of AC-4:</strong> when Mongock
 * fails mid-execution, the Spring context never refreshes — this listener does not
 * fire. The alert in that case is {@code up == 0} from Prometheus + the boot log.
 * This reporter exists for the *next* boot: once the FAILED entry is in the
 * changelog, a subsequent restart surfaces it as a metric and a structured log,
 * even if the engine somehow proceeds (e.g. operator marks the change-unit IGNORED).
 */
@Component
public class MigrationStartupReporter {

    private static final Logger log = LoggerFactory.getLogger(MigrationStartupReporter.class);

    private static final String[] CHANGELOG_COLLECTIONS = {"mongockChangeLog", "changelog"};
    private static final String[] TRACKED_STATES = {"EXECUTED", "FAILED", "ROLLED_BACK", "IGNORED"};

    private final MongoTemplate mongoTemplate;
    private final MeterRegistry meterRegistry;
    private final Map<String, AtomicLong> stateGauges = new HashMap<>();

    public MigrationStartupReporter(MongoTemplate mongoTemplate, MeterRegistry meterRegistry) {
        this.mongoTemplate = mongoTemplate;
        this.meterRegistry = meterRegistry;
        for (String state : TRACKED_STATES) {
            AtomicLong holder = new AtomicLong(0);
            stateGauges.put(state, holder);
            meterRegistry.gauge("saga.migration.state.total", Tags.of("state", state), holder);
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void report() {
        Map<String, Long> counts = new HashMap<>();
        for (String state : TRACKED_STATES) counts.put(state, 0L);

        for (String collection : CHANGELOG_COLLECTIONS) {
            try {
                mongoTemplate.getCollection(collection).find().forEach(doc -> {
                    String state = doc.getString("state");
                    if (state != null && counts.containsKey(state)) {
                        counts.merge(state, 1L, Long::sum);
                    }
                });
            } catch (Exception ignored) {
                // Collection may not exist for one of the two known names — that's fine.
            }
        }

        counts.forEach((state, count) -> stateGauges.get(state).set(count));

        long failed = counts.get("FAILED") + counts.get("ROLLED_BACK");
        long applied = counts.get("EXECUTED");

        if (failed > 0) {
            log.error("migrationStatus=FAILED applied={} failed={} ignored={} — inspect mongockChangeLog (failed/rolled-back entries left for operator review; engine should be considered unhealthy until reconciled)",
                    applied, failed, counts.get("IGNORED"));
        } else {
            log.info("migrationStatus=OK applied={} ignored={} — schema at change-unit head",
                    applied, counts.get("IGNORED"));
        }
    }
}
