package com.anirudh.saga.core.outbox;

/**
 * Outbox publisher strategy (P2-035 / E2-22).
 *
 * <p>Resolves the prior ambiguity where Debezium and {@link OutboxPoller} could both
 * publish the same outbox entry, with no documented contract about which was primary.
 *
 * <p>Trade-offs are documented in {@code docs/adr/ADR-001-outbox-publisher-mode.md}.
 */
public enum PublisherMode {

    /**
     * Debezium is the primary publisher. The poller acts as a fallback only — it
     * publishes entries that have been PENDING longer than {@code fallback-threshold-minutes},
     * which signals Debezium is down or lagging.
     *
     * <p>Default. Lowest publish latency. Requires Debezium to be deployed and watching
     * the {@code saga_outbox} collection's Change Stream.
     */
    DEBEZIUM_PRIMARY,

    /**
     * The poller is the only publisher. Debezium is not required and may not be deployed.
     * The poller publishes every PENDING entry on each tick (governed by
     * {@code saga.outbox.poll-interval-ms}, default 30s).
     *
     * <p>Higher publish latency (bounded by poll interval) but simpler ops profile.
     * Recommended for teams without a Debezium pipeline.
     */
    POLLER_ONLY
}
