package com.anirudh.saga.core.executor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-sagaId rate limiter for reply consumption (P2-061 AC-5).
 *
 * <p>Misconfigured participants can replay the same reply hundreds of times/sec for one
 * sagaId; without this, they can monopolize the consumer thread and starve other sagas.
 *
 * <p>Implementation: per-sagaId sliding window of length 1 second tracked as
 * (windowStart, count). On each call, if windowStart is older than 1s, reset; else
 * increment. Reject if the count exceeds the cap.
 *
 * <p>Memory: bounded by the number of distinct sagaIds active in any 1-sec window.
 * Stale entries are pruned opportunistically — every {@code pruneEveryN} {@code allow}
 * calls, entries with windowStart older than {@code pruneAgeMs} are removed.
 */
@Component
public class ReplyRateLimiter {

    private final Clock clock;
    private final long maxRatePerSecond;
    private final long pruneAgeMs = 60_000;            // 1 minute — generous
    private final int pruneEveryN = 1000;
    private final Map<String, Window> windows = new ConcurrentHashMap<>();
    private final AtomicLong callCounter = new AtomicLong();

    public ReplyRateLimiter(Clock clock,
                            @Value("${saga.reply.max-rate-per-second:10}") long maxRatePerSecond) {
        this.clock = clock;
        this.maxRatePerSecond = maxRatePerSecond;
    }

    /** Returns true if this reply should be processed; false if it's over the cap. */
    public boolean allow(String sagaId) {
        long now = clock.millis();
        Window window = windows.computeIfAbsent(sagaId, k -> new Window(now));
        boolean allowed;
        synchronized (window) {
            if (now - window.start >= 1000) {
                window.start = now;
                window.count = 1;
                allowed = true;
            } else if (window.count < maxRatePerSecond) {
                window.count++;
                allowed = true;
            } else {
                allowed = false;
            }
        }
        if (callCounter.incrementAndGet() % pruneEveryN == 0) {
            prune(now);
        }
        return allowed;
    }

    private void prune(long now) {
        windows.entrySet().removeIf(e -> {
            synchronized (e.getValue()) {
                return now - e.getValue().start > pruneAgeMs;
            }
        });
    }

    private static class Window {
        long start;
        long count = 0;

        Window(long start) { this.start = start; }
    }
}
