package com.anirudh.saga.core.domain;

public record StepDefinition(
        String name,
        StepType type,
        String action,
        String module,
        String topic,
        String url,
        String compensationAction,
        String compensationTopic,
        String compensationUrl,
        int retryMaxAttempts,
        int timeoutSeconds,
        // P2-067: optional declarative fallback value. When technical retries are
        // exhausted and `fallback` is non-null, the engine writes this value into
        // the saga's context (under the step's name) and continues — instead of
        // suspending. NEVER applies to BUSINESS failures (BTFC locked position).
        // Type is Object because Jackson deserializes it as primitive/map/list
        // depending on YAML shape; the engine stores it into context as-is.
        Object fallback
) {

    public boolean hasFallback() { return fallback != null; }
    /**
     * Resolved topic — if module is set, derives {module}-commands.
     * Explicit topic overrides convention.
     */
    public String resolvedTopic() {
        if (topic != null && !topic.isBlank()) return topic;
        if (module != null && !module.isBlank()) return module + "-commands";
        return null;
    }

    /**
     * Resolved compensation topic — same convention from module, or explicit override.
     */
    public String resolvedCompensationTopic() {
        if (compensationTopic != null && !compensationTopic.isBlank()) return compensationTopic;
        if (module != null && !module.isBlank()) return module + "-commands";
        return null;
    }

    public boolean hasCompensation() {
        return compensationAction != null && !compensationAction.isBlank();
    }
}
