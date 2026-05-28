package com.anirudh.saga.sdk.idempotency;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Document("saga_processed_commands")
public class ProcessedCommand {

    @Id
    private String commandKey;  // sagaId:stepId:action
    private String replyJson;
    private Instant processedAt;

    public ProcessedCommand() {}

    public ProcessedCommand(String commandKey, String replyJson, Instant processedAt) {
        this.commandKey = commandKey;
        this.replyJson = replyJson;
        this.processedAt = processedAt;
    }

    public static String buildKey(String sagaId, String stepId, String action) {
        return sagaId + ":" + stepId + ":" + action;
    }

    public String getCommandKey() { return commandKey; }
    public String getReplyJson() { return replyJson; }
    public Instant getProcessedAt() { return processedAt; }
}
