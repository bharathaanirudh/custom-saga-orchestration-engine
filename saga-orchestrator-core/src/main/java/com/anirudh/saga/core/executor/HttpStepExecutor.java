package com.anirudh.saga.core.executor;

import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.StepDefinition;
import com.anirudh.saga.core.domain.StepType;
import com.anirudh.saga.core.exception.SagaExecutionException;
import com.anirudh.saga.sdk.contract.SagaHttpCommand;
import com.anirudh.saga.sdk.contract.SagaHttpReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

@Component
public class HttpStepExecutor implements StepExecutor {

    private static final Logger log = LoggerFactory.getLogger(HttpStepExecutor.class);
    private static final long INITIAL_BACKOFF_MS = 500L;

    private final RestClient restClient;

    public HttpStepExecutor(RestClient.Builder restClientBuilder) {
        this.restClient = restClientBuilder.build();
    }

    @Override
    public StepType supports() {
        return StepType.HTTP;
    }

    @Override
    public StepResult execute(SagaInstance instance, StepDefinition step) {
        log.info("[sagaId={}] Dispatching HTTP step={} url={}",
                instance.getSagaId(), step.name(), step.url());
        SagaHttpCommand command = new SagaHttpCommand(
                instance.getSagaId(), step.name(), step.action(), instance.getPayload());

        // retryMaxAttempts is total attempts including the initial (matches P2-005 convention).
        // 0 (YAML default for unset int) means "1 attempt, no retry".
        int maxAttempts = step.retryMaxAttempts() < 1 ? 1 : step.retryMaxAttempts();
        String lastError = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                SagaHttpReply reply = postRaw(step.url(), command);
                if (reply == null) {
                    lastError = "null reply";
                } else if (reply.isSuccess()) {
                    return StepResult.success(reply.data());
                } else if (reply.isBusinessFailure()) {
                    // Business failure in the reply body is non-retriable — the participant
                    // told us "this won't succeed even if you ask again."
                    return StepResult.businessFailure(reply.error());
                } else {
                    // technical failure in body — retriable
                    lastError = reply.error();
                }
            } catch (HttpClientErrorException e) {
                if (isRetriable4xx(e.getStatusCode())) {
                    lastError = "HTTP " + e.getStatusCode() + ": " + e.getMessage();
                } else {
                    // Non-retriable 4xx (400, 401, 403, 404, ...) — business failure.
                    return StepResult.businessFailure(
                            "HTTP " + e.getStatusCode().value() + ": " + e.getMessage());
                }
            } catch (HttpServerErrorException | ResourceAccessException e) {
                lastError = e.getClass().getSimpleName() + ": " + e.getMessage();
            } catch (Exception e) {
                // Programming errors, deserialization issues — don't burn retries on them.
                log.error("[sagaId={}] HTTP step {} non-retriable failure", instance.getSagaId(), step.name(), e);
                return StepResult.technicalFailure(
                        "HTTP call to " + step.url() + " failed (non-retriable): " + e.getMessage());
            }

            if (attempt < maxAttempts) {
                long backoffMs = INITIAL_BACKOFF_MS << (attempt - 1); // 500, 1000, 2000, ...
                log.warn("[sagaId={}] HTTP step {} attempt {}/{} failed ({}); retrying in {}ms",
                        instance.getSagaId(), step.name(), attempt, maxAttempts, lastError, backoffMs);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return StepResult.technicalFailure("HTTP step " + step.name() + " interrupted during backoff");
                }
            }
        }

        log.error("[sagaId={}] HTTP step {} exhausted {} attempts; last error: {}",
                instance.getSagaId(), step.name(), maxAttempts, lastError);
        return StepResult.technicalFailure(
                "HTTP step " + step.name() + " failed after " + maxAttempts + " attempts: " + lastError);
    }

    /** 408 Request Timeout and 429 Too Many Requests are retriable; all other 4xx are not. */
    private static boolean isRetriable4xx(HttpStatusCode status) {
        int code = status.value();
        return code == 408 || code == 429;
    }

    @Override
    public void compensate(SagaInstance instance, StepDefinition step) {
        log.info("[sagaId={}] Dispatching HTTP compensation step={} url={}",
                instance.getSagaId(), step.name(), step.compensationUrl());
        SagaHttpCommand command = new SagaHttpCommand(
                instance.getSagaId(), step.name(), step.compensationAction(), instance.getContext());
        SagaHttpReply reply = post(step.compensationUrl(), command);
        if (reply == null || !reply.isSuccess()) {
            String error = reply != null ? reply.error() : "null reply";
            throw new SagaExecutionException(
                    "HTTP compensation failed for step " + step.name() + ": " + error, null);
        }
    }

    /** Wraps any failure into SagaExecutionException — used by compensate(), which has no retry. */
    private SagaHttpReply post(String url, SagaHttpCommand command) {
        try {
            return postRaw(url, command);
        } catch (Exception e) {
            throw new SagaExecutionException("HTTP call failed to " + url, e);
        }
    }

    /** Lets caller distinguish exception types (for retry classification). */
    private SagaHttpReply postRaw(String url, SagaHttpCommand command) {
        return restClient.post()
                .uri(url)
                .body(command)
                .retrieve()
                .body(SagaHttpReply.class);
    }
}
