package com.anirudh.saga.core.unit.executor;

import com.anirudh.saga.core.domain.StepDefinition;
import com.anirudh.saga.core.domain.StepType;
import com.anirudh.saga.core.executor.HttpStepExecutor;
import com.anirudh.saga.core.executor.StepResult;
import com.anirudh.saga.core.fixtures.SagaInstanceFixture;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link HttpStepExecutor} retry classification (P2-006).
 *
 * Uses MockWebServer for realistic HTTP semantics (status codes, abrupt closes)
 * rather than mocking RestClient's fluent chain.
 */
class HttpStepExecutorTest {

    private static final String SUCCESS_BODY = """
            {"sagaId":"s","stepId":"step","status":"SUCCESS","failureType":null,"data":null,"error":null}
            """;
    private static final String BUSINESS_FAILURE_BODY = """
            {"sagaId":"s","stepId":"step","status":"BUSINESS_FAILURE","failureType":"BUSINESS","data":null,"error":"insufficient funds"}
            """;

    private MockWebServer server;
    private HttpStepExecutor executor;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        executor = new HttpStepExecutor(RestClient.builder());
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    @Test
    void supports_returnsHttp() {
        assertThat(executor.supports()).isEqualTo(StepType.HTTP);
    }

    @Test
    void success_onFirstAttempt_noRetry() {
        server.enqueue(jsonResponse(200, SUCCESS_BODY));

        StepResult result = executor.execute(SagaInstanceFixture.inProgress(), httpStep(3));

        assertThat(result.isSuccess()).isTrue();
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void transient5xx_retriesUpToMaxAttempts_thenTechnicalFailure() {
        server.enqueue(jsonResponse(503, ""));
        server.enqueue(jsonResponse(503, ""));
        server.enqueue(jsonResponse(503, ""));

        StepResult result = executor.execute(SagaInstanceFixture.inProgress(), httpStep(3));

        assertThat(result.isTechnicalFailure()).isTrue();
        assertThat(result.error()).contains("after 3 attempts");
        assertThat(server.getRequestCount()).isEqualTo(3);
    }

    @Test
    void transient5xx_thenSuccess_returnsSuccess() {
        server.enqueue(jsonResponse(503, ""));
        server.enqueue(jsonResponse(503, ""));
        server.enqueue(jsonResponse(200, SUCCESS_BODY));

        StepResult result = executor.execute(SagaInstanceFixture.inProgress(), httpStep(3));

        assertThat(result.isSuccess()).isTrue();
        assertThat(server.getRequestCount()).isEqualTo(3);
    }

    @Test
    void permanent4xx_immediateBusinessFailure() {
        server.enqueue(jsonResponse(400, ""));

        StepResult result = executor.execute(SagaInstanceFixture.inProgress(), httpStep(3));

        assertThat(result.isBusinessFailure()).isTrue();
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void retriable4xx_429TooManyRequests_isRetried() {
        server.enqueue(jsonResponse(429, ""));
        server.enqueue(jsonResponse(200, SUCCESS_BODY));

        StepResult result = executor.execute(SagaInstanceFixture.inProgress(), httpStep(3));

        assertThat(result.isSuccess()).isTrue();
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    void replyBodyBusinessFailure_noRetry() {
        // Server returns 200 OK but body says businessFailure=true.
        // Participant has spoken — don't retry.
        server.enqueue(jsonResponse(200, BUSINESS_FAILURE_BODY));

        StepResult result = executor.execute(SagaInstanceFixture.inProgress(), httpStep(3));

        assertThat(result.isBusinessFailure()).isTrue();
        assertThat(result.error()).contains("insufficient funds");
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void ioException_retriedAsTechnical() throws Exception {
        // First request: server abruptly closes the socket → ResourceAccessException.
        // Second request: success.
        server.enqueue(new MockResponse()
                .setSocketPolicy(okhttp3.mockwebserver.SocketPolicy.DISCONNECT_AT_START));
        server.enqueue(jsonResponse(200, SUCCESS_BODY));

        StepResult result = executor.execute(SagaInstanceFixture.inProgress(), httpStep(3));

        assertThat(result.isSuccess()).isTrue();
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    void retryMaxAttemptsZero_treatedAsOne_noRetry() {
        // YAML default for unset int field is 0; engine treats this as "1 attempt, no retry".
        server.enqueue(jsonResponse(503, ""));

        StepResult result = executor.execute(SagaInstanceFixture.inProgress(), httpStep(0));

        assertThat(result.isTechnicalFailure()).isTrue();
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void retryMaxAttemptsTwo_attempts3SequenceIsExactly2() {
        // P2-006 AC-5: retryMaxAttempts=2 → exactly 2 total attempts.
        server.enqueue(jsonResponse(503, ""));
        server.enqueue(jsonResponse(503, ""));

        StepResult result = executor.execute(SagaInstanceFixture.inProgress(), httpStep(2));

        assertThat(result.isTechnicalFailure()).isTrue();
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private MockResponse jsonResponse(int code, String body) {
        return new MockResponse()
                .setResponseCode(code)
                .setHeader("Content-Type", "application/json")
                .setBody(body);
    }

    private StepDefinition httpStep(int retryMaxAttempts) {
        return new StepDefinition(
                "step",
                StepType.HTTP,
                "DO_THING",
                null,
                null,
                server.url("/").toString(),
                "UNDO_THING",
                null,
                server.url("/compensate").toString(),
                retryMaxAttempts,
                10,
                null);
    }
}
