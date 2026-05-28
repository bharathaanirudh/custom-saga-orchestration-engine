package com.anirudh.saga.sdk.validation;

import com.anirudh.saga.sdk.annotation.SagaCommandHandler;
import com.anirudh.saga.sdk.annotation.SagaParticipant;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

/**
 * At startup, fetches saga definitions from the orchestrator and validates
 * that this participant's module + actions match what the YAML defines.
 *
 * Configure orchestrator URL:
 *   saga.orchestrator.url=http://localhost:8080
 *
 * If orchestrator is unreachable, logs a WARNING but does NOT block startup
 * (allows independent development/testing). Set saga.participant.strict-validation=true
 * to fail fast instead.
 */
@Component
public class ParticipantValidator {

    private static final Logger log = LoggerFactory.getLogger(ParticipantValidator.class);

    private final ApplicationContext applicationContext;
    private final ObjectMapper objectMapper;

    @Value("${saga.orchestrator.url:}")
    private String orchestratorUrl;

    @Value("${saga.participant.module:}")
    private String configModule;

    @Value("${saga.participant.strict-validation:false}")
    private boolean strictValidation;

    public ParticipantValidator(ApplicationContext applicationContext, ObjectMapper objectMapper) {
        this.applicationContext = applicationContext;
        this.objectMapper = objectMapper;
    }

    @EventListener(ContextRefreshedEvent.class)
    public void validateOnStartup() {
        if (orchestratorUrl == null || orchestratorUrl.isBlank()) {
            log.info("saga.orchestrator.url not configured — skipping participant validation");
            return;
        }

        // Collect this participant's declared modules and actions
        Map<String, Set<String>> localHandlers = collectLocalHandlers();
        if (localHandlers.isEmpty()) {
            log.info("No @SagaParticipant beans found — skipping validation");
            return;
        }

        // Fetch definitions from orchestrator
        Map<String, Set<String>> remoteModuleActions;
        try {
            remoteModuleActions = fetchRemoteDefinitions();
        } catch (Exception e) {
            String msg = "Cannot reach orchestrator at " + orchestratorUrl + "/saga-definitions: " + e.getMessage();
            if (strictValidation) {
                throw new IllegalStateException("STRICT VALIDATION FAILED — " + msg, e);
            }
            log.warn("Participant validation skipped — {}", msg);
            return;
        }

        // Validate
        List<String> errors = new ArrayList<>();

        for (Map.Entry<String, Set<String>> entry : localHandlers.entrySet()) {
            String module = entry.getKey();
            Set<String> localActions = entry.getValue();

            if (!remoteModuleActions.containsKey(module)) {
                errors.add("Module [" + module + "] not found in any saga definition. "
                        + "Known modules: " + remoteModuleActions.keySet());
                continue;
            }

            Set<String> remoteActions = remoteModuleActions.get(module);

            // Check: every action defined in YAML should have a local handler
            for (String remoteAction : remoteActions) {
                if (!localActions.contains(remoteAction) && !localActions.contains("*")) {
                    errors.add("Module [" + module + "] missing handler for action [" + remoteAction
                            + "] defined in saga YAML");
                }
            }

            // Check: local handlers that don't match any YAML action (typo detection)
            for (String localAction : localActions) {
                if (!"*".equals(localAction) && !remoteActions.contains(localAction)) {
                    errors.add("Module [" + module + "] has handler for action [" + localAction
                            + "] but no saga YAML defines this action");
                }
            }
        }

        if (errors.isEmpty()) {
            log.info("Participant validation PASSED — all modules and actions match saga definitions");
        } else {
            String report = String.join("\n  - ", errors);
            if (strictValidation) {
                throw new IllegalStateException("PARTICIPANT VALIDATION FAILED:\n  - " + report);
            }
            log.warn("Participant validation WARNINGS:\n  - {}", report);
        }
    }

    private Map<String, Set<String>> collectLocalHandlers() {
        Map<String, Set<String>> result = new LinkedHashMap<>();

        Map<String, Object> participants = applicationContext.getBeansWithAnnotation(SagaParticipant.class);
        for (Object bean : participants.values()) {
            SagaParticipant annotation = bean.getClass().getAnnotation(SagaParticipant.class);
            if (annotation == null) continue;

            String module = !annotation.module().isEmpty() ? annotation.module() : configModule;
            if (module.isEmpty()) continue;

            Set<String> actions = result.computeIfAbsent(module, k -> new HashSet<>());

            for (Method method : bean.getClass().getDeclaredMethods()) {
                SagaCommandHandler handler = method.getAnnotation(SagaCommandHandler.class);
                if (handler == null) continue;
                actions.add(handler.action().isEmpty() ? "*" : handler.action());
            }
        }
        return result;
    }

    private Map<String, Set<String>> fetchRemoteDefinitions() {
        String url = orchestratorUrl + "/saga-definitions";
        log.info("Fetching saga definitions from {}", url);

        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
        String json;
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new RuntimeException("HTTP " + response.statusCode() + " from " + url);
            }
            json = response.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while fetching saga definitions", e);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to fetch saga definitions from " + url, e);
        }

        Map<String, Set<String>> moduleActions = new LinkedHashMap<>();
        try {
            JsonNode root = objectMapper.readTree(json);
            JsonNode data = root.has("data") ? root.get("data") : root;

            Iterator<Map.Entry<String, JsonNode>> sagas = data.fields();
            while (sagas.hasNext()) {
                Map.Entry<String, JsonNode> saga = sagas.next();
                JsonNode steps = saga.getValue().get("steps");
                if (steps == null || !steps.isArray()) continue;

                for (JsonNode step : steps) {
                    String type = step.has("type") ? step.get("type").asText() : "";
                    if (!"KAFKA".equals(type)) continue;

                    String module = step.has("module") && !step.get("module").isNull()
                            ? step.get("module").asText() : null;
                    String action = step.has("action") ? step.get("action").asText() : null;

                    // Derive module from topic if module not set
                    if (module == null && step.has("topic") && !step.get("topic").isNull()) {
                        String topic = step.get("topic").asText();
                        if (topic.endsWith("-commands")) {
                            module = topic.replace("-commands", "");
                        }
                    }

                    if (module != null && action != null) {
                        moduleActions.computeIfAbsent(module, k -> new HashSet<>()).add(action);
                    }

                    // Also collect compensation actions
                    String compAction = step.has("compensationAction") && !step.get("compensationAction").isNull()
                            ? step.get("compensationAction").asText() : null;
                    if (module != null && compAction != null) {
                        moduleActions.get(module).add(compAction);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse saga definitions response", e);
        }

        log.info("Fetched saga definitions — modules: {}", moduleActions.keySet());
        return moduleActions;
    }
}
