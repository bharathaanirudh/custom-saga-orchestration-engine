package com.anirudh.saga.core.api;

import com.anirudh.saga.core.domain.SagaDefinition;
import com.anirudh.saga.core.loader.SagaDefinitionLoader;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Exposes loaded saga definitions for SDK participant validation at startup.
 * Participants call GET /saga-definitions to verify their module/actions match.
 */
@RestController
@RequestMapping("/saga-definitions")
public class SagaDefinitionController {

    private final SagaDefinitionLoader definitionLoader;

    public SagaDefinitionController(SagaDefinitionLoader definitionLoader) {
        this.definitionLoader = definitionLoader;
    }

    @GetMapping
    public SagaResponse<Map<String, SagaDefinition>> getAll() {
        return SagaResponse.ok(definitionLoader.getAllDefinitions());
    }
}
