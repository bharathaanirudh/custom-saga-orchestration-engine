package com.anirudh.saga.core.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.anirudh.saga.core.domain.SagaDefinition;
import com.anirudh.saga.core.domain.StepDefinition;
import com.anirudh.saga.core.domain.StepType;
import com.anirudh.saga.core.exception.SagaDefinitionNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

@Component
public class SagaDefinitionLoader implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(SagaDefinitionLoader.class);

    private final Map<String, SagaDefinition> definitions = new HashMap<>();
    private final SagaDefinitionValidator validator = new SagaDefinitionValidator();
    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    @Override
    public void afterPropertiesSet() throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources;
        try {
            resources = resolver.getResources("classpath:sagas/*.yml");
        } catch (IOException e) {
            log.warn("No saga YAML files found in classpath:sagas/");
            return;
        }

        for (Resource resource : resources) {
            log.info("Loading saga definition from: {}", resource.getFilename());
            Map<String, Object> raw = yamlMapper.readValue(resource.getInputStream(), Map.class);
            SagaDefinition definition = parse(raw);
            validator.validate(definition);
            definitions.put(definition.name(), definition);
            log.info("Loaded saga definition: {}", definition.name());
        }
        log.info("Total saga definitions loaded: {}", definitions.size());
    }

    public SagaDefinition getDefinition(String sagaType) {
        SagaDefinition def = definitions.get(sagaType);
        if (def == null) throw new SagaDefinitionNotFoundException(sagaType);
        return def;
    }

    public Map<String, SagaDefinition> getAllDefinitions() {
        return Collections.unmodifiableMap(definitions);
    }

    @SuppressWarnings("unchecked")
    private SagaDefinition parse(Map<String, Object> raw) {
        String name = (String) raw.get("name");
        int timeoutMinutes = (int) raw.getOrDefault("timeoutMinutes", 30);
        String lockTargetType = (String) raw.get("lockTargetType");
        String lockTargetField = (String) raw.get("lockTargetField");

        List<Map<String, Object>> rawSteps = (List<Map<String, Object>>) raw.get("steps");
        List<StepDefinition> steps = new ArrayList<>();
        if (rawSteps != null) {
            for (Map<String, Object> s : rawSteps) {
                steps.add(new StepDefinition(
                        (String) s.get("name"),
                        StepType.valueOf((String) s.get("type")),
                        (String) s.get("action"),
                        (String) s.get("module"),
                        (String) s.get("topic"),
                        (String) s.get("url"),
                        (String) s.get("compensationAction"),
                        (String) s.get("compensationTopic"),
                        (String) s.get("compensationUrl"),
                        (int) s.getOrDefault("retryMaxAttempts", 3),
                        (int) s.getOrDefault("timeoutSeconds", 30),
                        s.get("fallback")  // P2-067: nullable Object — primitive/map/list per YAML
                ));
            }
        }
        return new SagaDefinition(name, timeoutMinutes, steps, lockTargetType, lockTargetField);
    }
}
