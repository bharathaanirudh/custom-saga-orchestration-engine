package com.anirudh.saga.sdk.config;

import com.anirudh.saga.sdk.annotation.Idempotent;
import com.anirudh.saga.sdk.annotation.SagaCommandHandler;
import com.anirudh.saga.sdk.annotation.SagaParticipant;
import com.anirudh.saga.sdk.contract.*;
import com.anirudh.saga.sdk.idempotency.ProcessedCommandRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.*;

@Component
public class SagaCommandHandlerRegistrar implements BeanPostProcessor {

    private static final Logger log = LoggerFactory.getLogger(SagaCommandHandlerRegistrar.class);

    private final KafkaListenerEndpointRegistry endpointRegistry;
    private final ConcurrentKafkaListenerContainerFactory<String, String> containerFactory;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final ProcessedCommandRepository processedCommandRepository;
    private final String configModule; // from application.yml: saga.participant.module
    private int endpointCounter = 0;

    public SagaCommandHandlerRegistrar(
            KafkaListenerEndpointRegistry endpointRegistry,
            ConcurrentKafkaListenerContainerFactory<String, String> containerFactory,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            Optional<ProcessedCommandRepository> processedCommandRepository,
            @org.springframework.beans.factory.annotation.Value("${saga.participant.module:}") String configModule) {
        this.endpointRegistry = endpointRegistry;
        this.containerFactory = containerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.processedCommandRepository = processedCommandRepository.orElse(null);
        this.configModule = configModule;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> beanClass = bean.getClass();
        SagaParticipant participant = beanClass.getAnnotation(SagaParticipant.class);
        if (participant == null) return bean;

        // Priority: annotation > application.yml
        String module = !participant.module().isEmpty() ? participant.module() : configModule;

        // Collect all handler methods grouped by resolved topic
        Map<String, List<ActionHandler>> topicHandlers = new LinkedHashMap<>();

        for (Method method : beanClass.getDeclaredMethods()) {
            SagaCommandHandler annotation = method.getAnnotation(SagaCommandHandler.class);
            if (annotation == null) continue;

            validateHandlerMethod(method, beanName);

            String topic = resolveTopic(annotation, module, beanName);
            String groupId = resolveGroupId(annotation, module);
            String action = annotation.action();
            boolean idempotent = method.isAnnotationPresent(Idempotent.class);

            topicHandlers.computeIfAbsent(topic + "|" + groupId, k -> new ArrayList<>())
                    .add(new ActionHandler(method, action, idempotent));
        }

        // Register one Kafka listener per topic
        for (Map.Entry<String, List<ActionHandler>> entry : topicHandlers.entrySet()) {
            String[] parts = entry.getKey().split("\\|", 2);
            String topic = parts[0];
            String groupId = parts[1];

            SagaCommandListenerAdapter adapter = new SagaCommandListenerAdapter(
                    bean, entry.getValue(), kafkaTemplate, objectMapper, processedCommandRepository);

            MethodKafkaListenerEndpoint<String, String> endpoint = new MethodKafkaListenerEndpoint<>();
            endpoint.setId("saga-handler-" + (++endpointCounter));
            endpoint.setGroupId(groupId);
            endpoint.setTopics(topic);
            endpoint.setBean(adapter);
            endpoint.setMethod(getAdapterMethod());
            endpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());

            endpointRegistry.registerListenerContainer(endpoint, containerFactory, true);
            log.info("Registered saga listener: topic={} groupId={} actions={} bean={}",
                    topic, groupId,
                    entry.getValue().stream().map(h -> h.action().isEmpty() ? "*" : h.action()).toList(),
                    beanClass.getSimpleName());
        }
        return bean;
    }

    private void validateHandlerMethod(Method method, String beanName) {
        if (method.getParameterCount() != 1 || !SagaCommand.class.equals(method.getParameterTypes()[0])) {
            throw new IllegalStateException("@SagaCommandHandler " + beanName + "." + method.getName()
                    + " must take exactly one SagaCommand parameter");
        }
        if (!SagaReply.class.equals(method.getReturnType())) {
            throw new IllegalStateException("@SagaCommandHandler " + beanName + "." + method.getName()
                    + " must return SagaReply");
        }
    }

    private String resolveTopic(SagaCommandHandler annotation, String module, String beanName) {
        if (!annotation.topic().isEmpty()) return annotation.topic();
        if (!module.isEmpty()) return module + "-commands";
        throw new IllegalStateException("@SagaCommandHandler on " + beanName
                + " has no topic and @SagaParticipant has no module — cannot derive topic");
    }

    private String resolveGroupId(SagaCommandHandler annotation, String module) {
        if (!annotation.groupId().isEmpty()) return annotation.groupId();
        if (!module.isEmpty()) return module + "-group";
        return "saga-participant";
    }

    private Method getAdapterMethod() {
        try {
            return SagaCommandListenerAdapter.class.getMethod("onMessage", ConsumerRecord.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("SagaCommandListenerAdapter.onMessage not found", e);
        }
    }

    public record ActionHandler(Method method, String action, boolean idempotent) {}
}
