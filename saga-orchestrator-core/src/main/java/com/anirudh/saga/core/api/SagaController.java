package com.anirudh.saga.core.api;

import com.anirudh.saga.core.audit.ReplayResult;
import com.anirudh.saga.core.audit.SagaExecutionLog;
import com.anirudh.saga.core.audit.SagaReplayService;
import com.anirudh.saga.core.diagnosis.Diagnosis;
import com.anirudh.saga.core.diagnosis.SagaDiagnosisService;
import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;
import com.anirudh.saga.core.engine.SagaOrchestrator;
import com.anirudh.saga.core.exception.SagaNotFoundException;
import com.anirudh.saga.core.repository.SagaExecutionLogRepository;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import com.anirudh.saga.sdk.contract.SagaStartRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/sagas")
public class SagaController {

    private static final Logger log = LoggerFactory.getLogger(SagaController.class);

    private final SagaInstanceRepository repository;
    private final SagaExecutionLogRepository logRepository;
    private final SagaOrchestrator orchestrator;
    private final SagaDiagnosisService diagnosisService;
    private final SagaReplayService replayService;

    public SagaController(SagaInstanceRepository repository,
                          SagaExecutionLogRepository logRepository,
                          SagaOrchestrator orchestrator,
                          SagaDiagnosisService diagnosisService,
                          SagaReplayService replayService) {
        this.repository = repository;
        this.logRepository = logRepository;
        this.orchestrator = orchestrator;
        this.diagnosisService = diagnosisService;
        this.replayService = replayService;
    }

    @PostMapping
    public SagaResponse<SagaInstance> start(@RequestBody SagaStartRequest request) {
        log.info("Starting saga type={}", request.sagaType());
        SagaInstance instance = orchestrator.start(request);
        return SagaResponse.ok(instance);
    }

    @GetMapping("/{sagaId}")
    public SagaResponse<Map<String, Object>> get(@PathVariable String sagaId) {
        SagaInstance instance = repository.findBySagaId(sagaId)
                .orElseThrow(() -> new SagaNotFoundException(sagaId));
        List<SagaExecutionLog> timeline = logRepository.findBySagaIdOrderByTimestampAsc(sagaId);

        // P2-038: embed a structured diagnosis when the saga needs operator attention.
        // Field is absent for IN_PROGRESS / COMPLETED / COMPENSATED — diagnosis is
        // meaningless mid-execution or on the happy path.
        Map<String, Object> body = new HashMap<>();
        body.put("saga", instance);
        body.put("timeline", timeline);
        Optional<Diagnosis> diagnosis = diagnosisService.diagnose(instance, timeline);
        diagnosis.ifPresent(d -> body.put("diagnosis", d));
        return SagaResponse.ok(body);
    }

    @GetMapping("/{sagaId}/events")
    public SagaResponse<List<SagaExecutionLog>> events(@PathVariable String sagaId) {
        // P2-016: ordered immutable event stream for a saga. 404 if the saga doesn't exist.
        repository.findBySagaId(sagaId).orElseThrow(() -> new SagaNotFoundException(sagaId));
        return SagaResponse.ok(logRepository.findBySagaIdOrderByTimestampAsc(sagaId));
    }

    @PostMapping("/{sagaId}/replay")
    public SagaResponse<ReplayResult> replay(@PathVariable String sagaId,
                                             @RequestParam(required = false) String upTo) {
        // P2-017: read-only reconstruction of saga state from its event history.
        // `upTo` (ISO-8601 instant) reconstructs state as of that point; omit for full history.
        // Does NOT require the live SagaInstance to exist (AC-4: derivable from events alone) —
        // but if there are no events at all, that's a genuine not-found.
        Instant cutoff = (upTo != null && !upTo.isBlank()) ? Instant.parse(upTo) : null;
        if (logRepository.findBySagaIdOrderByTimestampAsc(sagaId).isEmpty()) {
            throw new SagaNotFoundException(sagaId);
        }
        return SagaResponse.ok(replayService.replay(sagaId, cutoff));
    }

    @GetMapping
    public SagaResponse<List<SagaInstance>> list(@RequestParam(required = false) String status) {
        List<SagaInstance> results;
        if (status != null) {
            results = repository.findByStatus(SagaStatus.valueOf(status.toUpperCase()));
        } else {
            results = repository.findAll();
        }
        return SagaResponse.ok(results);
    }

    @PostMapping("/{sagaId}/retry")
    public SagaResponse<String> retry(@PathVariable String sagaId) {
        log.info("[sagaId={}] Manual retry requested", sagaId);
        orchestrator.retryFromSuspended(sagaId);
        return SagaResponse.ok("Saga retry triggered");
    }

    @PostMapping("/{sagaId}/compensate")
    public SagaResponse<String> compensate(@PathVariable String sagaId) {
        log.info("[sagaId={}] Manual compensation requested", sagaId);
        orchestrator.triggerCompensation(sagaId);
        return SagaResponse.ok("Saga compensation triggered");
    }
}
