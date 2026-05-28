package com.anirudh.saga.core.replay;

import com.anirudh.saga.core.audit.SagaExecutionLog;
import com.anirudh.saga.core.domain.SagaDefinition;
import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.StepDefinition;
import com.anirudh.saga.core.exception.SagaNotFoundException;
import com.anirudh.saga.core.loader.SagaDefinitionLoader;
import com.anirudh.saga.core.repository.SagaExecutionLogRepository;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The "time machine" (P2-027): replay a historical saga against a *different* definition,
 * using the participant outcomes recorded in the original event log — fully sandboxed
 * (no Mongo writes, no Kafka publish).
 *
 * <p>Pipeline: (1) reconstruct per-step outcomes + the original step order + original final
 * status from {@code saga_execution_log}; (2) load the target definition (404 if absent);
 * (3) simulate the new definition's steps through a pure in-memory BTFC router seeded by the
 * recorded outcomes; (4) diff structure + final status.
 *
 * <p>The router mirrors the locked BTFC rules: SUCCESS → next step; BUSINESS_FAILURE →
 * compensate (terminal COMPENSATED); TECHNICAL_FAILURE → suspend (terminal SUSPENDED) unless
 * the step declares a fallback (P2-067), in which case it continues. New steps with no
 * recorded outcome assume SUCCESS for routing but are flagged in the diff.
 */
@Service
public class DefinitionReplayService {

    // Matches the suspension / compensation reason strings written by SagaOrchestrator.
    private static final Pattern BUSINESS_AT =
            Pattern.compile("Business failure at (?:HTTP )?step: ([^\\s—]+)");
    private static final Pattern TECHNICAL_AT =
            Pattern.compile("Technical failure at (?:HTTP )?step: ([^\\s—]+)");

    private final SagaExecutionLogRepository logRepository;
    private final SagaInstanceRepository instanceRepository;
    private final SagaDefinitionLoader definitionLoader;

    public DefinitionReplayService(SagaExecutionLogRepository logRepository,
                                   SagaInstanceRepository instanceRepository,
                                   SagaDefinitionLoader definitionLoader) {
        this.logRepository = logRepository;
        this.instanceRepository = instanceRepository;
        this.definitionLoader = definitionLoader;
    }

    /** Replay one saga against a target definition (by name). Throws 404 if the saga has no
     *  events, or {@link com.anirudh.saga.core.exception.SagaDefinitionNotFoundException} (404)
     *  if the target definition isn't loaded. */
    public ReplayReport replay(String sagaId, String definitionName) {
        List<SagaExecutionLog> events = logRepository.findBySagaIdOrderByTimestampAsc(sagaId);
        if (events.isEmpty()) throw new SagaNotFoundException(sagaId);

        Reconstructed orig = reconstruct(events);
        SagaDefinition target = definitionLoader.getDefinition(definitionName); // 404 if absent

        MockedStepExecutor mock = new MockedStepExecutor(orig.outcomes);

        List<ReplayReport.SimulatedStep> timeline = new ArrayList<>();
        String divergencePoint = null;
        String replayedFinalStatus = "COMPLETED";

        for (StepDefinition step : target.steps()) {
            StepOutcome recorded = mock.outcomeFor(step.name());
            StepOutcome applied = (recorded == StepOutcome.UNKNOWN) ? StepOutcome.SUCCESS : recorded;
            timeline.add(new ReplayReport.SimulatedStep(step.name(), step.type().name(), step.action(), recorded));

            if (applied == StepOutcome.BUSINESS_FAILURE) {
                replayedFinalStatus = "COMPENSATED";
                divergencePoint = step.name();
                break;
            }
            if (applied == StepOutcome.TECHNICAL_FAILURE) {
                if (step.hasFallback()) continue; // P2-067: fallback continues the saga
                replayedFinalStatus = "SUSPENDED";
                divergencePoint = step.name();
                break;
            }
        }

        ReplayReport.ReplayDiff diff = buildDiff(orig, target, replayedFinalStatus, divergencePoint);

        return new ReplayReport(sagaId, definitionName, orig.outcomes, orig.finalStatus,
                timeline, replayedFinalStatus, diff);
    }

    /** Bulk replay all sagas of a type against a definition (AC-4), capped at {@link BulkReplayReport#CAP}. */
    public BulkReplayReport bulkReplay(String sagaType, String definitionName) {
        // Validate the target definition once, up front (404 if absent).
        definitionLoader.getDefinition(definitionName);

        List<SagaInstance> sagas = instanceRepository.findBySagaType(sagaType).stream()
                .limit(BulkReplayReport.CAP)
                .toList();

        int same = 0;
        List<String> diverged = new ArrayList<>();
        for (SagaInstance saga : sagas) {
            ReplayReport r = replay(saga.getSagaId(), definitionName);
            if (r.diff().finalStatusChanged()) diverged.add(saga.getSagaId());
            else same++;
        }
        return new BulkReplayReport(definitionName, sagaType, sagas.size(), same, diverged.size(), diverged);
    }

    // --- reconstruction from the event log ---

    private record Reconstructed(Map<String, StepOutcome> outcomes, List<String> originalStepOrder, String finalStatus) {}

    private Reconstructed reconstruct(List<SagaExecutionLog> events) {
        Map<String, StepOutcome> outcomes = new LinkedHashMap<>();
        Set<String> stepOrder = new LinkedHashSet<>();
        String finalStatus = null;

        for (SagaExecutionLog e : events) {
            String ev = e.getEvent();
            switch (ev) {
                case "STEP_STARTED" -> stepOrder.add(e.getStepName());
                case "STEP_COMPLETED" -> {
                    stepOrder.add(e.getStepName());
                    outcomes.put(e.getStepName(), StepOutcome.SUCCESS);
                }
                case "COMPENSATION_STARTED" -> {
                    String step = firstGroup(BUSINESS_AT, e.getData());
                    if (step != null) outcomes.put(step, StepOutcome.BUSINESS_FAILURE);
                }
                case "SUSPENDED" -> {
                    finalStatus = "SUSPENDED";
                    String step = firstGroup(TECHNICAL_AT, e.getData());
                    if (step != null) outcomes.put(step, StepOutcome.TECHNICAL_FAILURE);
                }
                case "COMPLETED" -> finalStatus = "COMPLETED";
                case "COMPENSATED" -> finalStatus = "COMPENSATED";
                case "FAILED" -> finalStatus = "FAILED";
                default -> { /* STARTED, COMPENSATION_STEP_*, RETRY_FROM_SUSPENDED, STEP_FALLBACK_APPLIED — no outcome */ }
            }
        }
        return new Reconstructed(outcomes, new ArrayList<>(stepOrder), finalStatus);
    }

    private ReplayReport.ReplayDiff buildDiff(Reconstructed orig, SagaDefinition target,
                                              String replayedFinalStatus, String divergencePoint) {
        List<String> newSteps = target.steps().stream().map(StepDefinition::name).toList();

        List<String> added = new ArrayList<>(newSteps);
        added.removeAll(orig.originalStepOrder);

        List<String> removed = new ArrayList<>(orig.originalStepOrder);
        removed.removeAll(newSteps);

        // Reordered: the relative order of shared steps differs between original and new.
        List<String> sharedOrig = new ArrayList<>(orig.originalStepOrder);
        sharedOrig.retainAll(newSteps);
        List<String> sharedNew = new ArrayList<>(newSteps);
        sharedNew.retainAll(orig.originalStepOrder);
        boolean reordered = !sharedOrig.equals(sharedNew);

        boolean finalChanged = orig.finalStatus != null && !orig.finalStatus.equals(replayedFinalStatus);

        return new ReplayReport.ReplayDiff(added, removed, reordered, divergencePoint, finalChanged);
    }

    private String firstGroup(Pattern p, String text) {
        if (text == null) return null;
        Matcher m = p.matcher(text);
        return m.find() ? m.group(1) : null;
    }
}
