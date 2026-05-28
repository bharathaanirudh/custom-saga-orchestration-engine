package com.anirudh.saga.core.api;

import com.anirudh.saga.core.replay.BulkReplayReport;
import com.anirudh.saga.core.replay.DefinitionReplayService;
import com.anirudh.saga.core.replay.ReplayReport;
import org.springframework.web.bind.annotation.*;

/**
 * Time-machine replay endpoints (P2-027) — replay historical sagas against a different
 * definition. Read-only: no Mongo writes, no Kafka publish.
 *
 * <p>Distinct from {@code SagaController}'s `POST /sagas/{id}/replay` (P2-017, state
 * reconstruction): these endpoints re-simulate against a *new definition*.
 */
@RestController
@RequestMapping("/sagas")
public class SagaReplayController {

    private final DefinitionReplayService replayService;

    public SagaReplayController(DefinitionReplayService replayService) {
        this.replayService = replayService;
    }

    /** Replay one saga against a target definition. 404 if saga has no events or definition not loaded. */
    @PostMapping("/{sagaId}/replay-definition")
    public SagaResponse<ReplayReport> replayDefinition(@PathVariable String sagaId,
                                                       @RequestParam String definition) {
        return SagaResponse.ok(replayService.replay(sagaId, definition));
    }

    /** Bulk-replay all sagas of a type against a target definition (capped at 1000). */
    @PostMapping("/replay-definition/bulk")
    public SagaResponse<BulkReplayReport> bulkReplayDefinition(@RequestParam String sagaType,
                                                               @RequestParam String definition) {
        return SagaResponse.ok(replayService.bulkReplay(sagaType, definition));
    }
}
