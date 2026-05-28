# Saga Orchestrator Dashboards

Prometheus + Grafana provisioning bundled with the engine. Auto-loads on `docker compose up`.

## What you get

- **Prometheus** at http://localhost:9090 — scrapes `host.docker.internal:8080/actuator/prometheus` every 15s.
- **Grafana** at http://localhost:3000 (admin / admin, anonymous-Viewer enabled) with the **Saga Orchestrator — Overview** dashboard pre-loaded under the `Saga` folder.

## Panels

| Panel | Metric source | Why |
|---|---|---|
| Saga rate by type and status | `saga_started_total`, `saga_completed_total`, `saga_compensated_total`, `saga_suspended_total`, `saga_failed_total` (all tagged by `sagaType`) | Throughput + outcome at a glance |
| Step failure heatmap | `saga_step_failed_total{stepName, failureType}` | Pinpoints which steps fail and why (BUSINESS vs TECHNICAL) |
| Saga duration p50/p95/p99 | `saga_duration_seconds_bucket{sagaType}` | End-to-end latency per saga type |
| Step duration p50/p95/p99 | `saga_step_duration_seconds_bucket{stepName}` | Per-step latency |
| Suspended sagas (current count) | derived | Operator's primary alert target |
| Step executions / sec | `saga_step_executed_total{sagaType, stepName}` | Step throughput |

## Manual import (without compose)

If you already run Prometheus + Grafana elsewhere:

```
# Grafana → Dashboards → Import → Upload JSON
dashboards/grafana/saga-overview.json
```

When prompted for the datasource, pick your Prometheus instance.

## Customizing the scrape target

The bundled `docker/prometheus/prometheus.yml` scrapes `host.docker.internal:8080`. To scrape a different host or port, edit that file and `docker compose restart prometheus`.

## Cardinality cap

`SagaMetrics` enforces a cardinality cap on the `sagaType` tag (default 100, configurable via `saga.metrics.max-tag-cardinality`). Beyond the cap, additional types fall back to `OTHER` to keep Prometheus storage bounded.
