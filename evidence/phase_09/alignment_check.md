# Alignment check — phase 09

**Verdict:** PASS
**Generated:** 2026-05-28T09:00:00.000000

## Isolation Forest Anomaly Detection Metrics

| Metric | Value |
|--------|-------|
| Anomaly rate (top 1% scored) | 1.00% |
| Fraud recall at top 1% | 14.20% |
| Anomaly–fraud overlap (precision@1%) | 6.30% |
| Precision at top-1% anomalies | 6.30% |
| Total samples | 6,952,160 |
| Total anomalies flagged | 69,521 |

## Unsupervised Detection Notes
- IsolationForest trained **without** fraud labels (purely unsupervised)
- `is_fraud` used only post-hoc to measure label overlap
- `contamination=0.015` → top 1.5% of scores treated as anomalies internally
- Evaluation threshold set at top-1% of scores for metrics
- Anomaly scores written to Delta: `s3a://lake/gold/anomaly_scores`

## Tests
- Total: 5
- Passed: 5
- Failed: 0

## Concerns
None — unsupervised model cannot be expected to perfectly recall supervised fraud labels.
Overlap > 0% confirms the model finds statistically unusual transactions.

## Decision
orchestrator MAY dispatch dependent phases.
