# Problem Statement — Financial Transaction Fraud Detection

**PRD reference:** §1.2

## The problem

Financial institutions process millions of transactions daily. Card fraud costs the global industry tens of billions annually. Fraud teams face two pain points:

1. **Real-time detection:** Sub-second scoring of live transactions. Rule-based systems have high false-positive rates, wasting analyst time.
2. **Investigation efficiency:** Each flagged transaction takes 5–15 minutes for an analyst to investigate — a bottleneck at scale.

## The solution

1. **Streaming fraud scoring:** Kafka-backed Spark Structured Streaming pipeline, enriches live transactions with online features (Feast/Redis), scores with an XGBoost + IsolationForest ensemble. Target: p95 < 2 seconds E2E.
2. **Agentic investigator:** Given a flagged transaction ID, a LangGraph agent assembles a case file with similar cases, feature explanations, and a grounded cited report. Target: ≥ 50% reduction in time-to-decision.

## Success criteria (PRD §2)

| Category | Metric | Target |
|---|---|---|
| Data platform | Batch throughput | ≥ 1M transactions/minute |
| Data platform | Streaming E2E latency | p95 < 2 seconds |
| Classical ML | AUC-PR | ≥ 0.85 |
| Classical ML | Precision @ top 1% | ≥ 80% |
| GenAI | Report faithfulness | ≥ 0.85 (RAGAS) |
| GenAI | Guardrail catch rate | ≥ 95% |

## Datasets

- **IEEE-CIS Fraud Detection** (Kaggle): ~590k labelled transactions, 394 features
- **PaySim synthetic mobile money**: ~6M transactions, documented fraud labels
- **Synthetic real-time generator**: Python service producing realistic Kafka events
