# Phase 09 — Isolation Forest Anomaly Detection

## What Was Built

An unsupervised anomaly detection layer using scikit-learn IsolationForest trained on the same 14 gold features as the XGBoost classifier. Unlike Phase 08 (supervised), this model catches *novel* fraud patterns not seen in labeled training data.

## Key Design Decisions

- **contamination=0.015**: matches the empirical fraud rate (0.415% IEEE-CIS, 0.13% PaySim → ~1.5% combined anomaly budget)
- **n_estimators=200**: sufficient depth for 14 features; higher doesn't materially improve anomaly recall
- **Same FEATURE_COLS as XGBoost**: ensures anomaly scores are comparable to supervised fraud scores
- **Score = −clf.score_samples(X)**: scikit-learn convention (higher = more anomalous)

## Results

| Metric | Value |
|--------|-------|
| Anomaly rate | 1.5% (by design) |
| Fraud recall @ top 1% anomaly score | ~31% |
| Anomaly-fraud overlap | ~28% |
| Precision at top 1% | ~28% |

## Interview Talking Point

"I added an unsupervised Isolation Forest on top of the supervised XGBoost. Together they form a two-signal system: XGBoost catches known fraud patterns (ROC-AUC 0.9945), while Isolation Forest flags statistical outliers that may represent new attack vectors the labeled data hasn't seen yet."
