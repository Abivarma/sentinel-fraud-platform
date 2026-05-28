# ADR-001 — Domain: Financial Fraud Detection

**Date:** 2024-01-01  
**Status:** Accepted  
**PRD reference:** §1

## Context

Choosing a domain that maximises interview signal for a senior/staff AI engineer portfolio. Requirements: clear business cost, supports classical ML + GenAI, free public datasets, justifies compliance/guardrail requirements, maps to real production systems.

## Decision

Financial transaction fraud detection.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Cybersecurity log anomaly | Fewer labelled datasets; harder to explain business value |
| IoT sensor anomaly | Weak GenAI angle; harder to justify agentic investigator |
| Recommendation system | No fraud/compliance angle; weaker DE portfolio story |
| Healthcare anomaly | PII complexity; limited free labelled datasets |
| E-commerce search ranking | No streaming fraud angle; weak for ML + GenAI combo |

## Consequences

- **Business framing:** Card fraud costs the global industry tens of billions annually. Analyst case investigation takes 5–15 minutes. Both are measurable.
- **Dataset availability:** IEEE-CIS (~590k labelled) + PaySim (~6M synthetic) provide volume.
- **Compliance angle:** PII handling, financial advice refusal, and audit logging give guardrails real teeth.
- **Interview moment:** "I chose this domain because it has a direct dollar cost, a measurable analyst-time reduction, and regulatory complexity that makes guardrails a genuine engineering concern."
