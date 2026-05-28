"""
Phase 10 — LLM Fraud Explanation Engine
Given a flagged transaction + its feature values + model score,
generates a plain-English explanation using Groq (llama-3.3-70b).

Design principles:
- Prompt is templated, not hardcoded — easy to tune
- Structured output: summary + risk_factors + recommended_action
- Falls back to rule-based explanation if LLM is unavailable
- Never sends raw customer PII to the LLM — only anonymized feature values
"""

import os
import json
from typing import Optional
from groq import Groq


GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")

SYSTEM_PROMPT = """You are a senior fraud analyst at a financial institution.
Your job is to explain why a transaction was flagged by the fraud detection system.
Be concise, specific, and actionable. Write for a fraud operations team.
Always respond in valid JSON with keys: summary, risk_factors (list), recommended_action.
Do NOT include any markdown formatting — pure JSON only."""

EXPLANATION_TEMPLATE = """A transaction has been flagged by our ML model with fraud probability {fraud_score:.1%}.

Transaction context:
- Amount: ${amount:.2f} (z-score: {amount_zscore:+.2f} vs customer average)
- Time: {hour_of_day}:00 {night_label}  |  {day_label}
- Is round amount: {round_amount_label}

Customer velocity (recent activity):
- Last 1 hour:   {customer_tx_count_1h} transactions
- Last 6 hours:  {customer_tx_count_6h} transactions
- Last 24 hours: {customer_tx_count_24h} transactions
- Last 7 days:   {customer_tx_count_7d} transactions
- Velocity spike flag: {velocity_label}

Historical risk signals:
- Customer 30-day fraud rate: {fraud_rate_30d:.1%}
- Amount vs customer mean ratio: {amount_vs_mean_ratio:.2f}x
- log(1+amount): {amount_log1p:.2f}  |  bucket: {amount_bucket}

Top model features that drove this score (SHAP-style ranking):
{top_features}

Respond in JSON: {{"summary": "...", "risk_factors": ["...", "..."], "recommended_action": "..."}}"""


def get_top_features(feature_values: dict) -> str:
    """Rank features by deviation from typical (rule-based, no SHAP needed)."""
    signals = []
    if feature_values.get("is_velocity_spike"):
        signals.append(f"1. VELOCITY SPIKE — {feature_values['customer_tx_count_1h']} txns in 1 hour")
    if abs(feature_values.get("amount_zscore", 0)) > 2:
        signals.append(f"2. UNUSUAL AMOUNT — {feature_values['amount_zscore']:+.1f} standard deviations")
    if feature_values.get("is_night"):
        signals.append("3. NIGHT-TIME TRANSACTION — unusual hours increase fraud risk")
    if feature_values.get("fraud_rate_30d", 0) > 0.05:
        signals.append(f"4. HIGH-RISK CUSTOMER — {feature_values['fraud_rate_30d']:.1%} fraud rate in last 30 days")
    if feature_values.get("is_round_amount"):
        signals.append("5. ROUND AMOUNT — often used in card testing attacks")
    if feature_values.get("amount_vs_mean_ratio", 1.0) > 3:
        signals.append(f"6. AMOUNT SPIKE — {feature_values['amount_vs_mean_ratio']:.1f}x above customer average")
    return "\n".join(signals) if signals else "No single dominant signal — ensemble of weak indicators"


def explain_fraud(
    transaction_id: str,
    feature_values: dict,
    fraud_score: float,
    client: Optional[Groq] = None,
) -> dict:
    """Generate a structured fraud explanation for a single transaction."""
    if client is None:
        client = Groq(api_key=os.environ["GROQ_API_KEY"])

    top_features = get_top_features(feature_values)
    fv = dict(feature_values)
    fv['night_label'] = '(night-time)' if fv.get('is_night') else '(daytime)'
    fv['day_label'] = 'Weekend' if fv.get('is_weekend') else 'Weekday'
    fv['round_amount_label'] = 'Yes (common in fraud testing)' if fv.get('is_round_amount') else 'No'
    fv['velocity_label'] = 'YES — unusual burst' if fv.get('is_velocity_spike') else 'No'
    prompt = EXPLANATION_TEMPLATE.format(
        fraud_score=fraud_score,
        top_features=top_features,
        **fv,
    )

    try:
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": prompt},
            ],
            max_tokens=400,
            temperature=0.2,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content
        explanation = json.loads(raw)
        explanation["transaction_id"] = transaction_id
        explanation["fraud_score"] = round(fraud_score, 4)
        explanation["model"] = response.model
        explanation["tokens_used"] = response.usage.total_tokens
        return explanation

    except Exception as e:
        # Rule-based fallback — system stays up even if LLM is unavailable
        return {
            "transaction_id": transaction_id,
            "fraud_score": round(fraud_score, 4),
            "summary": f"Transaction flagged at {fraud_score:.1%} confidence. Manual review required.",
            "risk_factors": [s.lstrip("0123456789. ") for s in top_features.split("\n")],
            "recommended_action": "Hold for manual review",
            "model": "rule-based-fallback",
            "error": str(e),
        }


def batch_explain(transactions: list[dict], fraud_scores: list[float]) -> list[dict]:
    """Explain a batch of flagged transactions."""
    client = Groq(api_key=os.environ["GROQ_API_KEY"])
    results = []
    for tx, score in zip(transactions, fraud_scores):
        tx_id = tx.pop("transaction_id", "UNKNOWN")
        result = explain_fraud(tx_id, tx, score, client)
        results.append(result)
    return results
