import os
import json
import time
from groq import Groq
from .models import ScoreRequest, RiskLevel

GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_SYSTEM = """You are a fraud analyst AI. Given transaction features and a fraud score, write a 2-sentence plain-English explanation for a fraud reviewer. Be specific about which signals drove the score. Output JSON: {"explanation": "...", "confidence": "high|medium|low"}"""


def explain(
    request: ScoreRequest,
    fraud_score: float,
    risk_level: str,
    top_factors: list[str],
) -> tuple[str, float]:
    """Returns (explanation_text, latency_ms). Falls back to rule-based if Groq unavailable."""
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        return _rule_based_explanation(fraud_score, top_factors), 0.0

    t0 = time.time()
    try:
        client = Groq(api_key=api_key)
        msg = (
            f"Transaction {request.transaction_id}: ${request.amount:.2f}, "
            f"fraud_score={fraud_score:.3f}, risk={risk_level}\n"
            f"Top signals: {', '.join(top_factors)}\n"
            f"Hour: {request.hour_of_day}h, "
            f"Velocity 1h/24h: {request.customer_tx_count_1h}/{request.customer_tx_count_24h}"
        )
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": GROQ_SYSTEM},
                {"role": "user", "content": msg},
            ],
            response_format={"type": "json_object"},
            max_tokens=200,
            temperature=0.2,
        )
        result = json.loads(resp.choices[0].message.content)
        latency = (time.time() - t0) * 1000
        return result.get("explanation", _rule_based_explanation(fraud_score, top_factors)), latency
    except Exception:
        return _rule_based_explanation(fraud_score, top_factors), 0.0


def _rule_based_explanation(score: float, factors: list[str]) -> str:
    level = "high risk" if score > 0.5 else "elevated risk"
    return f"Transaction flagged as {level} (score={score:.2f}). Key signals: {'; '.join(factors[:2])}."
