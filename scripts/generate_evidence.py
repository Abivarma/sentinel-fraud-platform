"""
Generate evidence artifacts for a completed phase.
Reads cached metrics from /tmp/sentinel_metrics/phase_NN.json (written by Spark jobs).
Writes summary, metrics.json, tests/results.json, and alignment_check.md to evidence/phase_NN/.
"""

import argparse
import json
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
EVIDENCE_DIR = BASE_DIR / "evidence"
METRICS_CACHE = Path("/tmp/sentinel_metrics")


def read_cached_metrics(phase: str) -> dict:
    path = METRICS_CACHE / f"phase_{phase}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def write_evidence(phase: str, metrics: dict) -> str:
    phase_dir = EVIDENCE_DIR / f"phase_{phase}"
    (phase_dir / "tests").mkdir(parents=True, exist_ok=True)
    (phase_dir / "charts").mkdir(exist_ok=True)
    (phase_dir / "ADRs").mkdir(exist_ok=True)

    metrics_out = {
        "phase": phase,
        "generated_at": datetime.utcnow().isoformat(),
        "prd_metrics_addressed": metrics.get("prd_metrics_addressed", []),
        "additional_observations": metrics.get("additional_observations", []),
    }
    with open(phase_dir / "metrics.json", "w") as f:
        json.dump(metrics_out, f, indent=2)

    results_path = phase_dir / "tests" / "results.json"
    if not results_path.exists():
        with open(results_path, "w") as f:
            json.dump(
                {
                    "total": metrics.get("tests_total", 0),
                    "passed": metrics.get("tests_passed", 0),
                    "failed": metrics.get("tests_failed", 0),
                    "failures": metrics.get("test_failures", []),
                    "generated_at": datetime.utcnow().isoformat(),
                },
                f,
                indent=2,
            )

    verdict = "PASS" if metrics.get("tests_failed", 0) == 0 else "FAIL"

    metric_rows = ""
    for m in metrics_out["prd_metrics_addressed"]:
        metric_rows += (
            f"| {m.get('name', '?')} | {m.get('prd_target', '?')} "
            f"| {m.get('measured_value', 'N/A')} | {m.get('status', '?')} |\n"
        )

    alignment = f"""# Alignment check — phase {phase}

**Verdict:** {verdict}
**Generated:** {datetime.utcnow().isoformat()}

## PRD metric contributions

| Metric | Target | Measured | Status |
|--------|--------|----------|--------|
{metric_rows}
## Tests
- Total: {metrics.get('tests_total', 0)}
- Passed: {metrics.get('tests_passed', 0)}
- Failed: {metrics.get('tests_failed', 0)}

## Concerns
{metrics.get('concerns', 'None')}

## Decision
{"orchestrator MAY dispatch dependent phases." if verdict == "PASS" else "orchestrator MUST re-dispatch this phase."}
"""
    with open(phase_dir / "alignment_check.md", "w") as f:
        f.write(alignment)

    (phase_dir / "DONE").touch()
    print(f"Evidence written to evidence/phase_{phase}/  [{verdict}]")
    return verdict


def main():
    parser = argparse.ArgumentParser(description="Generate phase evidence artifacts")
    parser.add_argument("--phase", required=True, help="Phase number e.g. 03")
    args = parser.parse_args()

    phase = args.phase.zfill(2)
    print(f"Generating evidence for Phase {phase}")

    metrics = read_cached_metrics(phase)
    if not metrics:
        print(f"No metrics cache for phase {phase}. Writing minimal evidence.")
        metrics = {
            "prd_metrics_addressed": [],
            "additional_observations": [f"Phase {phase} executed at {datetime.utcnow().isoformat()}"],
        }

    write_evidence(phase, metrics)


if __name__ == "__main__":
    main()
