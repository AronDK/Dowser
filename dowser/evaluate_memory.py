"""Offline retrieval metrics and paired ITBench-AA campaign comparisons."""

import argparse
import json
import math
import random
import sqlite3
from collections import defaultdict
from contextlib import closing
from pathlib import Path

from .bench_data import PILOT


def retrieval_metrics(relevant, retrieved, k=24):
    relevant = dict(relevant)
    hits = list(dict.fromkeys(retrieved))[:k]
    positives = {key for key, grade in relevant.items() if grade > 0}
    recall = len(positives & set(hits)) / len(positives) if positives else 1.0
    dcg = sum(
        (2 ** relevant.get(key, 0) - 1) / math.log2(position + 2)
        for position, key in enumerate(hits)
    )
    ideal = sum(
        (2**grade - 1) / math.log2(position + 2)
        for position, grade in enumerate(sorted(relevant.values(), reverse=True)[:k])
    )
    return {"recall_at_24": recall, "ndcg_at_24": dcg / ideal if ideal else 1.0}


def clustered_difference(left, right, *, iterations=10000, seed=42):
    """Pair scenarios/seeds, then resample scenarios rather than treating seeds as IID."""
    keys = set(left) & set(right)
    if not keys:
        raise ValueError("no paired trials")
    groups = defaultdict(list)
    for scenario, trial_seed in sorted(keys):
        groups[scenario].append(
            right[scenario, trial_seed] - left[scenario, trial_seed]
        )
    means = [sum(values) / len(values) for values in groups.values()]
    rng = random.Random(seed)
    samples = sorted(
        sum(rng.choices(means, k=len(means))) / len(means) for _ in range(iterations)
    )
    return {
        "difference": sum(means) / len(means),
        "ci95": [
            samples[int(iterations * 0.025)],
            samples[min(iterations - 1, int(iterations * 0.975))],
        ],
        "paired_trials": len(keys),
        "scenarios": len(groups),
    }


def compare(campaigns):
    if set(campaigns) != {"A", "B", "C", "D"}:
        raise ValueError("supply all four memory arms")
    records, manifests, summaries = {}, {}, {}
    for arm, directory in campaigns.items():
        directory = Path(directory)
        manifest = json.loads((directory / "manifest.json").read_text())
        config = manifest["configuration"]
        if (
            config.get("provider") != "openai"
            or config.get("model") != "gpt-6-luna"
            or config.get("memory_arm", "A") != arm
            or config.get("assessments")
        ):
            raise ValueError(
                "campaign is not the requested frozen Decisions memory arm"
            )
        manifests[arm] = manifest
        rows = [
            json.loads(p.read_text())
            for p in sorted((directory / "trials").glob("*.json"))
        ]
        full = [r for r in rows if r["status"] == "completed" and r["phase"] == "full"]
        paired = {}
        for row in full:
            key = (row["scenario"], row["seed"])
            if key in paired:
                raise ValueError("duplicate scenario/seed")
            paired[key] = row["score"]
        records[arm] = paired
        summaries[arm] = {
            "trials": len(full),
            "complete": len(full) == 120 and len({s for s, _ in paired}) == 40,
            "mean_score": sum(paired.values()) / len(paired) if paired else None,
            "duration_seconds": sum(r.get("duration_seconds", 0) for r in full),
            "accounted_nanodollars": sum(
                r.get("accounted_nanodollars", 0) for r in full
            ),
            "unknown_calls": sum(r.get("unknown_calls", 0) for r in full),
            "failure_categories": {
                category: sum(r["category"] == category for r in full)
                for category in sorted({r["category"] for r in full})
            },
            "api_usage": [r.get("api_usage", []) for r in full],
            "retrieval": [r.get("retrieval", {}) for r in full],
        }
    baseline = manifests["A"]
    for arm, manifest in manifests.items():
        if any(
            manifest[key] != baseline[key]
            for key in (
                "dataset_fingerprint",
                "dataset_revision",
                "code_hashes",
                "dependency_lock_sha256",
                "python",
            )
        ):
            raise ValueError("campaign implementation or dataset differs")
        allowed = {"memory_arm", "memory_profile", "semantic_memory"}
        if {k: v for k, v in manifest["configuration"].items() if k not in allowed} != {
            k: v for k, v in baseline["configuration"].items() if k not in allowed
        }:
            raise ValueError("non-memory campaign settings differ")
    # B-D differ only in engine mode; all retrieval and account settings are fixed.
    memory_settings = [
        {
            k: v
            for k, v in manifests[arm]["configuration"]["semantic_memory"].items()
            if k != "mode"
        }
        for arm in ("B", "C", "D")
    ]
    if any(settings != memory_settings[0] for settings in memory_settings[1:]):
        raise ValueError("retrieval arm settings differ")
    heldout = {
        arm: {key: value for key, value in values.items() if key[0] not in PILOT}
        for arm, values in records.items()
    }
    comparisons = {}
    for name, subset in (("all", records), ("heldout", heldout)):
        comparisons[name] = {
            f"{left}_to_{right}": clustered_difference(subset[left], subset[right])
            for left, right in (
                ("A", "B"),
                ("B", "C"),
                ("A", "C"),
                ("A", "D"),
                ("C", "D"),
            )
        }
    complete = all(s["complete"] for s in summaries.values()) and all(
        set(values) == set(records["A"]) for values in records.values()
    )
    ac, ad, cd = (comparisons["heldout"][key] for key in ("A_to_C", "A_to_D", "C_to_D"))

    def quality(value):
        return value["difference"] >= 0.05 and value["ci95"][0] > 0

    recommendation = "retain_A"
    if complete and quality(ad) and cd["ci95"][0] > 0:
        recommendation = "D_eligible_after_correctness_gate"
    elif complete and quality(ac):
        recommendation = "C_eligible_after_correctness_gate"
    return {
        "arms": summaries,
        "complete": complete,
        "paired": comparisons,
        "recommendation": recommendation,
        "correctness_gate": "Verify offline scope/provenance and retrieval gates separately; campaign scores do not establish them.",
        "heldout_excludes": list(PILOT),
    }


def evaluate_retrieval(database, labels):
    """Score captured request prefixes read-only; labels never reach a provider."""
    results = []
    with closing(
        sqlite3.connect(Path(database).absolute().as_uri() + "?mode=ro", uri=True)
    ) as db:
        for label in labels:
            incident, sequence = label["request_ref"].rsplit(":", 1)
            sequence = int(sequence)
            row = db.execute(
                "SELECT payload FROM events WHERE incident_id=? AND sequence=? AND kind='provider_request'",
                (incident, sequence),
            ).fetchone()
            if row is None:
                raise ValueError("label requires a captured provider request")
            request = json.loads(row[0])["request"]
            memory = request.get("memory") or {}
            entries = [
                item
                for section in ("facts", "actions", "hypotheses")
                for item in memory.get(section, [])
            ]
            selected = [item["event_ref"] for item in entries if item.get("event_ref")]
            relevant = label["relevant"]
            for ref in [*selected, *relevant]:
                owner, source_sequence = ref.rsplit(":", 1)
                if (
                    owner != incident
                    or int(source_sequence) >= sequence
                    or not db.execute(
                        "SELECT 1 FROM events WHERE incident_id=? AND sequence=? AND kind IN ('incident_ingested','parse_outcome','execution_started','hypothesis_assessment')",
                        (owner, int(source_sequence)),
                    ).fetchone()
                ):
                    raise ValueError(
                        "unowned, future or inadmissible evidence reference"
                    )
            results.append(
                {
                    "request_ref": label["request_ref"],
                    **retrieval_metrics(relevant, selected),
                    "provenance_correct": True,
                    "omission_reference_present": not request.get("retrieval", {}).get(
                        "omitted_count"
                    )
                    or bool(request["retrieval"].get("omitted_reference")),
                }
            )
    if not results:
        raise ValueError("no labeled prefixes")
    recall = sum(r["recall_at_24"] for r in results) / len(results)
    return {
        "prefixes": results,
        "mean_recall_at_24": recall,
        "mean_ndcg_at_24": sum(r["ndcg_at_24"] for r in results) / len(results),
        "gate_passed": recall >= 0.9
        and all(
            r["provenance_correct"] and r["omission_reference_present"] for r in results
        ),
        "interpretation": "Captured retrieval quality only; does not establish model understanding or diagnosis accuracy.",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest="command", required=True)
    comparison = subs.add_parser("compare")
    for arm in "ABCD":
        comparison.add_argument("--" + arm.lower(), type=Path, required=True)
    retrieval = subs.add_parser("retrieval")
    retrieval.add_argument("--database", type=Path, required=True)
    retrieval.add_argument("--labels", type=Path, required=True)
    args = parser.parse_args(argv)
    result = (
        compare({arm: getattr(args, arm.lower()) for arm in "ABCD"})
        if args.command == "compare"
        else evaluate_retrieval(args.database, json.loads(args.labels.read_text()))
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
