"""Deterministic memory projection; original evidence always remains in history."""

import hashlib
import json

from .models import ActionIdentity, MemoryFact


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def action_identity(candidate):
    return ActionIdentity(
        key=digest(
            [
                candidate.tool,
                candidate.plugin_version,
                sorted(candidate.resources),
                candidate.args,
            ]
        )
    )


def scope_key(state):
    if state.memory_scope is None:
        return "incident:" + state.incident_id
    return digest(
        [
            state.memory_scope.model_dump(),
            sorted((r.platform, r.platform_version, r.id) for r in state.resources),
        ]
    )


def facts_from_parse(parsed):
    """Explicit plugin facts override the conservative per-observation fallback."""
    if parsed.memory_facts:
        return parsed.memory_facts
    return [
        MemoryFact(
            key=f"{o.resource_id}:{o.kind}",
            resource_id=o.resource_id,
            payload=o.payload,
            evidence_refs=[o.id, *o.evidence_refs],
        )
        for o in parsed.observations
        if o.kind not in {"investigation", "history"}
    ]


def compact(value, limit=600):
    """Bound display text only; provenance allows retrieval of the full value."""
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False)
    if len(encoded.encode()) <= limit:
        return value
    return {
        **{
            k: value[k]
            for k in ("entity", "kind")
            if isinstance(value, dict) and k in value
        },
        "excerpt": encoded.encode()[:limit].decode("utf-8", errors="ignore"),
        "abridged": True,
    }
