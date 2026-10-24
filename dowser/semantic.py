"""Versioned memory text projections and deterministic local graph retrieval."""

import json
import re
from collections import defaultdict
from dataclasses import dataclass

from .memory import digest
from .models import MemoryQueryIntent
from .tokens import estimate

RENDERER_VERSION = "semantic/1"
ADMIN_FIELDS = {"managedFields", "resourceVersion", "creationTimestamp"}


def render(value):
    def clean(item):
        if isinstance(item, dict):
            return {k: clean(v) for k, v in item.items() if k not in ADMIN_FIELDS}
        if isinstance(item, list):
            return [clean(v) for v in item]
        return item

    return json.dumps(
        clean(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def chunks(value, model, limit=1024):
    """Split structured content first; long leaves retain explicit segment markers."""
    text = render(value)
    if estimate(text, model)["estimated_tokens"] <= limit:
        return [text]
    if isinstance(value, dict) and len(value) > 1:
        return [part for k, v in value.items() for part in chunks({k: v}, model, limit)]
    if isinstance(value, dict):
        key, child = next(iter(value.items()))
        if isinstance(child, list) and len(child) > 1:
            return [
                part for item in child for part in chunks({key: [item]}, model, limit)
            ]
        if isinstance(child, dict) and len(child) > 1:
            return [
                part
                for k, v in child.items()
                for part in chunks({key: {k: v}}, model, limit)
            ]
    # Find character boundaries without slicing a UTF-8 codepoint or tokenizer bytes.
    parts = []
    while text:
        low, high = 1, len(text)
        while low < high:
            middle = (low + high + 1) // 2
            if estimate(text[:middle], model)["estimated_tokens"] <= limit:
                low = middle
            else:
                high = middle - 1
        parts.append(text[:low])
        text = text[low:]
    return parts


def terms(text):
    return set(re.findall(r"[\w./:-]+", text.lower()))


def document_texts(value, entity, tool, evidence_kind, model, limit):
    """Retain target identity on every chunk rather than only the first record."""
    header = render({"entity": entity, "tool": tool, "evidence_kind": evidence_kind})
    allowance = limit - estimate(header, model)["estimated_tokens"] - 8
    if allowance <= 0:
        raise ValueError("evidence identity exceeds embedding chunk budget")
    return [header + "\n" + part for part in chunks(value, model, allowance)]


def query_intents(state, candidates, hints=(), limit=8):
    """No generated prose or platform-specific symptom-to-command dictionary."""
    values = [
        MemoryQueryIntent(
            text=render({"alert": state.alert, "desired_state": state.desired_state}),
            source="alert",
        )
    ]
    focus = sorted(
        {str(o.payload["focus"]) for o in state.observations if o.payload.get("focus")}
    )
    if focus or state.unresolved_questions:
        values.append(
            MemoryQueryIntent(
                text=render({"focus": focus, "questions": state.unresolved_questions}),
                source="focus",
            )
        )
    # Saved recall is never a new output query seed.
    observations = [
        o for o in state.observations if o.kind not in {"history", "memory_retrieval"}
    ]
    for observation in observations[-2:]:
        values.append(
            MemoryQueryIntent(
                text=render(
                    {
                        "resource": observation.resource_id,
                        "kind": observation.kind,
                        "output": observation.payload,
                    }
                ),
                source="output",
            )
        )
    values.extend(MemoryQueryIntent.model_validate(h) for h in hints)
    target = terms(values[0].text + " " + " ".join(focus))
    ordered = sorted(
        enumerate(candidates),
        key=lambda item: (
            -len(
                target
                & terms(
                    render(
                        {
                            "tool": item[1].tool,
                            "args": item[1].args,
                            "description": item[1].description,
                        }
                    )
                )
            ),
            item[0],
        ),
    )
    seen_tools = set()
    for _, candidate in ordered:
        group = (
            candidate.tool,
            candidate.args.get("operation"),
            candidate.args.get("kind"),
        )
        if group in seen_tools or candidate.tool in {
            "memory.search",
            "history.query",
        }:
            continue
        seen_tools.add(group)
        values.append(
            MemoryQueryIntent(
                text=render(
                    {
                        "tool": candidate.tool,
                        "args": candidate.args,
                        "description": candidate.description,
                    }
                ),
                source="action",
            )
        )
    result = {}
    for value in values:
        # Queries are bounded independently from evidence; publish query text actually used.
        value = value.model_copy(
            update={"text": chunks(value.text, "text-embedding-3-small")[0]}
        )
        result.setdefault(digest(value.model_dump()), value)
        if len(result) == limit:
            break
    return result


def relationships(value):
    """Only explicit relationship records; inferred causality is never a graph edge."""
    edges = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "relationships" and isinstance(child, list):
                for edge in child:
                    if (
                        isinstance(edge, (list, tuple))
                        and len(edge) == 3
                        and all(isinstance(x, str) for x in edge)
                    ):
                        edges.add(tuple(edge))
                    elif isinstance(edge, dict) and all(
                        isinstance(edge.get(k), str)
                        for k in ("source", "target", "relation")
                    ):
                        edges.add((edge["source"], edge["target"], edge["relation"]))
            edges.update(relationships(child))
    elif isinstance(value, list):
        for child in value:
            edges.update(relationships(child))
    return edges


@dataclass
class Document:
    id: str
    row_id: int
    category: str
    entity: str
    tool: str
    evidence_kind: str
    text: str
    content_hash: str
    segment: int
    segments: int
    sequence: int


def rank(documents, queries, vector_search, edges, graph=True, per_query=20):
    scores, reasons = defaultdict(float), defaultdict(list)
    eligible_union = set()
    for query_id, query in queries.items():
        eligible = [
            d
            for d in documents.values()
            if (not query.entity or query.entity == d.entity)
            and (not query.tool or query.tool == d.tool)
            and (not query.evidence_kind or query.evidence_kind == d.evidence_kind)
        ]
        ids = {d.id for d in eligible}
        eligible_union.update(ids)
        semantic = [
            (key, score) for key, score in vector_search(query_id, ids) if key in ids
        ][:per_query]
        qterms = terms(query.text)
        lexical = sorted(
            ((d.id, len(qterms & terms(d.text))) for d in eligible),
            key=lambda x: (-x[1], x[0]),
        )
        lexical = [(key, score) for key, score in lexical if score][:per_query]
        for method, hits in (("vector", semantic), ("lexical", lexical)):
            for position, (key, similarity) in enumerate(hits, 1):
                contribution = 1 / (60 + position)
                scores[key] += contribution
                reasons[key].append(
                    {
                        "query": query_id,
                        "method": method,
                        "rank": position,
                        "score": similarity,
                        "contribution": contribution,
                    }
                )
    if graph:
        # Expand once from the original matches; never recursively walk the corpus.
        original = dict(scores)
        by_entity = defaultdict(list)
        for document in documents.values():
            by_entity[document.entity].append(document.id)
        for key, score in original.items():
            entity = documents[key].entity
            for source, target, relation, ref in sorted(edges):
                neighbor = (
                    target if source == entity else source if target == entity else None
                )
                for related in by_entity.get(neighbor, []):
                    if related == key or related not in eligible_union:
                        continue
                    scores[related] += score / 2
                    reasons[related].append(
                        {
                            "method": "graph",
                            "from": key,
                            "relation": relation,
                            "edge_ref": ref,
                            "contribution": score / 2,
                        }
                    )
    ordered = sorted(scores, key=lambda key: (-scores[key], key))
    # First pass diversifies entities/kinds/categories; second pass fills remaining slots.
    seen, diverse, remainder = set(), [], []
    for key in ordered:
        d = documents[key]
        group = (d.entity, d.evidence_kind, d.category)
        if group in seen:
            remainder.append(key)
        else:
            seen.add(group)
            diverse.append(key)
    return diverse + remainder, reasons
