"""Post-termination deterministic normalization. Never imported by agent plugins."""

import re


def score(ground_truth, diagnosis, entities, relationships=()):
    gt = ground_truth.get("spec", ground_truth)
    groups = {}
    for group in gt.get("groups", []):
        key = group["id"]
        if key in groups:
            if any(
                groups[key].get(field) != group.get(field)
                for field in ("kind", "namespace", "name", "filter")
            ):
                raise ValueError("conflicting ground-truth group definitions")
            groups[key]["root_cause"] = (
                groups[key].get("root_cause") is True or group.get("root_cause") is True
            )
        else:
            groups[key] = dict(group)
    patterns, repairs = {}, []
    for key, group in groups.items():
        if not isinstance(group.get("kind"), str) or not group["kind"]:
            raise ValueError("ground-truth kind missing")
        filters = group.get("filter", [])
        if isinstance(filters, str):
            filters = [filters]
        patterns[key] = []
        for pattern in filters:
            try:
                compiled = re.compile(pattern)
            except re.error:
                # The pinned release includes '*.*' for one HPA group. A bare
                # leading wildcard is deterministically repaired to regex '.*'.
                # Other invalid regexes fail preparation before any paid call.
                if not pattern.startswith("*"):
                    raise
                compiled = re.compile("." + pattern)
                repairs.append(
                    {"group": key, "original": pattern, "regex": "." + pattern}
                )
            patterns[key].append(compiled)
        if not group.get("name") and not filters:
            raise ValueError("ground-truth identity missing")
    if not groups or not any(g.get("root_cause") is True for g in groups.values()):
        raise ValueError("ground truth lacks root-cause groups")
    parent = {k: k for k in groups}

    def find(key):
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    for aliases in gt.get("aliases", []):
        if (
            not isinstance(aliases, list)
            or not aliases
            or any(k not in groups for k in aliases)
        ):
            raise ValueError("invalid ground-truth aliases")
        for key in aliases[1:]:
            parent[find(key)] = find(aliases[0])
    roots = {find(k) for k, v in groups.items() if v.get("root_cause") is True}
    observed = set(entities)
    neighbours = {k: set() for k in observed}
    # Only actual owner links establish workload equivalence. Service/Pod
    # equivalence requires explicit ground-truth aliases, not a selector alone.
    # Never use fuzzy suffix matching or infer a mapping from a causal chain.
    for source, target, relation in relationships:
        if source in observed and target in observed and relation == "owner":
            if source.split("/")[1] in {
                "Pod",
                "ReplicaSet",
                "Deployment",
                "StatefulSet",
                "DaemonSet",
            } and target.split("/")[1] in {
                "Pod",
                "ReplicaSet",
                "Deployment",
                "StatefulSet",
                "DaemonSet",
            }:
                neighbours[source].add(target)
                neighbours[target].add(source)

    def matches(name, group):
        namespace, kind, explicit = name.split("/")
        expected_namespace = group.get("namespace") or "cluster"
        chaos_schedule = (
            kind == "Schedule" and namespace == expected_namespace == "chaos-mesh"
        )
        if (
            kind != group.get("kind") and not chaos_schedule
        ) or namespace != expected_namespace:
            return False
        if "name" in group:
            return explicit == group["name"]
        return any(pattern.search(explicit) for pattern in patterns[group["id"]])

    normalized, predictions = [], set()
    for factor in diagnosis.get("contributing_factors", []):
        name = factor.get("name", "")
        matched = set()
        if name in observed and len(name.split("/")) == 3:
            matched = {find(k) for k, g in groups.items() if matches(name, g)}
            if not matched:
                visited, queue = {name}, [name]
                while queue:
                    for other in neighbours[queue.pop()] - visited:
                        visited.add(other)
                        queue.append(other)
                # Ownership can normalize an unlisted Deployment onto an observed
                # Pod group, but only within one unique scoring group.
                matched = {
                    find(k)
                    for other in visited
                    for k, g in groups.items()
                    if matches(other, g)
                }
        key = next(iter(matched)) if len(matched) == 1 else "unmatched:" + name
        predictions.add(key)
        normalized.append(
            {
                "name": name,
                "group": key if len(matched) == 1 else None,
                "status": "matched"
                if len(matched) == 1
                else ("ambiguous" if matched else "unmatched"),
            }
        )
    tp, fp = len(predictions & roots), len(predictions - roots)
    missed = roots - predictions
    return {
        "score": tp / (tp + fp) if not missed and tp + fp else 0.0,
        "exact_root_set": predictions == roots,
        "tp": tp,
        "fp": fp,
        "missed_groups": sorted(missed),
        "normalization": normalized,
        "root_groups": sorted(roots),
        "filter_repairs": repairs,
    }
