"""Lossless catalogue compression using explicit per-tool field defaults."""

import json
from collections import Counter
from os.path import commonprefix


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def description_parts(candidate):
    parts = [candidate["description"]]
    for key in ("entity", "kind", "reason"):
        value = candidate["args"].get(key)
        if not isinstance(value, str) or not value:
            continue
        updated = []
        for part in parts:
            if not isinstance(part, str):
                updated.append(part)
                continue
            pieces = part.split(value)
            for index, piece in enumerate(pieces):
                if index:
                    updated.append({"argument": key})
                if piece:
                    updated.append(piece)
        parts = updated
    return parts


def encode_catalogue(candidates):
    groups = {}
    for candidate in candidates:
        groups.setdefault(candidate["tool"], []).append(candidate)
    defaults, argument_defaults = {}, {}
    for tool, group in groups.items():
        defaults[tool], argument_defaults[tool] = {}, {}
        for target, values in (
            (
                defaults[tool],
                [
                    {
                        k: v
                        for k, v in c.items()
                        if k not in {"id", "tool", "args", "description"}
                    }
                    for c in group
                ],
            ),
            (argument_defaults[tool], [c["args"] for c in group]),
        ):
            keys = set.intersection(*(set(v) for v in values))
            for key in sorted(keys):
                frequencies = Counter(canonical(v[key]) for v in values)
                value, count = frequencies.most_common(1)[0]
                if count > 1:
                    target[key] = json.loads(value)
    prefixes = {}
    for tool, group in groups.items():
        for field, minimum in (("created_at", 8), ("id", 2)):
            if (
                field not in defaults[tool]
                and len(group) > 1
                and all(isinstance(c.get(field), str) for c in group)
            ):
                prefix = commonprefix([c[field] for c in group])
                if len(prefix) > minimum:
                    prefixes.setdefault(tool, {})[field] = prefix
    string_fields, repeated = {}, set()
    for tool, group in groups.items():
        keys = set.intersection(*(set(c["args"]) for c in group))
        for key in sorted(keys):
            if all(isinstance(c["args"][key], str) for c in group):
                counts = Counter(c["args"][key] for c in group)
                values = {
                    value
                    for value, count in counts.items()
                    if count > 1 and len(value) > 16
                }
                if values:
                    string_fields.setdefault(tool, []).append(key)
                    repeated.update(values)
    strings = sorted(repeated)
    string_indices = {value: index for index, value in enumerate(strings)}
    templates = [description_parts(c) for c in candidates]
    frequencies = Counter(canonical(parts) for parts in templates)
    template_keys = sorted(key for key, count in frequencies.items() if count > 1)
    template_indices = {key: index for index, key in enumerate(template_keys)}
    encoded = []
    for candidate, template in zip(candidates, templates, strict=True):
        common, arguments = (
            defaults[candidate["tool"]],
            argument_defaults[candidate["tool"]],
        )
        item = {
            k: v
            for k, v in candidate.items()
            if k not in common or canonical(v) != canonical(common[k])
        }
        item["args"] = {
            k: v
            for k, v in candidate["args"].items()
            if k not in arguments or canonical(v) != canonical(arguments[k])
        }
        for key in string_fields.get(candidate["tool"], []):
            if key in item["args"] and item["args"][key] in string_indices:
                item["args"][key] = string_indices[item["args"][key]]
        for key, prefix in prefixes.get(candidate["tool"], {}).items():
            item[key] = item[key][len(prefix) :]
        template_key = canonical(template)
        if template_key in template_indices:
            item["description"] = template_indices[template_key]
        encoded.append(item)
    return {
        "catalogue_encoding": "per_tool_defaults/1",
        "candidate_defaults": defaults,
        "candidate_argument_defaults": argument_defaults,
        "candidate_text_prefixes": prefixes,
        "candidate_string_fields": string_fields,
        "candidate_string_table": strings,
        "candidate_description_templates": [json.loads(key) for key in template_keys],
        "candidates": encoded,
    }


def decode_catalogue(value):
    if value.get("catalogue_encoding") != "per_tool_defaults/1":
        return value["candidates"]
    result = [
        {
            **value["candidate_defaults"][c["tool"]],
            **c,
            "args": {**value["candidate_argument_defaults"][c["tool"]], **c["args"]},
        }
        for c in value["candidates"]
    ]
    for candidate in result:
        for key in value.get("candidate_string_fields", {}).get(candidate["tool"], []):
            if type(candidate["args"].get(key)) is int:
                candidate["args"][key] = value["candidate_string_table"][
                    candidate["args"][key]
                ]
        for key, prefix in (
            value.get("candidate_text_prefixes", {}).get(candidate["tool"], {}).items()
        ):
            candidate[key] = prefix + candidate[key]
        if type(candidate.get("description")) is int:
            template = value["candidate_description_templates"][
                candidate["description"]
            ]
            candidate["description"] = "".join(
                part if isinstance(part, str) else candidate["args"][part["argument"]]
                for part in template
            )
    return result


def encode_action_history(value):
    history = value.get("action_history", {})
    ids = {c["id"] for c in decode_catalogue(value)}
    if not history or set(history) != ids:
        return value
    default, count = Counter(canonical(v) for v in history.values()).most_common(1)[0]
    if count < 2:
        return value
    value["action_history_default"] = json.loads(default)
    value["action_history"] = {
        key: entry for key, entry in history.items() if canonical(entry) != default
    }
    return value


def decode_decision_input(value):
    result = dict(value)
    result["candidates"] = decode_catalogue(value)
    if "action_history_default" in value:
        result["action_history"] = {
            c["id"]: value["action_history"].get(
                c["id"], value["action_history_default"]
            )
            for c in result["candidates"]
        }
    return result
