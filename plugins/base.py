"""Common registered-candidate and evidence boundaries."""

import hashlib
import json

from dowser.contracts import Component
from dowser.models import ActionCandidate, Observation, ParseResult, VerificationResult

from .common import Inventory, credential_values, sanitized


class PlatformTools(Component):
    platform = ""
    parser_version = "1"

    def __init__(self, settings, context):
        self.settings = settings
        self.inventory = Inventory.load(settings.inventory_path, context.base_dir)
        self.freshness = context.limits.freshness_seconds

    def target(self, candidate, state):
        spec = next(t for t in self.tools if t.name == candidate.tool)
        args = spec.argument_model.model_validate(candidate.args)
        if (
            candidate.plugin_version,
            candidate.effect,
            candidate.kind,
            candidate.verification,
        ) != (
            spec.plugin_version,
            spec.effect,
            spec.kind,
            spec.verification_hooks[0],
        ) or candidate.recovery is not None:
            raise ValueError("candidate differs from registered capability")
        if candidate.resources != [args.resource_id]:
            raise ValueError("arguments differ from affected resources")
        resource = next(r for r in state.resources if r.id == args.resource_id)
        item = self.inventory.resources[args.resource_id]
        if (
            resource.platform != self.platform
            or item.platform != self.platform
            or resource.platform_version != item.platform_version
        ):
            raise ValueError("unsupported platform/version")
        scope = resource.payload.get("scope", {})
        from .normalizers import PlatformNormalizer

        PlatformNormalizer.check_desired(self.platform, state.desired_state, scope)
        keys = (
            ("interfaces", "vlans", "vrfs", "prefixes")
            if self.platform == "nxos"
            else ("models",)
        )
        if not isinstance(scope, dict) or set(scope) - set(keys):
            raise ValueError("unsupported scope")
        for key, values in scope.items():
            if not isinstance(values, list) or not set(values) <= set(
                getattr(item, key)
            ):
                raise ValueError("scope exceeds inventory")
        for arg, key in (
            ("interface", "interfaces"),
            ("vlan", "vlans"),
            ("vrf", "vrfs"),
            ("prefix", "prefixes"),
            ("model", "models"),
        ):
            value = getattr(args, arg, None)
            if value is not None and (
                value not in scope.get(key, []) or value not in getattr(item, key)
            ):
                raise ValueError("argument outside approved incident scope")
        if candidate.effect == "change" and not self.settings.enable_changes:
            raise ValueError("plugin changes disabled")
        return args, item, resource

    def candidate(self, tool, args, evidence=()):
        spec = next(t for t in self.tools if t.name == f"{self.platform}.{tool}")
        digest = hashlib.sha256(json.dumps(args, sort_keys=True).encode()).hexdigest()[
            :24
        ]
        return ActionCandidate(
            id=f"{spec.name}:{digest}",
            tool=spec.name,
            plugin_version=spec.plugin_version,
            args=args,
            description=spec.name.replace("_", " "),
            effect=spec.effect,
            kind=spec.kind,
            resources=[args["resource_id"]],
            required_observation_ids=list(evidence),
            timeout_seconds=360
            if spec.effect == "change" and self.platform == "vllm"
            else 60,
            verification=spec.verification_hooks[0],
        )

    def clean(self, result):
        secrets = credential_values(self.inventory)
        result.raw_output = sanitized(result.raw_output, secrets)
        for outcome in result.steps:
            outcome.raw_output = sanitized(outcome.raw_output, secrets)
        return result

    async def parse(self, candidate, result):
        if result.status != "succeeded":
            return ParseResult(
                status="skipped",
                parser_version=self.parser_version,
                reason="execution did not succeed",
            )
        try:
            raw = result.raw_output
            if (
                not isinstance(raw, dict)
                or set(raw) != {"facts"}
                or not isinstance(raw["facts"], dict)
            ):
                raise ValueError("unrecognized facts envelope")
            from dowser.store import reject_credentials

            reject_credentials(raw)
            return ParseResult(
                status="valid",
                parser_version=self.parser_version,
                observations=[
                    Observation(
                        resource_id=candidate.args["resource_id"],
                        kind=candidate.tool,
                        payload=raw["facts"],
                    )
                ],
            )
        except (ValueError, TypeError):
            return ParseResult(
                status="failed",
                parser_version=self.parser_version,
                reason="invalid sanitized facts",
            )

    async def recover(self, state, candidate, result):
        return []

    @staticmethod
    def verdict(
        result, matched, *, reason="desired state checked using current evidence"
    ):
        observations = result.parse.observations
        if (
            result.status != "succeeded"
            or result.parse.status != "valid"
            or not observations
            or matched is None
        ):
            return VerificationResult(
                status="inconclusive", reason="insufficient incident-specific evidence"
            )
        return VerificationResult(
            status="passed" if matched else "failed",
            reason=reason,
            evidence_refs=[o.id for o in observations],
        )
