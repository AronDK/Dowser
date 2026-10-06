"""Single explicit-platform router and inventory enrichment for JSONL intake."""

import hashlib
import json

from dowser.contracts import Component, factory
from dowser.models import IncidentState, Resource, mapped_value

from .common import Inventory, InventorySettings, credential_values, sanitized


class PlatformNormalizer(Component):
    platform = None

    def __init__(self, settings, context):
        self.settings = settings
        self.inventory = Inventory.load(settings.inventory_path, context.base_dir)

    async def normalize(self, record):
        data = record.payload
        if not isinstance(data, dict):
            raise ValueError("platform incident must be an object")
        platform = data.get("platform")
        if platform not in {"nxos", "vllm"} or (
            self.platform and platform != self.platform
        ):
            raise ValueError("unknown platform")
        if "resources" in data:
            raise ValueError("platform intake uses resource_id and explicit scope")
        alert = data.get("alert", {})
        if not isinstance(alert, dict):
            raise ValueError("alert must be an object")
        identities = [
            data.get("resource_id"),
            alert.get("resource_id"),
            alert.get("device_id"),
        ]
        ids = {i for i in identities if i is not None}
        if len(ids) != 1:
            raise ValueError("one consistent resource identity is required")
        resource_id = ids.pop()
        item = self.inventory.resources[resource_id]
        if (
            item.platform != platform
            or alert.get("platform", platform) != platform
            or (record.metadata or {}).get("platform", platform) != platform
        ):
            raise ValueError("conflicting platform identities")
        if (
            alert.get("platform_version", item.platform_version)
            != item.platform_version
            or data.get("platform_version", item.platform_version)
            != item.platform_version
        ):
            raise ValueError("conflicting version")
        scope = data.get("scope", {})
        allowed_fields = (
            ("interfaces", "vlans", "vrfs", "prefixes")
            if platform == "nxos"
            else ("models",)
        )
        if not isinstance(scope, dict) or set(scope) - set(allowed_fields):
            raise ValueError("unknown scope")
        for key, values in scope.items():
            if not isinstance(values, list) or not set(values) <= set(
                getattr(item, key)
            ):
                raise ValueError("scope exceeds inventory")
        desired = data.get("desired_state", {})
        if not isinstance(desired, dict):
            raise ValueError("desired_state must be an object")
        desired = sanitized(desired, credential_values(self.inventory))
        self.check_desired(platform, desired, scope)
        clean_alert = sanitized(alert, credential_values(self.inventory))
        clean_alert.update(
            platform=platform,
            platform_version=item.platform_version,
            resource_id=resource_id,
        )
        for field in ("severity", "priority"):
            canonical = mapped_value(
                getattr(self.settings, f"{field}_map"), clean_alert.get(field)
            )
            if canonical is not None:
                clean_alert[f"canonical_{field}"] = canonical
        identifier = data.get("incident_id", data.get("event_id", record.event_id))
        if not isinstance(identifier, str) or not identifier:
            raise ValueError("incident identity must be a nonempty string")
        stable = hashlib.sha256(
            json.dumps([record.source_id, identifier], separators=(",", ":")).encode()
        ).hexdigest()
        return IncidentState(
            incident_id=f"{platform}:{stable}",
            alert=clean_alert,
            desired_state=desired,
            resources=[
                Resource(
                    id=resource_id,
                    platform=platform,
                    platform_version=item.platform_version,
                    payload={"scope": scope},
                )
            ],
        )

    @staticmethod
    def check_desired(platform, desired, scope):
        fields = (
            {
                "admin_state",
                "oper_state",
                "access_vlan",
                "vlan_exists",
                "vlan_state",
                "route_present",
                "reachable",
            }
            if platform == "nxos"
            else {"available", "performance"}
        )
        if set(desired) - fields:
            raise ValueError("unsupported desired state")
        if platform == "nxos":
            if (
                set(desired) & {"admin_state", "oper_state", "access_vlan"}
                and len(scope.get("interfaces", [])) != 1
            ):
                raise ValueError(
                    "interface desired state requires one scoped interface"
                )
            if (
                set(desired) & {"vlan_exists", "vlan_state"}
                and len(scope.get("vlans", [])) != 1
            ):
                raise ValueError("VLAN desired state requires one scoped VLAN")
            if "route_present" in desired and (
                len(scope.get("vrfs", [])) != 1 or len(scope.get("prefixes", [])) != 1
            ):
                raise ValueError("route desired state requires one prefix and VRF")
            if "access_vlan" in desired and desired["access_vlan"] not in scope.get(
                "vlans", []
            ):
                raise ValueError("desired VLAN outside incident scope")
            for key in ("admin_state", "oper_state"):
                if key in desired and desired[key] not in ("up", "down"):
                    raise ValueError("unsupported interface state")
            if "access_vlan" in desired and type(desired["access_vlan"]) is not int:
                raise ValueError("expected integer desired VLAN")
            if "vlan_state" in desired and desired["vlan_state"] not in {
                "active",
                "suspend",
            }:
                raise ValueError("unsupported VLAN state")
            for key in ("route_present", "vlan_exists", "reachable"):
                if key in desired and type(desired[key]) is not bool:
                    raise ValueError("expected boolean desired state")
        else:
            if desired and len(scope.get("models", [])) != 1:
                raise ValueError("service desired state requires one scoped model")
            for key in ("available", "performance"):
                if key in desired and type(desired[key]) is not bool:
                    raise ValueError("expected boolean service desired state")


@factory(
    subsystem="normalizer",
    component_type=PlatformNormalizer,
    settings_model=InventorySettings,
)
def platform_normalizer(settings, context):
    return PlatformNormalizer(settings, context)
