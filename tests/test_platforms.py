"""Offline platform acceptance. All remote clients are simulated or socket-blocked."""

import copy
import importlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from dowser.config import Application, FactoryReference, read_config, validate_config
from dowser.contracts import AppContext
from dowser.intake import IntakeRunner, JSONLSource
from dowser.models import (
    DecisionRequest,
    ExecutionResult,
    Limits,
    Observation,
    RawIncident,
    now,
)
from dowser.store import reject_credentials
from plugins import nxos, vllm
from plugins.common import Inventory, InventorySettings, ToolSettings, optional
from plugins.normalizers import PlatformNormalizer

ROOT = Path(__file__).resolve().parents[1]
VERSION = {
    "nxos_ver_str": "10.4(3)",
    "chassis_id": "Nexus9000 C93180YC-EX Chassis",
    "password": "never-store",
}
INTERFACE = {
    "TABLE_interface": {
        "ROW_interface": [
            {
                "interface": "Ethernet1/1",
                "admin_state": "down",
                "state": "down",
                "eth_inerr": "7",
            }
        ]
    }
}
SWITCHPORT = {
    "TABLE_interface": {
        "ROW_interface": [
            {
                "interface": "Ethernet1/1",
                "switchport": "Enabled",
                "oper_mode": "access",
                "access_vlan": "100",
            }
        ]
    }
}
CHANNELS = {"TABLE_channel": {"ROW_channel": []}}
VLAN = {
    "TABLE_vlanbrief": {
        "ROW_vlanbrief": [
            {
                "vlanshowbr-vlanid": "200",
                "vlanshowbr-vlanstate": "active",
                "vlanshowplist-ifidx": "Eth1/1, Eth1/9",
            }
        ]
    }
}
ROUTES = {
    "TABLE_vrf": {
        "ROW_vrf": [
            {
                "vrf-name-out": "default",
                "TABLE_addrf": {
                    "ROW_addrf": [
                        {
                            "addrf": "ipv4",
                            "TABLE_prefix": {
                                "ROW_prefix": [
                                    {"ipprefix": "192.0.2.0/24", "unrestricted": "drop"}
                                ]
                            },
                        }
                    ]
                },
            }
        ]
    }
}


class Response:
    def __init__(self, value=None, status=200, text=""):
        self.value, self.status_code, self.text = value, status, text

    def json(self):
        return copy.deepcopy(self.value)

    def raise_for_status(self):
        if not 200 <= self.status_code < 300:
            raise RuntimeError("simulated HTTP error containing secret")


class FakeNX:
    def __init__(self):
        self.interface = copy.deepcopy(INTERFACE)
        self.switchport = copy.deepcopy(SWITCHPORT)
        self.channels = copy.deepcopy(CHANNELS)
        self.version = copy.deepcopy(VERSION)
        self.vlan = copy.deepcopy(VLAN)
        self.commands, self.writes = [], []
        self.fail_write = None
        self.post_failure = False

    async def show(self, command):
        self.commands.append(command)
        if self.post_failure and self.writes:
            raise RuntimeError("lost post-change read")
        return copy.deepcopy(
            {
                "show version": self.version,
                "show interface Ethernet1/1": self.interface,
                "show interface Ethernet1/1 switchport": self.switchport,
                "show port-channel summary": self.channels,
                "show vlan id 200": self.vlan,
                "show ip route 192.0.2.0/24 vrf default": ROUTES,
            }[command]
        )

    async def write(self, commands, resource, steps):
        self.writes.append(commands)
        if self.fail_write:
            raise self.fail_write
        from plugins.common import step

        for i, command in enumerate(commands):
            steps.append(step(f"command-{i}", resource, now()))
            if command == "no shutdown":
                row = self.interface["TABLE_interface"]["ROW_interface"][0]
                row.update(admin_state="up", state="up")
            if command.startswith("switchport access vlan"):
                self.switchport["TABLE_interface"]["ROW_interface"][0][
                    "access_vlan"
                ] = "200"

    @asynccontextmanager
    async def session(self, device):
        yield self


class FakeHTTP:
    def __init__(self):
        self.calls, self.closed = [], 0
        self.count = 100
        self.decision = None
        self.finish_reason = "stop"
        self.healthy, self.canary = True, True
        self.health_sequence = []
        self.models = ["som"]
        self.runtime = {
            "Id": "abc123",
            "Name": "/dowser-vllm-1",
            "Config": {
                "Labels": {
                    "com.docker.compose.project": "dowser",
                    "com.docker.compose.service": "vllm",
                },
                "Env": ["PASSWORD=never-store"],
            },
            "State": {"Running": False, "Status": "exited", "OOMKilled": True},
            "RestartCount": 2,
        }
        self.outage = False
        self.mutation_status = 204
        self.lost_mutation = False
        self.metric_texts = []

    @asynccontextmanager
    async def client(self, settings, **kwargs):
        try:
            yield self
        finally:
            self.closed += 1

    async def get(self, path):
        self.calls.append(("GET", path, None))
        if self.outage:
            raise ConnectionError("token=secret")
        if path.endswith("/json"):
            return Response(self.runtime)
        if path == "/health":
            healthy = (
                self.health_sequence.pop(0) if self.health_sequence else self.healthy
            )
            return Response(status=200 if healthy else 503)
        if path == "/v1/models":
            return Response({"data": [{"id": v} for v in self.models]})
        if path == "/version":
            return Response({"version": "0.11.0"})
        if path == "/metrics":
            return Response(text=self.metric_texts.pop(0))
        raise AssertionError(path)

    async def post(self, path, **kwargs):
        body = kwargs.get("json")
        self.calls.append(("POST", path, body))
        if self.outage:
            raise ConnectionError("token=secret")
        if path.endswith(("/start", "/restart")):
            if self.lost_mutation:
                raise ConnectionError("lost acknowledgment")
            self.runtime["State"].update(Running=True, Status="running")
            return Response(status=self.mutation_status)
        if path == "/tokenize":
            return Response({"count": self.count})
        if path == "/v1/chat/completions":
            if body.get("response_format"):
                if self.decision is None:
                    request = json.loads(body["messages"][1]["content"])
                    candidates = request["candidates"]
                    chosen = next(
                        (c for c in candidates if c["effect"] == "change"), None
                    ) or next(
                        (
                            c
                            for c in candidates
                            if c["tool"] == "nxos.inspect_interface"
                        ),
                        candidates[0],
                    )
                    content = json.dumps(
                        {
                            "decisions": [
                                {"operation": "select", "candidate_id": chosen["id"]}
                            ]
                        }
                    )
                else:
                    content = (
                        self.decision
                        if isinstance(self.decision, str)
                        else json.dumps(self.decision)
                    )
                return Response(
                    {
                        "choices": [
                            {
                                "finish_reason": self.finish_reason,
                                "message": {"content": content},
                            }
                        ]
                    }
                )
            return Response(
                {
                    "model": body["model"],
                    "choices": [
                        {"finish_reason": "stop", "message": {"content": "OK"}}
                    ],
                },
                status=200 if self.canary else 503,
            )
        raise AssertionError(path)


def metric_text(count=10, seconds=10, queue=1, cache=0.5, model="som", engine="0"):
    label = f'model_name="{model}",engine="{engine}"'
    return "\n".join(
        f"{name}{{{label}}} {value}"
        for name, value in (
            ("vllm:num_requests_waiting", queue),
            ("vllm:kv_cache_usage_perc", cache),
            ("vllm:e2e_request_latency_seconds_count", count),
            ("vllm:e2e_request_latency_seconds_sum", seconds),
        )
    )


class Platforms(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.inventory = json.loads((ROOT / "docs/examples/inventory.json").read_text())
        self.path = self.base / "inventory.json"
        self.write_inventory()
        self.context = AppContext({}, Limits(freshness_seconds=60), self.base)
        self.nx = FakeNX()
        self.http = FakeHTTP()
        self.socket_patch = patch.object(
            socket.socket, "connect", side_effect=AssertionError("network forbidden")
        )
        self.socket_patch.start()

    async def asyncTearDown(self):
        self.socket_patch.stop()
        self.temp.cleanup()

    def write_inventory(self):
        self.path.write_text(json.dumps(self.inventory))

    def plugin(self, *, platform="nxos", changes=False, **kwargs):
        model = ToolSettings if platform == "nxos" else vllm.ServiceSettings
        cls = nxos.NXOSTools if platform == "nxos" else vllm.VLLMTools
        return cls(
            model(inventory_path="inventory.json", enable_changes=changes, **kwargs),
            self.context,
        )

    async def state(self, platform="nxos", desired=None, scope=None):
        if scope is None:
            scope = (
                {
                    "interfaces": ["Ethernet1/1"],
                    "vlans": [200],
                    "vrfs": ["default"],
                    "prefixes": ["192.0.2.0/24"],
                }
                if platform == "nxos"
                else {"models": ["som"]}
            )
        return await PlatformNormalizer(
            InventorySettings(
                inventory_path="inventory.json", severity_map={"major": "high"}
            ),
            self.context,
        ).normalize(
            RawIncident(
                source_id="monitor",
                event_id="one",
                payload={
                    "platform": platform,
                    "resource_id": "nexus-lab"
                    if platform == "nxos"
                    else "inference-lab",
                    "alert": {
                        "severity": "major",
                        "priority": 3,
                        "details": {"native": 7},
                    },
                    "desired_state": desired
                    if desired is not None
                    else {"admin_state": "up"}
                    if platform == "nxos"
                    else {"available": True},
                    "scope": scope,
                },
            )
        )

    def provider(self, capacity=2):
        return vllm.VLLMDecisionProvider(
            vllm.ProviderSettings(
                endpoint="http://127.0.0.1:8000",
                profile={
                    "model": "som",
                    "context_window": 1000,
                    "output_tokens": 200,
                    "inference_timeout": 10,
                    "max_decisions_per_round": capacity,
                },
            )
        )

    async def request(self):
        state = await self.state()
        return DecisionRequest(
            incident_id=state.incident_id,
            state=state,
            candidates=await self.plugin().candidates(state),
        )

    async def executed(self, plugin, candidate, state):
        transport = await plugin.execute(candidate, state)
        parsed = await plugin.parse(candidate, transport)
        return transport, ExecutionResult(
            execution_id="e",
            action_id=candidate.id,
            tool=candidate.tool,
            target=candidate.resources,
            started_at=now(),
            finished_at=now(),
            status=transport.status,
            transport_status=transport.transport_status,
            parse=parsed,
        )

    async def with_evidence(self, platform="nxos", desired=None):
        state = await self.state(platform, desired)
        state.observations.append(
            Observation(
                resource_id=state.resources[0].id,
                kind="nxos.inspect_interface"
                if platform == "nxos"
                else "vllm.inspect_runtime",
                payload={"interface": "Ethernet1/1", "admin_state": "down"}
                if platform == "nxos"
                else {"running": False},
            )
        )
        return state

    async def test_constructor_free_factory_validation(self):
        cfg = read_config(ROOT / "docs/examples/platforms.json")
        with (
            patch.object(
                Inventory, "load", side_effect=AssertionError("constructor reached")
            ),
            patch.object(
                importlib, "import_module", wraps=importlib.import_module
            ) as imports,
        ):
            validate_config(cfg)
        self.assertFalse(
            any(
                call.args[0] in {"httpx", "asyncssh", "prometheus_client.parser"}
                for call in imports.call_args_list
            )
        )

    async def test_all_factories_validate_and_construct_without_network(self):
        from dowser.config import load_factory

        for name in (
            "plugins.nxos:tool_plugin",
            "plugins.nxos:normalizer",
            "plugins.vllm:tool_plugin",
            "plugins.vllm:normalizer",
            "plugins.normalizers:platform_normalizer",
        ):
            slot = "tool_plugin" if name.endswith("tool_plugin") else "normalizer"
            fn, settings = load_factory(
                FactoryReference(
                    factory=name, settings={"inventory_path": "inventory.json"}
                ),
                slot,
            )
            component = fn(settings, self.context)
            await component.aclose()

    async def test_missing_optional_dependencies_have_actionable_error(self):
        with patch.object(importlib, "import_module", side_effect=ImportError):
            for name, extra in (
                ("httpx", "vllm"),
                ("asyncssh", "nxos"),
                ("prometheus_client.parser", "vllm"),
            ):
                with self.assertRaisesRegex(RuntimeError, f"dowser\\[{extra}\\]"):
                    optional(name, extra)

    async def test_private_inventory_rejects_injection_and_unsafe_transports(self):
        for changes in (
            {"interfaces": ["Ethernet1/1;reload"]},
            {"vrfs": ["default;reload"]},
            {"prefixes": ["192.0.2.1/24"]},
            {"transport": "nxapi", "endpoint": "http://nexus"},
            {"endpoint": "https://user:password@nexus"},
            {"transport": "ssh", "endpoint": "nexus", "known_hosts": None},
        ):
            value = copy.deepcopy(self.inventory)
            value["resources"]["nexus-lab"].update(changes)
            with self.assertRaises(ValueError):
                Inventory.model_validate(value)
        with self.assertRaises(ValueError):
            vllm.ProviderSettings(
                endpoint="http://user:pass@host",
                profile=self.provider().settings.profile,
            )
        from plugins.common import DockerTarget

        with self.assertRaises(ValueError):
            DockerTarget(container="safe", unix_socket=None, endpoint="https://docker")

    async def test_normalization_native_fields_scope_and_stable_ids(self):
        a, b = await self.state(), await self.state("vllm")
        self.assertEqual(a.incident_id, (await self.state()).incident_id)
        self.assertNotEqual(a.incident_id, b.incident_id)
        self.assertEqual(a.alert["severity"], "major")
        self.assertEqual(a.alert["canonical_severity"], "high")
        self.assertEqual(a.alert["details"], {"native": 7})
        self.assertEqual(a.phase, "observe")
        for scope in (
            {"interfaces": ["Ethernet1/9"]},
            {"vlans": [999]},
            {"vrfs": ["outside"]},
            {"prefixes": ["0.0.0.0/0"]},
            {"endpoint": ["arbitrary"]},
        ):
            with self.assertRaises(ValueError):
                await self.state(scope=scope)
        with self.assertRaises(ValueError):
            await self.state("vllm", scope={"models": ["other"]})

    async def test_conflicting_platform_and_identity_rejected(self):
        normalizer = PlatformNormalizer(
            InventorySettings(inventory_path="inventory.json"), self.context
        )
        base = {"platform": "nxos", "resource_id": "nexus-lab", "alert": {}}
        for change in (
            {"platform": "unknown"},
            {"resource_id": "inference-lab"},
            {"alert": {"platform": "vllm"}},
            {"alert": {"device_id": "other"}},
            {"alert": {"platform_version": "9"}},
        ):
            with self.assertRaises((ValueError, KeyError)):
                await normalizer.normalize(
                    RawIncident(source_id="s", event_id="e", payload={**base, **change})
                )

    async def test_credentials_prompts_env_logs_are_excluded(self):
        with patch.dict(os.environ, {"DOWSER_NX_PASSWORD": "secret-value"}):
            normalizer = PlatformNormalizer(
                InventorySettings(inventory_path="inventory.json"), self.context
            )
            state = await normalizer.normalize(
                RawIncident(
                    source_id="s",
                    event_id="e",
                    payload={
                        "platform": "nxos",
                        "resource_id": "nexus-lab",
                        "alert": {
                            "password": "secret-value",
                            "details": {
                                "environment": {"TOKEN": "secret-value"},
                                "logs": ["secret-value"],
                                "message": "failure secret-value token=embedded",
                            },
                        },
                    },
                )
            )
            text = state.model_dump_json()
            self.assertNotIn("secret-value", text)
            self.assertNotIn("embedded", text)
            reject_credentials(state.model_dump(mode="json"))

    async def test_scope_and_candidate_capabilities_rechecked(self):
        state, plugin = await self.state(), self.plugin()
        candidate = plugin.candidate(
            "inspect_interface",
            {"resource_id": "nexus-lab", "interface": "Ethernet1/9"},
        )
        self.assertFalse((await plugin.validate(candidate, state)).allowed)
        for field, value in (
            ("resources", ["other"]),
            ("effect", "change"),
            ("plugin_version", "9"),
        ):
            candidate = plugin.candidate(
                "inspect_device", {"resource_id": "nexus-lab"}
            ).model_copy(update={field: value})
            self.assertFalse((await plugin.validate(candidate, state)).allowed)

    async def test_nxapi_ssh_observation_equivalence(self):
        class APIHTTP:
            async def post(inner, path, **kwargs):
                command = kwargs["json"]["ins_api"]["input"]
                self.assertEqual(kwargs["json"]["ins_api"]["type"], "cli_show_array")
                body = await self.nx.show(command)
                return Response(
                    {
                        "ins_api": {
                            "outputs": {
                                "output": [
                                    {"input": command, "code": "200", "body": body}
                                ]
                            }
                        }
                    }
                )

        class SSHConnection:
            async def run(inner, command, **kwargs):
                self.assertTrue(command.endswith(" | json"))
                return SimpleNamespace(
                    exit_status=0, stdout=json.dumps(await self.nx.show(command[:-7]))
                )

        device = Inventory.model_validate(self.inventory).resources["nexus-lab"]
        with patch.dict(os.environ, {"DOWSER_NX_USER": "u", "DOWSER_NX_PASSWORD": "p"}):
            api, ssh = nxos.NXAPI(APIHTTP(), device), nxos.SSH(SSHConnection(), device)
            args = nxos.InterfaceArgs(resource_id="nexus-lab", interface="Ethernet1/1")
            plugin = self.plugin()
            self.assertEqual(
                await plugin.read_interface(api, args),
                await plugin.read_interface(ssh, args),
            )
            self.assertEqual(
                nxos.device_facts(await api.show("show version")),
                nxos.device_facts(await ssh.show("show version")),
            )

    async def test_nxos_reads_selected_facts(self):
        plugin, state = self.plugin(), await self.state(desired={})
        with patch.object(nxos, "session", self.nx.session):
            for tool, args in (
                ("inspect_device", {}),
                ("inspect_interface", {"interface": "Ethernet1/1"}),
                ("inspect_vlan", {"vlan": 200}),
                ("inspect_routes", {"vrf": "default", "prefix": "192.0.2.0/24"}),
            ):
                c = plugin.candidate(tool, {"resource_id": "nexus-lab", **args})
                transport, result = await self.executed(plugin, c, state)
                self.assertEqual(transport.status, "succeeded")
                self.assertEqual(result.parse.status, "valid")
                self.assertNotIn("never-store", transport.model_dump_json())
                self.assertNotIn("unrestricted", transport.model_dump_json())
                self.assertEqual(
                    (await plugin.verify(state, c, result)).status, "inconclusive"
                )

    async def test_unsupported_versions_and_shapes_fail_closed(self):
        plugin, state = self.plugin(), await self.state()
        c = plugin.candidate("inspect_device", {"resource_id": "nexus-lab"})
        with patch.object(nxos, "session", self.nx.session):
            for version in (
                {"nxos_ver_str": "9.3(8)", "chassis_id": "Nexus9000"},
                {},
                {"nxos_ver_str": "10.4(3)", "chassis_id": "Nexus7000"},
            ):
                self.nx.version = version
                self.assertEqual((await plugin.execute(c, state)).status, "failed")
        with self.assertRaises(ValueError):
            nxos.nxapi_outputs(
                {"ins_api": {"outputs": {"output": []}}}, ["show version"]
            )
        with self.assertRaises(ValueError):
            nxos.nxapi_outputs(
                {
                    "ins_api": {
                        "outputs": {"output": [{"input": "reload", "code": "200"}]}
                    }
                },
                ["show version"],
            )

    async def test_nxos_admin_fix_and_current_verification(self):
        state, plugin = await self.with_evidence(), self.plugin(changes=True)
        c = plugin.candidate(
            "ensure_interface_enabled",
            {"resource_id": "nexus-lab", "interface": "Ethernet1/1"},
            [state.observations[0].id],
        )
        with patch.object(nxos, "session", self.nx.session):
            self.assertTrue((await plugin.validate(c, state)).allowed)
            transport, result = await self.executed(plugin, c, state)
        self.assertEqual(transport.status, "succeeded")
        self.assertEqual((await plugin.verify(state, c, result)).status, "passed")
        self.assertEqual(len(self.nx.writes), 1)
        self.assertEqual(self.nx.writes[0], ["interface Ethernet1/1", "no shutdown"])

    async def test_access_vlan_preconditions_and_fix(self):
        plugin = self.plugin(changes=True)
        state = await self.with_evidence(desired={"access_vlan": 200})
        c = plugin.candidate(
            "ensure_access_vlan",
            {"resource_id": "nexus-lab", "interface": "Ethernet1/1", "vlan": 200},
        )
        with patch.object(nxos, "session", self.nx.session):
            for mode in ("trunk", "routed"):
                self.nx.switchport["TABLE_interface"]["ROW_interface"][0][
                    "oper_mode"
                ] = mode
                self.assertFalse((await plugin.validate(c, state)).allowed)
            self.nx.switchport["TABLE_interface"]["ROW_interface"][0]["oper_mode"] = (
                "access"
            )
            self.nx.channels = {
                "TABLE_channel": {
                    "ROW_channel": [
                        {"TABLE_member": {"ROW_member": [{"port": "Eth1/1"}]}}
                    ]
                }
            }
            self.assertFalse((await plugin.validate(c, state)).allowed)
            self.nx.channels = CHANNELS
            self.nx.vlan = {"TABLE_vlanbrief": {"ROW_vlanbrief": []}}
            self.assertFalse((await plugin.validate(c, state)).allowed)
            self.nx.vlan = VLAN
            transport, result = await self.executed(plugin, c, state)
            self.assertEqual(transport.status, "succeeded")
            self.assertEqual((await plugin.verify(state, c, result)).status, "passed")

    async def test_protected_interface_and_changed_preconditions_refuse_writes(self):
        self.inventory["resources"]["nexus-lab"]["protected_interfaces"] = [
            "Ethernet1/1"
        ]
        self.write_inventory()
        plugin, state = self.plugin(changes=True), await self.with_evidence()
        c = plugin.candidate(
            "ensure_interface_enabled",
            {"resource_id": "nexus-lab", "interface": "Ethernet1/1"},
        )
        with patch.object(nxos, "session", self.nx.session):
            self.assertFalse((await plugin.validate(c, state)).allowed)
            self.assertEqual((await plugin.execute(c, state)).status, "failed")
        self.assertEqual(self.nx.writes, [])

    async def test_partial_write_and_lost_ack_no_replay(self):
        state, plugin = await self.with_evidence(), self.plugin(changes=True)
        c = plugin.candidate(
            "ensure_interface_enabled",
            {"resource_id": "nexus-lab", "interface": "Ethernet1/1"},
        )
        with patch.object(nxos, "session", self.nx.session):
            for error, status in (
                (nxos.CommandRejected("rejected"), "partial"),
                (ConnectionError("lost"), "unknown"),
            ):
                self.nx.fail_write = error
                transport = await plugin.execute(c, state)
                self.assertEqual(transport.status, status)
                self.assertEqual((await plugin.parse(c, transport)).observations, [])
                self.assertEqual(len(self.nx.writes), 1)
                self.nx.writes.clear()

    async def test_nxapi_records_each_configuration_ack(self):
        class WriteHTTP:
            async def post(inner, path, **kwargs):
                commands = kwargs["json"]["ins_api"]["input"].split(" ; ")
                return Response(
                    {
                        "ins_api": {
                            "outputs": {
                                "output": [
                                    {"input": c, "code": "200" if i == 0 else "400"}
                                    for i, c in enumerate(commands)
                                ]
                            }
                        }
                    }
                )

        device = Inventory.model_validate(self.inventory).resources["nexus-lab"]
        steps = []
        with patch.dict(os.environ, {"DOWSER_NX_USER": "u", "DOWSER_NX_PASSWORD": "p"}):
            with self.assertRaises(nxos.CommandRejected):
                await nxos.NXAPI(WriteHTTP(), device).write(
                    ["interface Ethernet1/1", "no shutdown"], "nexus-lab", steps
                )
        self.assertEqual([s.status for s in steps], ["succeeded", "failed"])

    async def test_decision_capacities_and_exact_context_messages(self):
        request = await self.request()
        for capacity in (1, 2, 32, 64):
            provider = self.provider(capacity)
            self.assertEqual(
                (await provider.capabilities()).max_decisions_per_round, capacity
            )
            self.http.decision = {
                "decisions": [
                    {"operation": "select", "candidate_id": request.candidates[0].id}
                ]
                * capacity
            }
            with patch.object(vllm, "http_client", self.http.client):
                check = await provider.check_context(request)
                batch = await provider.decide(request)
            self.assertEqual(len(batch.decisions), capacity)
            self.assertTrue(check.fits)
            token_body, inference_body = self.http.calls[-2][2], self.http.calls[-1][2]
            self.assertEqual(token_body["messages"], inference_body["messages"])
            self.assertTrue(token_body["add_generation_prompt"])
            self.assertEqual(inference_body["max_tokens"], 200)
            self.assertFalse(inference_body["stream"])
            schema = inference_body["response_format"]["json_schema"]["schema"]
            self.assertEqual(schema["properties"]["decisions"]["maxItems"], capacity)
            self.assertEqual(
                schema["$defs"]["DecisionResult"]["properties"]["candidate_id"][
                    "anyOf"
                ][0]["enum"],
                [c.id for c in request.candidates],
            )

    async def test_context_boundary_and_no_inference(self):
        request, provider = await self.request(), self.provider()
        with patch.object(vllm, "http_client", self.http.client):
            self.http.count = 800
            self.assertTrue((await provider.check_context(request)).fits)
            self.http.count = 801
            self.assertFalse((await provider.check_context(request)).fits)
        self.assertTrue(all(call[1] == "/tokenize" for call in self.http.calls))

    async def test_invalid_truncated_unknown_over_capacity_decisions(self):
        request, provider = await self.request(), self.provider(1)
        invalid = [
            "not json",
            '{"decisions":[],"decisions":[]}',
            {"decisions": []},
            {"decisions": [{"operation": "select", "candidate_id": "unknown"}]},
            {"decisions": [{"operation": "wait", "wait_seconds": 0}]},
            {"decisions": [{"operation": "escalate"}, {"operation": "escalate"}]},
            {
                "decisions": [
                    {"operation": "select", "candidate_id": request.candidates[0].id}
                ]
                * 2
            },
        ]
        with patch.object(vllm, "http_client", self.http.client):
            for value in invalid:
                self.http.decision = value
                with self.assertRaises(ValueError):
                    await provider.decide(request)
            self.http.decision = {"decisions": [{"operation": "escalate"}]}
            for reason in ("length", "content_filter", None):
                self.http.finish_reason = reason
                with self.assertRaises(ValueError):
                    await provider.decide(request)

    async def test_service_runtime_oom_sanitized(self):
        plugin, state = self.plugin(platform="vllm"), await self.state("vllm")
        with patch.object(vllm, "docker_client", self.http.client):
            c = plugin.candidate("inspect_runtime", {"resource_id": "inference-lab"})
            transport = await plugin.execute(c, state)
        self.assertTrue(transport.raw_output["facts"]["oom_killed"])
        self.assertNotIn("Env", transport.model_dump_json())
        self.assertNotIn("PASSWORD", transport.model_dump_json())

    async def test_docker_identity_and_compose_ownership(self):
        plugin, state = self.plugin(platform="vllm"), await self.state("vllm")
        c = plugin.candidate("inspect_runtime", {"resource_id": "inference-lab"})
        original = copy.deepcopy(self.http.runtime)
        with patch.object(vllm, "docker_client", self.http.client):
            for key, value in (("Name", "/wrong"), ("Id", "wrong")):
                self.http.runtime = copy.deepcopy(original)
                self.http.runtime["Name"], self.http.runtime["Id"] = "/wrong", "wrong"
                self.http.runtime[key] = value
                self.assertEqual((await plugin.execute(c, state)).status, "failed")
            self.http.runtime = copy.deepcopy(original)
            self.http.runtime["Config"]["Labels"]["com.docker.compose.service"] = (
                "wrong"
            )
            self.assertEqual((await plugin.execute(c, state)).status, "failed")

    async def test_start_delayed_readiness_and_verification(self):
        plugin = self.plugin(
            platform="vllm", changes=True, startup_seconds=0.5, readiness_interval=0.001
        )
        state = await self.with_evidence("vllm")
        c = plugin.candidate(
            "ensure_running", {"resource_id": "inference-lab", "model": "som"}
        )
        self.http.health_sequence = [False, True, False, True, True, True]
        with (
            patch.object(vllm, "docker_client", self.http.client),
            patch.object(vllm, "http_client", self.http.client),
        ):
            self.assertTrue((await plugin.validate(c, state)).allowed)
            transport, result = await self.executed(plugin, c, state)
        self.assertEqual(transport.status, "succeeded")
        self.assertEqual((await plugin.verify(state, c, result)).status, "passed")
        readiness = [s for s in transport.steps if s.step_id.startswith("readiness-")]
        self.assertEqual(len(readiness), 6)
        self.assertEqual(transport.raw_output["facts"]["readiness_checks"], 3)
        mutations = [
            c for c in self.http.calls if c[0] == "POST" and c[1].endswith("/start")
        ]
        self.assertEqual(mutations[0][1], "/v1.51/containers/abc123/start")
        self.assertNotIn("Reply with OK", transport.model_dump_json())

    async def test_mutation_preconditions_freshness_and_restart(self):
        plugin = self.plugin(
            platform="vllm", changes=True, startup_seconds=0.2, readiness_interval=0.001
        )
        state = await self.with_evidence("vllm")
        args = {"resource_id": "inference-lab", "model": "som"}
        start, restart = (
            plugin.candidate("ensure_running", args),
            plugin.candidate("restart_service", args),
        )
        with (
            patch.object(vllm, "docker_client", self.http.client),
            patch.object(vllm, "http_client", self.http.client),
        ):
            self.assertFalse((await plugin.validate(restart, state)).allowed)
            self.http.runtime["State"].update(Running=True, Status="running")
            self.assertFalse((await plugin.validate(start, state)).allowed)
            self.assertTrue((await plugin.validate(restart, state)).allowed)
            self.assertEqual((await plugin.execute(restart, state)).status, "succeeded")
            state.observations = []
            self.assertFalse((await plugin.validate(restart, state)).allowed)

    async def test_expected_model_canary_and_readiness_failure_after_mutation(self):
        plugin = self.plugin(
            platform="vllm",
            changes=True,
            startup_seconds=0.015,
            readiness_interval=0.001,
        )
        state = await self.with_evidence("vllm")
        c = plugin.candidate(
            "ensure_running", {"resource_id": "inference-lab", "model": "som"}
        )
        with (
            patch.object(vllm, "docker_client", self.http.client),
            patch.object(vllm, "http_client", self.http.client),
        ):
            for change in ("model", "canary", "health"):
                self.http.runtime["State"].update(Running=False, Status="exited")
                self.http.models = ["other"] if change == "model" else ["som"]
                self.http.canary = change != "canary"
                self.http.healthy = change != "health"
                transport, result = await self.executed(plugin, c, state)
                self.assertEqual(transport.status, "partial")
                self.assertEqual(
                    (await plugin.verify(state, c, result)).status, "inconclusive"
                )

    async def test_docker_lost_ack_never_retries(self):
        plugin, state = (
            self.plugin(platform="vllm", changes=True),
            await self.with_evidence("vllm"),
        )
        c = plugin.candidate(
            "ensure_running", {"resource_id": "inference-lab", "model": "som"}
        )
        self.http.lost_mutation = True
        with patch.object(vllm, "docker_client", self.http.client):
            transport = await plugin.execute(c, state)
        self.assertEqual(transport.status, "unknown")
        self.assertEqual(len([c for c in self.http.calls if c[0] == "POST"]), 1)

    async def test_health_alone_cannot_verify_availability(self):
        plugin, state = self.plugin(platform="vllm"), await self.state("vllm")
        with patch.object(vllm, "http_client", self.http.client):
            c = plugin.candidate("inspect_service", {"resource_id": "inference-lab"})
            transport, result = await self.executed(plugin, c, state)
            self.assertEqual(
                (await plugin.verify(state, c, result)).status, "inconclusive"
            )
            c = plugin.candidate(
                "probe_inference", {"resource_id": "inference-lab", "model": "som"}
            )
            transport, result = await self.executed(plugin, c, state)
            self.assertEqual((await plugin.verify(state, c, result)).status, "passed")

    async def test_metrics_window_thresholds_and_counter_resets(self):
        plugin = self.plugin(
            platform="vllm",
            measurement_seconds=0.008,
            sample_interval=0.004,
            metrics_thresholds={"queue_max": 2, "cache_max": 0.9, "latency_mean": 2},
        )
        state = await self.state("vllm", {"performance": True})
        c = plugin.candidate(
            "sample_metrics", {"resource_id": "inference-lab", "model": "som"}
        )
        samples = [
            vllm.metric_sample(metric_text(10, 10), "som"),
            vllm.metric_sample(metric_text(11, 11), "som"),
            vllm.metric_sample(metric_text(12, 12), "som"),
        ]
        with (
            patch.object(vllm, "http_client", self.http.client),
            patch.object(vllm, "metric_sample", side_effect=samples),
        ):
            self.http.metric_texts = ["simulated"] * 10
            transport, result = await self.executed(plugin, c, state)
        self.assertEqual(transport.status, "succeeded")
        self.assertEqual((await plugin.verify(state, c, result)).status, "passed")
        facts = result.parse.observations[0].payload
        self.assertEqual(facts["request_count"], 2)
        self.assertGreaterEqual(facts["window_seconds"], 0.008)
        self.assertIn("window_started_at", facts)
        facts["queue_max"] = 10
        self.assertEqual((await plugin.verify(state, c, result)).status, "failed")
        facts["sample_count"] = 1
        self.assertEqual((await plugin.verify(state, c, result)).status, "inconclusive")
        reset = vllm.summarize_metrics(
            [samples[0], vllm.metric_sample(metric_text(1, 1), "som")],
            "som",
            10,
            "s",
            "f",
        )
        self.assertTrue(reset["counter_reset"])
        self.assertFalse(reset["complete"])

    async def test_missing_metrics_zero_requests_changed_series_and_model_scope(self):
        sample = vllm.metric_sample(
            metric_text() + "\n" + metric_text(999, 999, model="private"), "som"
        )
        self.assertEqual(len(sample), 4)
        for samples in (
            [{}, {}],
            [sample, sample],
            [sample, vllm.metric_sample(metric_text(engine="1"), "som")],
        ):
            summary = vllm.summarize_metrics(samples, "som", 10, "s", "f")
            self.assertTrue(not summary["complete"] or summary["latency_mean"] is None)
        with self.assertRaises(ValueError):
            vllm.metric_sample(metric_text(cache=float("nan")), "som")

    async def test_default_changes_disabled(self):
        for platform in ("nxos", "vllm"):
            plugin, state = (
                self.plugin(platform=platform),
                await self.with_evidence(platform),
            )
            self.assertFalse(
                any(c.effect == "change" for c in await plugin.candidates(state))
            )

    async def test_harness_ordered_diagnosis_remediation_and_durable_terminal(self):
        cfg = read_config(ROOT / "docs/examples/platforms.json")
        cfg.event_store.settings = {"path": str(self.base / "history.sqlite3")}
        cfg.tool_registry.settings["plugins"] = [
            {
                "factory": "plugins.nxos:tool_plugin",
                "settings": {
                    "inventory_path": "inventory.json",
                    "enable_changes": True,
                },
            }
        ]
        cfg.limits.changes = 1
        cfg.validation_policy.settings = {"allow_changes": True}
        with (
            patch.object(nxos, "session", self.nx.session),
            patch.object(vllm, "http_client", self.http.client),
        ):
            async with Application(cfg, self.base, requested=("incident_loop",)) as app:
                state = await self.state()
                terminal = await app.services["incident_loop"].run(state)
                history = await app.services["event_store"].inspect(state.incident_id)
        self.assertEqual(terminal.outcome, "resolved")
        self.assertEqual(len(self.nx.writes), 1)
        self.assertEqual(history["events"][-1]["kind"], "terminated")
        self.assertNotIn("never-store", json.dumps(history))
        executed = [
            e["payload"]["tool"]
            for e in history["events"]
            if e["kind"] == "execution_result"
        ]
        self.assertEqual(
            executed, ["nxos.inspect_interface", "nxos.ensure_interface_enabled"]
        )

    async def test_som_outage_shared_endpoint_escalates_without_mutation(self):
        cfg = read_config(ROOT / "docs/examples/platforms.json")
        cfg.event_store.settings = {"path": str(self.base / "history.sqlite3")}
        cfg.tool_registry.settings["plugins"] = [
            {
                "factory": "plugins.vllm:tool_plugin",
                "settings": {
                    "inventory_path": "inventory.json",
                    "enable_changes": True,
                },
            }
        ]
        cfg.limits.changes = 1
        cfg.validation_policy.settings = {"allow_changes": True}
        self.http.outage = True
        with (
            patch.object(vllm, "http_client", self.http.client),
            patch.object(vllm, "docker_client", self.http.client),
        ):
            async with Application(cfg, self.base, requested=("incident_loop",)) as app:
                terminal = await app.services["incident_loop"].run(
                    await self.with_evidence("vllm")
                )
                history = await app.services["event_store"].history(
                    terminal.incident_id
                )
        self.assertEqual(terminal.outcome, "escalated")
        self.assertFalse(
            any(call[1].endswith(("/start", "/restart")) for call in self.http.calls)
        )
        self.assertFalse(any(e.kind == "execution_result" for e in history))
        self.assertTrue(any(e.kind == "terminated" for e in history))

    async def test_bundled_mixed_jsonl_intake_and_duplicate_delivery(self):
        source = JSONLSource(ROOT / "docs/examples/incidents.jsonl", "platform-tests")
        iterator = await source.open()
        normalizer = PlatformNormalizer(
            InventorySettings(inventory_path="inventory.json"), self.context
        )
        states = [await normalizer.normalize(record) async for record in iterator]
        await source.aclose()
        self.assertEqual([s.resources[0].platform for s in states], ["nxos", "vllm"])
        cfg = read_config(ROOT / "docs/examples/platforms.json")
        cfg.event_store.settings = {"path": str(self.base / "history.sqlite3")}
        cfg.normalizer.settings = {"inventory_path": "inventory.json"}
        cfg.tool_registry.settings["plugins"] = [
            {
                "factory": "plugins.vllm:tool_plugin",
                "settings": {"inventory_path": "inventory.json"},
            }
        ]
        cfg.incident_source.settings = {
            "path": str(ROOT / "docs/examples/incidents.jsonl")
        }
        self.http.outage = True
        with patch.object(vllm, "http_client", self.http.client):
            async with Application(cfg, self.base) as app:
                results = []
                self.assertTrue(
                    await IntakeRunner(
                        app.services,
                        cfg.intake,
                        on_result=results.append,
                        on_diagnostic=lambda d: None,
                    ).run()
                )
                self.assertEqual(len(results), 2)
                count = len(self.http.calls)
            async with Application(cfg, self.base) as app:
                self.assertTrue(
                    await IntakeRunner(
                        app.services,
                        cfg.intake,
                        on_result=lambda r: None,
                        on_diagnostic=lambda d: None,
                    ).run()
                )
                self.assertEqual(len(self.http.calls), count)

    async def test_ssh_write_command_acknowledgments_and_cleanup(self):
        from plugins.common import NXDevice

        device = NXDevice(
            transport="ssh",
            endpoint="nexus",
            known_hosts="known_hosts",
            username_env="NX_USER",
            password_env="NX_PASSWORD",
        )
        commands, closed = [], []

        class Process:
            def __init__(inner):
                inner.stdin = SimpleNamespace(
                    write=lambda value: commands.append(value.strip())
                )
                inner.stdout = SimpleNamespace(readuntil=inner.readuntil)

            async def readuntil(inner, pattern):
                return "switch(config-if)# "

            async def __aenter__(inner):
                return inner

            async def __aexit__(inner, *args):
                closed.append(True)

        connection = SimpleNamespace(create_process=lambda **kwargs: Process())
        steps = []
        await nxos.SSH(connection, device).write(
            ["interface Ethernet1/1", "no shutdown"], "nexus-lab", steps
        )
        self.assertEqual(
            commands,
            [
                "configure terminal",
                "interface Ethernet1/1",
                "no shutdown",
                "end",
                "exit",
            ],
        )
        self.assertEqual(len(steps), 4)
        self.assertTrue(all(s.status == "succeeded" for s in steps))
        self.assertEqual(closed, [True])

    async def test_ssh_uses_pinned_hosts_and_per_call_connection_ownership(self):
        import asyncio

        from plugins.common import NXDevice

        opened, closed = [], []
        device = NXDevice(
            transport="ssh",
            endpoint="nexus",
            known_hosts="pinned_hosts",
            username_env="NX_USER",
            password_env="NX_PASSWORD",
        )

        class Connection:
            async def __aenter__(inner):
                opened.append(id(asyncio.get_running_loop()))
                return inner

            async def __aexit__(inner, *args):
                closed.append(True)

        def connect(host, **kwargs):
            self.assertEqual(kwargs["known_hosts"], "pinned_hosts")
            self.assertEqual(host, "nexus")
            return Connection()

        async def operation():
            async with nxos.session(device):
                pass

        from dowser.runtime import bounded_call

        with (
            patch.object(
                nxos, "optional", return_value=SimpleNamespace(connect=connect)
            ),
            patch.dict(os.environ, {"NX_USER": "user", "NX_PASSWORD": "secret"}),
        ):
            await bounded_call(operation, seconds=2)
            await bounded_call(operation, seconds=2)
        self.assertEqual(len(opened), 2)
        self.assertEqual(closed, [True, True])

    async def test_http_clients_created_and_closed_in_each_worker_loop(self):
        import asyncio

        import httpx

        from dowser.runtime import bounded_call
        from plugins.common import HTTPSettings, http_client

        opened, closed = [], []
        original = httpx.AsyncClient

        class Client(original):
            async def __aenter__(inner):
                opened.append(asyncio.get_running_loop())
                return await super().__aenter__()

            async def __aexit__(inner, *args):
                await super().__aexit__(*args)
                closed.append(inner.is_closed)

        async def operation():
            async with http_client(
                HTTPSettings(endpoint="http://127.0.0.1:8000")
            ) as client:
                response = await client.get("/health")
                self.assertEqual(response.status_code, 200)

        with (
            patch.object(httpx, "AsyncClient", Client),
            patch.object(
                httpx,
                "AsyncHTTPTransport",
                side_effect=lambda **kwargs: httpx.MockTransport(
                    lambda request: httpx.Response(200)
                ),
            ),
        ):
            await bounded_call(operation, seconds=2)
            await bounded_call(operation, seconds=2)
        self.assertEqual(closed, [True, True])
        self.assertIsNot(opened[0], opened[1])

    async def test_post_change_read_failure_is_partial_with_command_evidence(self):
        state, plugin = await self.with_evidence(), self.plugin(changes=True)
        candidate = plugin.candidate(
            "ensure_interface_enabled",
            {"resource_id": "nexus-lab", "interface": "Ethernet1/1"},
        )
        self.nx.post_failure = True
        with patch.object(nxos, "session", self.nx.session):
            result = await plugin.execute(candidate, state)
        self.assertEqual(result.status, "partial")
        self.assertEqual(
            [s.status for s in result.steps if s.step_id.startswith("command-")],
            ["succeeded", "succeeded"],
        )

    async def test_live_preconditions_change_between_validate_and_execute(self):
        state, plugin = await self.with_evidence(), self.plugin(changes=True)
        candidate = plugin.candidate(
            "ensure_interface_enabled",
            {"resource_id": "nexus-lab", "interface": "Ethernet1/1"},
        )
        with patch.object(nxos, "session", self.nx.session):
            self.assertTrue((await plugin.validate(candidate, state)).allowed)
            self.nx.interface["TABLE_interface"]["ROW_interface"][0]["admin_state"] = (
                "up"
            )
            self.assertEqual((await plugin.execute(candidate, state)).status, "failed")
        self.assertEqual(self.nx.writes, [])

    async def test_unreachable_managed_service_supplies_sanitized_failure_evidence(
        self,
    ):
        plugin, state = (
            self.plugin(platform="vllm", changes=True),
            await self.state("vllm"),
        )
        self.http.outage = True
        with patch.object(vllm, "http_client", self.http.client):
            c = plugin.candidate("inspect_service", {"resource_id": "inference-lab"})
            transport, result = await self.executed(plugin, c, state)
        self.assertEqual(transport.status, "succeeded")
        self.assertFalse(result.parse.observations[0].payload["healthy"])
        self.assertNotIn("secret", transport.model_dump_json())
        state.observations.extend(result.parse.observations)
        self.assertTrue(
            any(
                c.tool == "vllm.restart_service" for c in await plugin.candidates(state)
            )
        )

    async def test_fresh_supporting_evidence_is_required_for_restart(self):
        from datetime import timedelta

        plugin, state = (
            self.plugin(platform="vllm", changes=True),
            await self.with_evidence("vllm"),
        )
        state.observations[0].observed_at = now() - timedelta(seconds=61)
        self.http.runtime["State"].update(Running=True, Status="running")
        c = plugin.candidate(
            "restart_service", {"resource_id": "inference-lab", "model": "som"}
        )
        with patch.object(vllm, "docker_client", self.http.client):
            self.assertFalse((await plugin.validate(c, state)).allowed)
            self.assertEqual((await plugin.execute(c, state)).status, "failed")
        self.assertFalse(any(call[0] == "POST" for call in self.http.calls))

    async def test_performance_minimum_window_requests_and_series_are_required(self):
        plugin = self.plugin(
            platform="vllm",
            metrics_thresholds={"queue_max": 2, "cache_max": 0.9, "latency_mean": 2},
        )
        state = await self.state("vllm", {"performance": True})
        c = plugin.candidate(
            "sample_metrics", {"resource_id": "inference-lab", "model": "som"}
        )
        facts = {
            "model": "som",
            "complete": True,
            "window_seconds": 10,
            "sample_count": 3,
            "request_count": 5,
            "queue_max": 1,
            "cache_max": 0.5,
            "latency_mean": 1,
        }
        from dowser.models import TransportResult

        for update in (
            {"complete": False},
            {"window_seconds": 9},
            {"request_count": 0},
            {"latency_mean": None},
        ):
            transport = TransportResult(
                status="succeeded", raw_output={"facts": {**facts, **update}}
            )
            parsed = await plugin.parse(c, transport)
            result = ExecutionResult(
                execution_id="e",
                action_id=c.id,
                tool=c.tool,
                target=c.resources,
                started_at=now(),
                finished_at=now(),
                status="succeeded",
                parse=parsed,
            )
            self.assertEqual(
                (await plugin.verify(state, c, result)).status, "inconclusive"
            )

    async def test_provider_outage_during_inference_has_no_tool_execution(self):
        cfg = read_config(ROOT / "docs/examples/platforms.json")
        cfg.event_store.settings = {"path": str(self.base / "history.sqlite3")}
        cfg.tool_registry.settings["plugins"] = [
            {
                "factory": "plugins.nxos:tool_plugin",
                "settings": {"inventory_path": "inventory.json"},
            }
        ]
        original = self.http.post

        async def post(path, **kwargs):
            if path == "/v1/chat/completions":
                raise ConnectionError("failed SOM")
            return await original(path, **kwargs)

        with (
            patch.object(vllm, "http_client", self.http.client),
            patch.object(self.http, "post", post),
            patch.object(nxos, "session", self.nx.session),
        ):
            async with Application(cfg, self.base, requested=("incident_loop",)) as app:
                result = await app.services["incident_loop"].run(await self.state())
        self.assertEqual(result.outcome, "escalated")
        self.assertEqual(self.nx.commands, [])

    async def test_interrupted_mutation_records_unknown_and_terminal(self):
        import asyncio

        cfg = read_config(ROOT / "docs/examples/platforms.json")
        cfg.event_store.settings = {"path": str(self.base / "history.sqlite3")}
        cfg.tool_registry.settings["plugins"] = [
            {
                "factory": "plugins.nxos:tool_plugin",
                "settings": {
                    "inventory_path": "inventory.json",
                    "enable_changes": True,
                },
            }
        ]
        cfg.validation_policy.settings = {"allow_changes": True}
        cfg.limits.changes = 1

        async def interrupted(*args):
            raise asyncio.CancelledError()

        with (
            patch.object(nxos, "session", self.nx.session),
            patch.object(vllm, "http_client", self.http.client),
            patch.object(self.nx, "write", interrupted),
        ):
            async with Application(cfg, self.base, requested=("incident_loop",)) as app:
                state = await self.with_evidence()
                with self.assertRaises(asyncio.CancelledError):
                    await app.services["incident_loop"].run(state)
                history = await app.services["event_store"].history(state.incident_id)
        self.assertEqual(history[-1].kind, "terminated")
        self.assertTrue(any(e.kind == "execution_unknown" for e in history))

    async def test_routed_interface_health_and_registered_read_outcomes(self):
        self.nx.switchport = {
            "TABLE_interface": {
                "ROW_interface": [
                    {"interface": "Ethernet1/1", "switchport": "Disabled"}
                ]
            }
        }
        plugin, state = self.plugin(), await self.state()
        c = plugin.candidate(
            "inspect_interface",
            {"resource_id": "nexus-lab", "interface": "Ethernet1/1"},
        )
        with patch.object(nxos, "session", self.nx.session):
            transport = await plugin.execute(c, state)
        self.assertEqual(transport.status, "succeeded")
        self.assertEqual(transport.raw_output["facts"]["mode"], "routed")
        self.assertEqual(
            len([s for s in transport.steps if s.step_id.startswith("read-")]), 4
        )
        self.assertIsNone(transport.raw_output["facts"]["access_vlan"])

    async def test_cli_configuration_compatibility(self):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "dowser",
                "validate",
                "--config",
                "docs/examples/platforms.json",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(result.stdout)["valid"])


if __name__ == "__main__":
    unittest.main()
