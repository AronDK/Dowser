"""Nexus 9000 10.4(x): fixed templates, strict structured parsing, scoped writes."""

import json
import re
from contextlib import asynccontextmanager

from pydantic import Field

from dowser.contracts import ToolSpec, factory
from dowser.models import TransportResult, ValidationResult, now

from .base import PlatformTools
from .common import (
    InventorySettings,
    Strict,
    ToolSettings,
    env_value,
    http_client,
    optional,
    step,
)
from .normalizers import PlatformNormalizer


class DeviceArgs(Strict):
    resource_id: str = Field(min_length=1)


class InterfaceArgs(DeviceArgs):
    interface: str = Field(pattern=r"^Ethernet\d+/\d+(?:/\d+)?$")


class VLANArgs(DeviceArgs):
    vlan: int = Field(ge=1, le=4094)


class AccessArgs(InterfaceArgs):
    vlan: int = Field(ge=1, le=4094)


class RouteArgs(DeviceArgs):
    vrf: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    prefix: str = Field(pattern=r"^\d+\.\d+\.\d+\.\d+/\d+$")


class CommandRejected(ValueError):
    pass


def rows(body, table, row):
    value = body[table][row]
    if isinstance(value, dict):
        value = [value]
    if not isinstance(value, list) or any(not isinstance(v, dict) for v in value):
        raise ValueError("unsupported NX-OS row shape")
    return value


def one(body, table, row, key, expected):
    result = [v for v in rows(body, table, row) if str(v[key]) == str(expected)]
    if len(result) != 1:
        raise ValueError("missing or ambiguous scoped row")
    return result[0]


def nxapi_outputs(value, commands):
    output = value["ins_api"]["outputs"]["output"]
    if isinstance(output, dict):
        output = [output]
    if not isinstance(output, list) or len(output) != len(commands):
        raise ValueError("missing command acknowledgments")
    if any(
        o.get("input") != cmd
        or str(o.get("code")) not in {"200", "400", "401", "403", "500"}
        for o, cmd in zip(output, commands, strict=True)
    ):
        raise ValueError("unsupported NX-API acknowledgment")
    return output


@asynccontextmanager
async def session(device):
    if device.transport == "nxapi":
        async with http_client(device) as client:
            yield NXAPI(client, device)
    else:
        ssh = optional("asyncssh", "nxos")
        async with ssh.connect(
            device.endpoint,
            port=device.port,
            username=env_value(device.username_env),
            password=env_value(device.password_env),
            known_hosts=device.known_hosts,
            connect_timeout=device.timeout,
        ) as conn:
            yield SSH(conn, device)


class NXAPI:
    def __init__(self, client, device):
        self.client, self.device = client, device

    async def request(self, commands, write=False):
        response = await self.client.post(
            "/ins",
            auth=(
                env_value(self.device.username_env),
                env_value(self.device.password_env),
            ),
            json={
                "ins_api": {
                    "version": "1.0",
                    "type": "cli_conf" if write else "cli_show_array",
                    "chunk": "0",
                    "sid": "1",
                    "input": " ; ".join(commands),
                    "output_format": "json",
                }
            },
        )
        response.raise_for_status()
        return nxapi_outputs(response.json(), commands)

    async def show(self, command):
        value = (await self.request([command]))[0]
        if str(value["code"]) != "200":
            raise CommandRejected("device rejected registered show command")
        if not isinstance(value.get("body"), dict):
            raise ValueError("unsupported NX-API body")
        return value["body"]

    async def write(self, commands, resource, steps):
        started = now()
        # All command results are correlated before claiming any success.
        try:
            output = await self.request(commands, write=True)
        except Exception:
            for index in range(len(commands)):
                steps.append(step(f"command-{index}", resource, started, "unknown"))
            raise
        for index, ack in enumerate(output):
            steps.append(
                step(
                    f"command-{index}",
                    resource,
                    started,
                    "succeeded" if str(ack["code"]) == "200" else "failed",
                )
            )
        if any(str(ack["code"]) != "200" for ack in output):
            raise CommandRejected(
                "configuration procedure was rejected or partially applied"
            )


class SSH:
    def __init__(self, connection, device):
        self.connection, self.device = connection, device

    async def show(self, command):
        result = await self.connection.run(
            command + " | json", check=False, timeout=self.device.timeout
        )
        if result.exit_status != 0:
            raise CommandRejected("SSH show failed")
        body = json.loads(result.stdout)
        if not isinstance(body, dict):
            raise ValueError("unsupported SSH JSON body")
        return body

    async def write(self, commands, resource, steps):
        import asyncio

        async with self.connection.create_process(term_type="vt100") as process:

            async def prompt():
                return await asyncio.wait_for(
                    process.stdout.readuntil(re.compile(r"(?m)^[\w()./-]+[>#] ?$")),
                    self.device.timeout,
                )

            await prompt()
            for index, command in enumerate(["configure terminal", *commands, "end"]):
                started = now()
                process.stdin.write(command + "\n")
                try:
                    output = await prompt()
                except Exception:
                    steps.append(step(f"command-{index}", resource, started, "unknown"))
                    raise
                failed = bool(
                    re.search(
                        r"(?im)^\s*(?:%|error:|invalid command|syntax error)", output
                    )
                )
                steps.append(
                    step(
                        f"command-{index}",
                        resource,
                        started,
                        "failed" if failed else "succeeded",
                    )
                )
                if failed:
                    raise CommandRejected("SSH configuration command rejected")
            process.stdin.write("exit\n")


class RecordedSession:
    """Record registered read-command outcomes without retaining their raw bodies."""

    def __init__(self, client, resource, steps):
        self.client, self.resource, self.steps = client, resource, steps
        self.read_index = 0

    async def show(self, command):
        started = now()
        name = f"read-{self.read_index}"
        self.read_index += 1
        try:
            body = await self.client.show(command)
        except Exception:
            self.steps.append(step(name, self.resource, started, "failed"))
            raise
        outcome = step(name, self.resource, started)
        outcome.detail = command
        self.steps.append(outcome)
        return body

    async def write(self, commands, resource, steps):
        await self.client.write(commands, resource, steps)


def device_facts(body):
    version, chassis = body["nxos_ver_str"], body["chassis_id"]
    if (
        not isinstance(version, str)
        or not re.fullmatch(r"10\.4\(\d+[a-z]?\)", version)
        or not isinstance(chassis, str)
        or not re.search(r"(?:Nexus\s*9000|N9K|C9\d{3})", chassis, re.I)
    ):
        raise ValueError("unsupported Nexus platform/version")
    return {"version": version, "chassis": chassis[:128], "reachable": True}


def interface_facts(body, switchport, channels, interface):
    row = one(body, "TABLE_interface", "ROW_interface", "interface", interface)
    sw = one(switchport, "TABLE_interface", "ROW_interface", "interface", interface)
    admin, oper = row["admin_state"], row["state"]
    if admin not in {"up", "down"} or oper not in {"up", "down"}:
        raise ValueError("unsupported interface state")
    if sw["switchport"] == "Disabled":
        mode, access_vlan = "routed", None
    elif sw["switchport"] == "Enabled" and sw["oper_mode"] in {"access", "trunk"}:
        mode, access_vlan = sw["oper_mode"], int(sw["access_vlan"])
        if not 1 <= access_vlan <= 4094:
            raise ValueError("invalid live access VLAN")
    else:
        raise ValueError("unsupported switchport mode")
    members = []
    for channel in rows(channels, "TABLE_channel", "ROW_channel"):
        for member in rows(channel, "TABLE_member", "ROW_member"):
            members.append(member["port"])
    # Port-channel summary may abbreviate physical ports as Eth1/1.
    member = interface in members or interface.replace("Ethernet", "Eth") in members
    facts = {
        "interface": interface,
        "admin_state": admin,
        "oper_state": oper,
        "mode": mode,
        "access_vlan": access_vlan,
        "port_channel_member": member,
    }
    for field in ("eth_inerr", "eth_outerr", "eth_indiscard", "eth_outdiscard"):
        if field in row:
            facts[field] = int(row[field])
    return facts


def vlan_facts(body, vlan, interfaces):
    matches = [
        r
        for r in rows(body, "TABLE_vlanbrief", "ROW_vlanbrief")
        if int(r["vlanshowbr-vlanid"]) == vlan
    ]
    if not matches:
        return {"vlan": vlan, "vlan_exists": False}
    if len(matches) != 1 or matches[0]["vlanshowbr-vlanstate"] not in {
        "active",
        "suspend",
    }:
        raise ValueError("unsupported VLAN state/shape")
    row = matches[0]
    ports = row.get("vlanshowplist-ifidx", "")
    if not isinstance(ports, str):
        raise ValueError("unsupported VLAN membership")
    tokens = {
        v.strip().replace("Eth", "Ethernet", 1)
        if v.strip().startswith("Eth") and not v.strip().startswith("Ethernet")
        else v.strip()
        for v in ports.split(",")
    }
    return {
        "vlan": vlan,
        "vlan_exists": True,
        "vlan_state": row["vlanshowbr-vlanstate"],
        "members": [v for v in interfaces if v in tokens],
    }


def route_facts(body, vrf, prefix):
    row = one(body, "TABLE_vrf", "ROW_vrf", "vrf-name-out", vrf)
    address = one(row, "TABLE_addrf", "ROW_addrf", "addrf", "ipv4")
    found = [
        v
        for v in rows(address, "TABLE_prefix", "ROW_prefix")
        if v["ipprefix"] == prefix
    ]
    if len(found) > 1:
        raise ValueError("ambiguous route")
    return {"vrf": vrf, "prefix": prefix, "route_present": bool(found)}


class NXOSTools(PlatformTools):
    platform = "nxos"
    parser_version = "n9k-10.4-v1"
    tools = tuple(
        ToolSpec(
            f"nxos.{name}",
            "1",
            args,
            "change" if change else "read_only",
            "remediation" if change else "observation",
            {"nxos": ("10.4(x)",)},
            ("nxos.desired",),
        )
        for name, args, change in (
            ("inspect_device", DeviceArgs, False),
            ("inspect_interface", InterfaceArgs, False),
            ("inspect_vlan", VLANArgs, False),
            ("inspect_routes", RouteArgs, False),
            ("ensure_interface_enabled", InterfaceArgs, True),
            ("ensure_access_vlan", AccessArgs, True),
        )
    )

    async def candidates(self, state):
        result = []
        for resource in state.resources:
            if (
                resource.platform != self.platform
                or resource.id not in self.inventory.resources
            ):
                continue
            scope = resource.payload.get("scope", {})
            base = {"resource_id": resource.id}
            result.append(self.candidate("inspect_device", base))
            for interface in scope.get("interfaces", []):
                args = {**base, "interface": interface}
                result.append(self.candidate("inspect_interface", args))
                evidence = [
                    o.id
                    for o in state.observations
                    if o.resource_id == resource.id
                    and o.kind == "nxos.inspect_interface"
                    and o.payload.get("interface") == interface
                ]
                if self.settings.enable_changes and evidence:
                    if state.desired_state.get("admin_state") == "up":
                        result.append(
                            self.candidate(
                                "ensure_interface_enabled", args, evidence[-1:]
                            )
                        )
                    if "access_vlan" in state.desired_state:
                        result.append(
                            self.candidate(
                                "ensure_access_vlan",
                                {**args, "vlan": state.desired_state["access_vlan"]},
                                evidence[-1:],
                            )
                        )
            for vlan in scope.get("vlans", []):
                result.append(self.candidate("inspect_vlan", {**base, "vlan": vlan}))
            for vrf in scope.get("vrfs", []):
                for prefix in scope.get("prefixes", []):
                    result.append(
                        self.candidate(
                            "inspect_routes", {**base, "vrf": vrf, "prefix": prefix}
                        )
                    )
        return result

    async def read_interface(self, client, args):
        return interface_facts(
            await client.show(f"show interface {args.interface}"),
            await client.show(f"show interface {args.interface} switchport"),
            await client.show("show port-channel summary"),
            args.interface,
        )

    def preconditions(self, candidate, state, args, item, facts):
        if args.interface in item.protected_interfaces or facts["port_channel_member"]:
            raise ValueError("protected or port-channel member interface")
        if candidate.tool.endswith("ensure_interface_enabled"):
            if (
                state.desired_state.get("admin_state") != "up"
                or facts["admin_state"] != "down"
            ):
                raise ValueError("admin-up intent and live admin-down are required")
        else:
            if (
                state.desired_state.get("access_vlan") != args.vlan
                or facts["mode"] != "access"
                or facts["access_vlan"] == args.vlan
            ):
                raise ValueError("access-port VLAN intent/precondition mismatch")

    async def validate(self, candidate, state):
        try:
            args, item, resource = self.target(candidate, state)
            if candidate.effect == "change":
                async with session(item) as client:
                    device_facts(await client.show("show version"))
                    facts = await self.read_interface(client, args)
                    self.preconditions(candidate, state, args, item, facts)
                    if isinstance(args, AccessArgs):
                        vlan = vlan_facts(
                            await client.show(f"show vlan id {args.vlan}"),
                            args.vlan,
                            resource.payload.get("scope", {}).get("interfaces", []),
                        )
                        if (
                            not vlan.get("vlan_exists")
                            or vlan.get("vlan_state") != "active"
                        ):
                            raise ValueError("approved VLAN must exist and be active")
            return ValidationResult(allowed=True)
        except Exception:
            return ValidationResult(
                allowed=False, reason="NX-OS scope or live preconditions failed"
            )

    async def execute(self, candidate, state):
        steps, writing, acknowledged = [], False, False
        try:
            args, item, resource = self.target(candidate, state)
            async with session(item) as transport:
                client = RecordedSession(transport, args.resource_id, steps)
                facts = device_facts(await client.show("show version"))
                steps.append(
                    step("device-profile", args.resource_id, now(), facts=facts)
                )
                name = candidate.tool.split(".")[1]
                if name in {
                    "inspect_interface",
                    "ensure_interface_enabled",
                    "ensure_access_vlan",
                }:
                    facts = await self.read_interface(client, args)
                if name in {"inspect_vlan", "ensure_access_vlan"}:
                    vlan = vlan_facts(
                        await client.show(f"show vlan id {args.vlan}"),
                        args.vlan,
                        resource.payload.get("scope", {}).get("interfaces", []),
                    )
                    if name == "inspect_vlan":
                        facts = vlan
                if name == "inspect_routes":
                    facts = route_facts(
                        await client.show(
                            f"show ip route {args.prefix} vrf {args.vrf}"
                        ),
                        args.vrf,
                        args.prefix,
                    )
                if candidate.effect == "change":
                    self.preconditions(candidate, state, args, item, facts)
                    if isinstance(args, AccessArgs) and (
                        not vlan.get("vlan_exists")
                        or vlan.get("vlan_state") != "active"
                    ):
                        raise ValueError("VLAN is not active")
                    steps.append(
                        step("live-preconditions", args.resource_id, now(), facts=facts)
                    )
                    commands = [
                        f"interface {args.interface}",
                        "no shutdown"
                        if name == "ensure_interface_enabled"
                        else f"switchport access vlan {args.vlan}",
                    ]
                    writing = True
                    await client.write(commands, args.resource_id, steps)
                    acknowledged = True
                    facts = await self.read_interface(client, args)
                    steps.append(
                        step("post-change", args.resource_id, now(), facts=facts)
                    )
            return self.clean(
                TransportResult(
                    status="succeeded", raw_output={"facts": facts}, steps=steps
                )
            )
        except Exception as exc:
            # Missing/malformed ack or failure after dispatch leaves effects uncertain.
            status = (
                "partial"
                if writing and (isinstance(exc, CommandRejected) or acknowledged)
                else "unknown"
                if writing
                else "failed"
            )
            steps.append(
                step("procedure-failure", candidate.args["resource_id"], now(), status)
            )
            return self.clean(
                TransportResult(
                    status=status,
                    transport_status="unknown" if status == "unknown" else "failed",
                    steps=steps,
                    detail="NX-OS procedure failed; reconcile unknown writes before retry",
                )
            )

    async def verify(self, state, candidate, result):
        facts = (
            result.parse.observations[0].payload if result.parse.observations else {}
        )
        desired = state.desired_state
        # Never resolve other symptoms merely because the device is reachable.
        matched = None
        if desired and set(desired) <= set(facts):
            matched = all(facts[k] == v for k, v in desired.items())
        return self.verdict(result, matched)


class NXOSNormalizer(PlatformNormalizer):
    platform = "nxos"


@factory(subsystem="tool_plugin", component_type=NXOSTools, settings_model=ToolSettings)
def tool_plugin(settings, context):
    return NXOSTools(settings, context)


@factory(
    subsystem="normalizer",
    component_type=NXOSNormalizer,
    settings_model=InventorySettings,
)
def normalizer(settings, context):
    return NXOSNormalizer(settings, context)
