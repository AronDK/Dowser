"""Kubernetes API v1/apps-v1 diagnostics and scoped deployment remediation."""

import asyncio
import re
import time
from contextlib import asynccontextmanager
from urllib.parse import quote

from pydantic import Field

from dowser.contracts import ToolSpec, factory
from dowser.models import MemoryQueryIntent, TransportResult, ValidationResult, now

from .base import PlatformTools
from .common import (
    K8S_NAME,
    K8S_SUBDOMAIN,
    InventorySettings,
    Strict,
    ToolSettings,
    http_client,
    network_error,
    step,
)
from .normalizers import PlatformNormalizer


class ClusterArgs(Strict):
    resource_id: str = Field(min_length=1)


class NodeArgs(ClusterArgs):
    node: str = Field(pattern=f"^{K8S_SUBDOMAIN}$")


class NamespaceArgs(ClusterArgs):
    namespace: str = Field(pattern=f"^{K8S_NAME}$")


class PodArgs(NamespaceArgs):
    pod: str = Field(pattern=f"^{K8S_SUBDOMAIN}$")


class DeploymentArgs(NamespaceArgs):
    deployment: str = Field(pattern=f"^{K8S_SUBDOMAIN}$")


class ScaleArgs(DeploymentArgs):
    replicas: int = Field(ge=1, le=100)


class K8sServiceArgs(NamespaceArgs):
    service: str = Field(pattern=f"^{K8S_NAME}$")


class K8sSettings(ToolSettings):
    rollout_seconds: float = Field(default=300, gt=0, allow_inf_nan=False)
    readiness_checks: int = Field(default=3, ge=1)
    readiness_interval: float = Field(default=5, gt=0, allow_inf_nan=False)


@asynccontextmanager
async def k8s_client(cluster):
    async with http_client(cluster) as client:
        yield client


def condition_map(conditions):
    if not isinstance(conditions, list):
        raise ValueError("unsupported Kubernetes conditions shape")
    result = {}
    for row in conditions:
        if (
            not isinstance(row, dict)
            or not isinstance(row.get("type"), str)
            or row.get("status") not in {"True", "False", "Unknown"}
        ):
            raise ValueError("unsupported Kubernetes condition entry")
        result[row["type"]] = row["status"]
    return result


async def inspect_cluster(client):
    try:
        livez = await client.get("/livez")
    except Exception as exc:
        if not network_error(exc):
            raise
        return {"reachable": False, "git_version": None}
    if livez.status_code not in {200, 500, 503}:
        raise ValueError("unexpected Kubernetes /livez status")
    if livez.status_code != 200:
        return {"reachable": False, "git_version": None}
    response = await client.get("/version")
    response.raise_for_status()
    body = response.json()
    version = body.get("gitVersion") if isinstance(body, dict) else None
    if not isinstance(version, str) or not re.fullmatch(
        r"v1\.\d+\.\d+(?:[-+][A-Za-z0-9._-]+)?", version
    ):
        raise ValueError("unsupported Kubernetes gitVersion")
    return {"reachable": True, "git_version": version[:64]}


def node_facts(body, node):
    if not isinstance(body, dict):
        raise ValueError("unsupported Node object")
    metadata = body.get("metadata") or {}
    if metadata.get("name") != node:
        raise ValueError("Kubernetes Node identity mismatch")
    spec = body.get("spec") or {}
    unschedulable = spec.get("unschedulable", False)
    if type(unschedulable) is not bool:
        raise ValueError("unsupported Node unschedulable field")
    status = body.get("status") or {}
    conditions = condition_map(status.get("conditions"))
    if "Ready" not in conditions:
        raise ValueError("missing Node Ready condition")
    info = status.get("nodeInfo") or {}
    version = info.get("kubeletVersion")
    if not isinstance(version, str) or not version:
        raise ValueError("missing kubeletVersion")
    return {
        "node": node,
        "node_ready": conditions["Ready"] == "True",
        "unschedulable": unschedulable,
        "memory_pressure": conditions.get("MemoryPressure") == "True",
        "disk_pressure": conditions.get("DiskPressure") == "True",
        "pid_pressure": conditions.get("PIDPressure") == "True",
        "kubelet_version": version[:64],
    }


def pod_facts(body, namespace, pod):
    if not isinstance(body, dict):
        raise ValueError("unsupported Pod object")
    metadata = body.get("metadata") or {}
    if metadata.get("namespace") != namespace or metadata.get("name") != pod:
        raise ValueError("Kubernetes Pod identity mismatch")
    status = body.get("status") or {}
    phase = status.get("phase")
    if phase not in {"Pending", "Running", "Succeeded", "Failed", "Unknown"}:
        raise ValueError("unsupported Pod phase")
    conditions = condition_map(status.get("conditions") or [])
    containers = status.get("containerStatuses") or []
    if not isinstance(containers, list):
        raise ValueError("unsupported Pod containerStatuses")
    restarts = 0
    oom_killed = False
    waiting = []
    for item in containers:
        if not isinstance(item, dict):
            raise ValueError("unsupported container status")
        count = item.get("restartCount", 0)
        if type(count) is not int or count < 0:
            raise ValueError("invalid container restartCount")
        restarts += count
        for key in ("state", "lastState"):
            state = item.get(key) or {}
            if not isinstance(state, dict):
                raise ValueError("unsupported container state")
            terminated = state.get("terminated")
            if isinstance(terminated, dict) and terminated.get("reason") == "OOMKilled":
                oom_killed = True
            if key == "state" and isinstance(state.get("waiting"), dict):
                reason = state["waiting"].get("reason")
                if isinstance(reason, str) and reason:
                    waiting.append(reason[:64])
    return {
        "namespace": namespace,
        "pod": pod,
        "pod_phase": phase,
        "pod_ready": conditions.get("Ready") == "True",
        "restart_count": restarts,
        "oom_killed": oom_killed,
        "waiting_reasons": sorted(set(waiting)),
    }


def deployment_facts(body, namespace, deployment):
    if not isinstance(body, dict):
        raise ValueError("unsupported Deployment object")
    metadata = body.get("metadata") or {}
    if (
        metadata.get("namespace") != namespace
        or metadata.get("name") != deployment
        or type(metadata.get("generation")) is not int
        or metadata["generation"] < 1
    ):
        raise ValueError("Kubernetes Deployment identity or generation mismatch")
    spec = body.get("spec") or {}
    if not isinstance(spec, dict):
        raise ValueError("unsupported Deployment spec")
    spec_replicas = spec.get("replicas", 1)
    paused = spec.get("paused", False)
    if type(spec_replicas) is not int or spec_replicas < 0 or type(paused) is not bool:
        raise ValueError("unsupported Deployment replica or paused state")
    status = body.get("status") or {}
    if not isinstance(status, dict):
        raise ValueError("unsupported Deployment status")
    counts = {}
    for field in (
        "observedGeneration",
        "readyReplicas",
        "availableReplicas",
        "updatedReplicas",
        "unavailableReplicas",
    ):
        value = status.get(field, 0)
        if type(value) is not int or value < 0:
            raise ValueError(f"invalid Deployment {field}")
        counts[field] = value
    conditions = condition_map(status.get("conditions") or [])
    available = bool(
        not paused
        and spec_replicas > 0
        and counts["observedGeneration"] >= metadata["generation"]
        and counts["readyReplicas"] >= spec_replicas
        and counts["availableReplicas"] >= spec_replicas
        and counts["updatedReplicas"] >= spec_replicas
        and counts["unavailableReplicas"] == 0
        and conditions.get("Available") == "True"
    )
    return {
        "namespace": namespace,
        "deployment": deployment,
        "paused": paused,
        "generation": metadata["generation"],
        "observed_generation": counts["observedGeneration"],
        "spec_replicas": spec_replicas,
        "ready_replicas": counts["readyReplicas"],
        "available_replicas": counts["availableReplicas"],
        "updated_replicas": counts["updatedReplicas"],
        "unavailable_replicas": counts["unavailableReplicas"],
        "deployment_available": available,
    }


def service_facts(service_body, endpoints_body, namespace, service):
    if not isinstance(service_body, dict) or not isinstance(endpoints_body, dict):
        raise ValueError("unsupported Service or Endpoints object")
    svc_meta = service_body.get("metadata") or {}
    ep_meta = endpoints_body.get("metadata") or {}
    if (
        svc_meta.get("namespace") != namespace
        or svc_meta.get("name") != service
        or ep_meta.get("namespace") != namespace
        or ep_meta.get("name") != service
    ):
        raise ValueError("Kubernetes Service/Endpoints identity mismatch")
    spec = service_body.get("spec") or {}
    service_type = spec.get("type", "ClusterIP")
    if service_type not in {"ClusterIP", "NodePort", "LoadBalancer", "ExternalName"}:
        raise ValueError("unsupported Kubernetes Service type")
    raw_ports = spec.get("ports") or []
    if not isinstance(raw_ports, list):
        raise ValueError("unsupported Service ports")
    ports = []
    for port in raw_ports:
        if (
            not isinstance(port, dict)
            or type(port.get("port")) is not int
            or not 1 <= port["port"] <= 65535
        ):
            raise ValueError("invalid Service port")
        ports.append(port["port"])
    subsets = endpoints_body.get("subsets") or []
    if not isinstance(subsets, list):
        raise ValueError("unsupported Endpoints subsets")
    ready, not_ready = 0, 0
    for subset in subsets:
        if not isinstance(subset, dict):
            raise ValueError("unsupported Endpoints subset")
        addrs = subset.get("addresses") or []
        unready = subset.get("notReadyAddresses") or []
        if not isinstance(addrs, list) or not isinstance(unready, list):
            raise ValueError("unsupported Endpoints address list")
        ready += len(addrs)
        not_ready += len(unready)
    return {
        "namespace": namespace,
        "service": service,
        "service_type": service_type,
        "ports": sorted(ports),
        "ready_addresses": ready,
        "not_ready_addresses": not_ready,
        "endpoints_ready": ready > 0,
    }


async def read_deployment(client, namespace, deployment):
    ns, dep = quote(namespace, safe=""), quote(deployment, safe="")
    response = await client.get(f"/apis/apps/v1/namespaces/{ns}/deployments/{dep}")
    response.raise_for_status()
    return deployment_facts(response.json(), namespace, deployment)


class K8sTools(PlatformTools):
    platform = "k8s"
    parser_version = "k8s-v1"
    tools = tuple(
        ToolSpec(
            f"k8s.{name}",
            "1",
            args,
            "change" if change else "read_only",
            "remediation" if change else "observation",
            {"k8s": ("v1",)},
            ("k8s.desired",),
        )
        for name, args, change in (
            ("inspect_cluster", ClusterArgs, False),
            ("inspect_node", NodeArgs, False),
            ("inspect_pod", PodArgs, False),
            ("inspect_deployment", DeploymentArgs, False),
            ("inspect_service", K8sServiceArgs, False),
            ("scale_deployment", ScaleArgs, True),
            ("restart_deployment", DeploymentArgs, True),
        )
    )

    async def candidates(self, state):
        result = []
        for resource in state.resources:
            if (
                resource.platform != "k8s"
                or resource.id not in self.inventory.resources
            ):
                continue
            item = self.inventory.resources[resource.id]
            scope = resource.payload.get("scope", {})
            base = {"resource_id": resource.id}
            result.append(self.candidate("inspect_cluster", base))
            for node in scope.get("nodes", []):
                result.append(self.candidate("inspect_node", {**base, "node": node}))
            for namespace in scope.get("namespaces", []):
                ns_base = {**base, "namespace": namespace}
                for pod in scope.get("pods", []):
                    result.append(
                        self.candidate("inspect_pod", {**ns_base, "pod": pod})
                    )
                for service in scope.get("services", []):
                    result.append(
                        self.candidate(
                            "inspect_service", {**ns_base, "service": service}
                        )
                    )
                for deployment in scope.get("deployments", []):
                    dep_args = {**ns_base, "deployment": deployment}
                    result.append(self.candidate("inspect_deployment", dep_args))
                    if (
                        not self.settings.enable_changes
                        or namespace in item.protected_namespaces
                    ):
                        continue
                    target_replicas = self.desired_scale_target(
                        state, resource.id, namespace, deployment, item
                    )
                    scale_evidence = self.supporting_scale(
                        state, resource.id, namespace, deployment
                    )
                    if target_replicas is not None and scale_evidence:
                        result.append(
                            self.candidate(
                                "scale_deployment",
                                {**dep_args, "replicas": target_replicas},
                                scale_evidence,
                            )
                        )
                    restart_evidence = self.supporting_restart(
                        state, resource.id, namespace, deployment
                    )
                    if (
                        state.desired_state.get("deployment_available") is True
                        and restart_evidence
                    ):
                        result.append(
                            self.candidate(
                                "restart_deployment", dep_args, restart_evidence
                            )
                        )
        return result

    async def memory_query_hints(self, state, candidates):
        intents = []
        for resource in state.resources:
            if resource.platform != self.platform:
                continue
            scope = resource.payload.get("scope", {})
            for ns in scope.get("namespaces", []):
                for dep in scope.get("deployments", []):
                    intents.append(
                        MemoryQueryIntent(
                            text=f"Kubernetes deployment {ns}/{dep} availability replicas rollout",
                            source="k8s",
                            entity=resource.id,
                            tool="k8s.inspect_deployment",
                        )
                    )
                for pod in scope.get("pods", []):
                    intents.append(
                        MemoryQueryIntent(
                            text=f"Kubernetes pod {ns}/{pod} readiness phase restarts OOMKilled",
                            source="k8s",
                            entity=resource.id,
                            tool="k8s.inspect_pod",
                        )
                    )
                for svc in scope.get("services", []):
                    intents.append(
                        MemoryQueryIntent(
                            text=f"Kubernetes service {ns}/{svc} endpoints ready addresses",
                            source="k8s",
                            entity=resource.id,
                            tool="k8s.inspect_service",
                        )
                    )
            for node in scope.get("nodes", []):
                intents.append(
                    MemoryQueryIntent(
                        text=f"Kubernetes node {node} readiness pressure schedulable",
                        source="k8s",
                        entity=resource.id,
                        tool="k8s.inspect_node",
                    )
                )
        return intents

    def desired_scale_target(self, state, resource_id, namespace, deployment, item):
        desired = state.desired_state
        if "ready_replicas" in desired:
            replicas = desired["ready_replicas"]
            return replicas if 1 <= replicas <= item.max_replicas else None
        if desired.get("deployment_available") is True:
            for o in reversed(state.observations):
                if (
                    o.resource_id == resource_id
                    and o.kind == "k8s.inspect_deployment"
                    and o.payload.get("namespace") == namespace
                    and o.payload.get("deployment") == deployment
                ):
                    if o.payload.get("spec_replicas") == 0:
                        return 1
                    break
        return None

    def supporting_scale(self, state, resource_id, namespace, deployment):
        result = []
        for o in state.observations:
            age = (now() - o.observed_at).total_seconds()
            if o.resource_id != resource_id or not 0 <= age <= self.freshness:
                continue
            p = o.payload
            if (
                o.kind == "k8s.inspect_deployment"
                and p.get("namespace") == namespace
                and p.get("deployment") == deployment
            ):
                result.append(o.id)
        return result[-1:]

    def supporting_restart(self, state, resource_id, namespace, deployment):
        result = []
        for o in state.observations:
            age = (now() - o.observed_at).total_seconds()
            if o.resource_id != resource_id or not 0 <= age <= self.freshness:
                continue
            p = o.payload
            if p.get("namespace") != namespace:
                continue
            failed = (
                o.kind == "k8s.inspect_deployment"
                and p.get("deployment") == deployment
                and (
                    p.get("deployment_available") is False
                    or p.get("unavailable_replicas", 0) > 0
                )
                or o.kind == "k8s.inspect_pod"
                and (p.get("pod_ready") is False or p.get("oom_killed") is True)
                or o.kind == "k8s.inspect_service"
                and p.get("endpoints_ready") is False
            )
            if failed:
                result.append(o.id)
        return result[-1:]

    def preconditions(self, candidate, state, args, item, facts):
        if args.namespace in item.protected_namespaces:
            raise ValueError("protected Kubernetes namespace cannot be modified")
        if facts["paused"]:
            raise ValueError("paused Kubernetes deployment cannot be modified")
        if candidate.tool == "k8s.scale_deployment":
            if not 1 <= args.replicas <= item.max_replicas:
                raise ValueError("requested replicas exceed inventory max_replicas")
            target = self.desired_scale_target(
                state, args.resource_id, args.namespace, args.deployment, item
            )
            if (
                target != args.replicas
                or not self.supporting_scale(
                    state, args.resource_id, args.namespace, args.deployment
                )
                or facts["spec_replicas"] == args.replicas
            ):
                raise ValueError("scale intent or live replica preconditions failed")
        else:
            if (
                state.desired_state.get("deployment_available") is not True
                or not self.supporting_restart(
                    state, args.resource_id, args.namespace, args.deployment
                )
                or facts["spec_replicas"] < 1
            ):
                raise ValueError(
                    "restart requires availability intent, fresh failure evidence and active replicas"
                )

    async def validate(self, candidate, state):
        try:
            args, item, _ = self.target(candidate, state)
            if candidate.effect == "change":
                async with k8s_client(item) as client:
                    facts = await read_deployment(
                        client, args.namespace, args.deployment
                    )
                    self.preconditions(candidate, state, args, item, facts)
            return ValidationResult(allowed=True)
        except Exception:
            return ValidationResult(
                allowed=False, reason="Kubernetes scope or live preconditions failed"
            )

    async def rollout_readiness(
        self, client, args, steps, *, expected_replicas, min_generation=1
    ):
        deadline = time.monotonic() + self.settings.rollout_seconds
        consecutive, index = 0, 0
        facts = {
            "namespace": args.namespace,
            "deployment": args.deployment,
            "deployment_available": False,
            "ready_replicas": 0,
        }
        while time.monotonic() < deadline:
            started = now()
            try:
                facts = await asyncio.wait_for(
                    read_deployment(client, args.namespace, args.deployment),
                    max(0.001, deadline - time.monotonic()),
                )
                ready = bool(
                    facts["deployment_available"]
                    and facts["observed_generation"] >= min_generation
                    and facts["spec_replicas"] == expected_replicas
                    and facts["ready_replicas"] == expected_replicas
                )
            except Exception:
                facts = {
                    "namespace": args.namespace,
                    "deployment": args.deployment,
                    "deployment_available": False,
                    "ready_replicas": 0,
                }
                ready = False
            consecutive = consecutive + 1 if ready else 0
            steps.append(
                step(f"readiness-{index}", args.resource_id, started, facts=facts)
            )
            index += 1
            if consecutive >= self.settings.readiness_checks:
                facts["readiness_checks"] = consecutive
                return facts
            await asyncio.sleep(
                min(
                    self.settings.readiness_interval,
                    max(0, deadline - time.monotonic()),
                )
            )
        facts["readiness_checks"] = consecutive
        return facts

    async def execute(self, candidate, state):
        steps, dispatched, acknowledged = [], False, False
        try:
            args, item, _ = self.target(candidate, state)
            name = candidate.tool.split(".")[1]
            async with k8s_client(item) as client:
                if name == "inspect_cluster":
                    facts = await inspect_cluster(client)
                elif name == "inspect_node":
                    response = await client.get(
                        f"/api/v1/nodes/{quote(args.node, safe='')}"
                    )
                    response.raise_for_status()
                    facts = node_facts(response.json(), args.node)
                elif name == "inspect_pod":
                    ns, pod = quote(args.namespace, safe=""), quote(args.pod, safe="")
                    response = await client.get(f"/api/v1/namespaces/{ns}/pods/{pod}")
                    response.raise_for_status()
                    facts = pod_facts(response.json(), args.namespace, args.pod)
                elif name == "inspect_deployment":
                    facts = await read_deployment(
                        client, args.namespace, args.deployment
                    )
                elif name == "inspect_service":
                    ns, svc = (
                        quote(args.namespace, safe=""),
                        quote(args.service, safe=""),
                    )
                    svc_resp = await client.get(
                        f"/api/v1/namespaces/{ns}/services/{svc}"
                    )
                    svc_resp.raise_for_status()
                    ep_resp = await client.get(
                        f"/api/v1/namespaces/{ns}/endpoints/{svc}"
                    )
                    ep_resp.raise_for_status()
                    facts = service_facts(
                        svc_resp.json(), ep_resp.json(), args.namespace, args.service
                    )
                elif candidate.effect == "change":
                    facts = await read_deployment(
                        client, args.namespace, args.deployment
                    )
                    self.preconditions(candidate, state, args, item, facts)
                    steps.append(
                        step("live-preconditions", args.resource_id, now(), facts=facts)
                    )
                    ns = quote(args.namespace, safe="")
                    dep = quote(args.deployment, safe="")
                    started = now()
                    dispatched = True
                    if name == "scale_deployment":
                        expected_replicas = args.replicas
                        min_generation = facts["generation"]
                        response = await client.patch(
                            f"/apis/apps/v1/namespaces/{ns}/deployments/{dep}/scale",
                            json={"spec": {"replicas": args.replicas}},
                        )
                        ack_body = (
                            response.json() if response.status_code == 200 else {}
                        )
                        ack_ok = (
                            response.status_code == 200
                            and isinstance(ack_body, dict)
                            and (ack_body.get("spec") or {}).get("replicas")
                            == args.replicas
                        )
                    else:
                        expected_replicas = facts["spec_replicas"]
                        restarted_at = now().isoformat()
                        response = await client.patch(
                            f"/apis/apps/v1/namespaces/{ns}/deployments/{dep}",
                            json={
                                "spec": {
                                    "template": {
                                        "metadata": {
                                            "annotations": {
                                                "kubectl.kubernetes.io/restartedAt": restarted_at
                                            }
                                        }
                                    }
                                }
                            },
                        )
                        ack_body = (
                            response.json() if response.status_code == 200 else {}
                        )
                        ack_meta = (
                            ack_body.get("metadata") or {}
                            if isinstance(ack_body, dict)
                            else {}
                        )
                        min_generation = (
                            ack_meta.get("generation")
                            if type(ack_meta.get("generation")) is int
                            else facts["generation"]
                        )
                        ack_ok = (
                            response.status_code == 200
                            and ack_meta.get("name") == args.deployment
                            and ack_meta.get("namespace") == args.namespace
                        )
                    if not ack_ok:
                        steps.append(
                            step("mutation", args.resource_id, started, "unknown")
                        )
                        return self.clean(
                            TransportResult(
                                status="unknown",
                                transport_status="unknown",
                                steps=steps,
                                detail="Kubernetes mutation lacked expected acknowledgment",
                            )
                        )
                    acknowledged = True
                    steps.append(step("mutation", args.resource_id, started))
                    facts = await self.rollout_readiness(
                        client,
                        args,
                        steps,
                        expected_replicas=expected_replicas,
                        min_generation=min_generation,
                    )
                    if (
                        facts.get("readiness_checks", 0)
                        < self.settings.readiness_checks
                    ):
                        steps.append(
                            step(
                                "readiness-exhausted",
                                args.resource_id,
                                now(),
                                "failed",
                                facts,
                            )
                        )
                        return self.clean(
                            TransportResult(
                                status="partial",
                                raw_output={"facts": facts},
                                steps=steps,
                                detail="Kubernetes mutation acknowledged but rollout readiness failed",
                            )
                        )
                else:
                    raise ValueError("unsupported Kubernetes operation")
            return self.clean(
                TransportResult(
                    status="succeeded", raw_output={"facts": facts}, steps=steps
                )
            )
        except Exception:
            status = (
                "partial" if acknowledged else "unknown" if dispatched else "failed"
            )
            steps.append(
                step("procedure-failure", candidate.args["resource_id"], now(), status)
            )
            return self.clean(
                TransportResult(
                    status=status,
                    transport_status="unknown" if status == "unknown" else "failed",
                    steps=steps,
                    detail="Kubernetes procedure failed",
                )
            )

    async def verify(self, state, candidate, result):
        facts = (
            result.parse.observations[0].payload if result.parse.observations else {}
        )
        desired = state.desired_state
        matched = None
        if desired and set(desired) <= set(facts):
            matched = all(facts[k] == v for k, v in desired.items())
            if candidate.effect == "change":
                matched = (
                    matched
                    and facts.get("deployment_available") is True
                    and facts.get("readiness_checks", 0)
                    >= self.settings.readiness_checks
                )
        return self.verdict(result, matched)


class K8sNormalizer(PlatformNormalizer):
    platform = "k8s"


@factory(subsystem="tool_plugin", component_type=K8sTools, settings_model=K8sSettings)
def tool_plugin(settings, context):
    return K8sTools(settings, context)


@factory(
    subsystem="normalizer",
    component_type=K8sNormalizer,
    settings_model=InventorySettings,
)
def normalizer(settings, context):
    return K8sNormalizer(settings, context)
