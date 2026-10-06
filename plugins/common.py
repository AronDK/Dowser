"""Private inventory and per-call transports. Never expose endpoint settings as facts."""

import importlib
import ipaddress
import json
import os
import re
import ssl
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import ConfigDict, Field, field_validator, model_validator

from dowser.models import Boundary, ProcedureStep, now


class Strict(Boundary):
    model_config = ConfigDict(strict=True, extra="forbid", validate_assignment=True)


def optional(name, extra):
    try:
        return importlib.import_module(name)
    except ImportError:
        raise RuntimeError(f"Install dowser[{extra}] for this operation") from None


def endpoint(value, *, https=False):
    parsed = urlsplit(value)
    if (
        parsed.scheme not in ({"https"} if https else {"http", "https"})
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in ("", "/")
    ):
        raise ValueError("endpoint must be an origin without credentials or a path")
    return value.rstrip("/")


def env_value(ref):
    if ref is None:
        return None
    value = os.environ.get(ref)
    if not value:
        raise ValueError("required credential environment reference is unavailable")
    return value


class HTTPSettings(Strict):
    endpoint: str
    token_env: str | None = None
    ca_file: str | None = None
    timeout: float = Field(default=10, gt=0, allow_inf_nan=False)

    @field_validator("endpoint")
    @classmethod
    def origin(cls, value):
        return endpoint(value)


@asynccontextmanager
async def http_client(settings, *, unix_socket=None, cert=None):
    httpx = optional("httpx", "vllm" if isinstance(settings, HTTPSettings) else "nxos")
    verify = ssl.create_default_context(cafile=settings.ca_file)
    if cert:
        verify.load_cert_chain(*cert)
    headers = {}
    if getattr(settings, "token_env", None):
        headers["Authorization"] = f"Bearer {env_value(settings.token_env)}"
    transport = httpx.AsyncHTTPTransport(uds=unix_socket, verify=verify, retries=0)
    async with httpx.AsyncClient(
        transport=transport,
        base_url=settings.endpoint,
        timeout=settings.timeout,
        headers=headers,
        follow_redirects=False,
        trust_env=False,
    ) as client:
        yield client


class NXDevice(Strict):
    platform: Literal["nxos"] = "nxos"
    platform_version: Literal["10.4(x)"] = "10.4(x)"
    transport: Literal["nxapi", "ssh"]
    endpoint: str
    username_env: str
    password_env: str
    ca_file: str | None = None
    known_hosts: str | None = None
    port: int = Field(default=22, ge=1, le=65535)
    timeout: float = Field(default=10, gt=0, allow_inf_nan=False)
    interfaces: list[str] = Field(default_factory=list)
    protected_interfaces: list[str] = Field(default_factory=list)
    vlans: list[int] = Field(default_factory=list)
    vrfs: list[str] = Field(default_factory=list)
    prefixes: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def safe(self):
        if self.transport == "nxapi":
            endpoint(self.endpoint, https=True)
        elif not self.known_hosts or not re.fullmatch(
            r"[A-Za-z0-9.:-]+", self.endpoint
        ):
            raise ValueError("SSH requires a hostname and pinned known_hosts")
        if any(
            not re.fullmatch(r"Ethernet\d+/\d+(?:/\d+)?", x) for x in self.interfaces
        ):
            raise ValueError("only explicit physical Ethernet interfaces are supported")
        if not set(self.protected_interfaces) <= set(self.interfaces):
            raise ValueError("protected interfaces must be in inventory")
        if any(not 1 <= v <= 4094 for v in self.vlans):
            raise ValueError("invalid VLAN")
        if any(not re.fullmatch(r"[A-Za-z0-9_.-]+", v) for v in self.vrfs):
            raise ValueError("invalid VRF")
        for prefix in self.prefixes:
            if str(ipaddress.IPv4Network(prefix)) != prefix:
                raise ValueError("prefix must be canonical IPv4 CIDR")
        return self


class DockerTarget(Strict):
    endpoint: str = "http://docker"
    unix_socket: str | None = "/var/run/docker.sock"
    ca_file: str | None = None
    cert_file: str | None = None
    key_file: str | None = None
    timeout: float = Field(default=10, gt=0, allow_inf_nan=False)
    api_version: Literal["v1.51"] = "v1.51"
    container: str
    compose_project: str | None = None
    compose_service: str | None = None

    @model_validator(mode="after")
    def safe(self):
        endpoint(self.endpoint, https=self.unix_socket is None)
        if self.unix_socket is None and not all(
            (self.ca_file, self.cert_file, self.key_file)
        ):
            raise ValueError(
                "remote Docker requires explicit TLS CA and client certificate"
            )
        if self.unix_socket is not None and not Path(self.unix_socket).is_absolute():
            raise ValueError("Docker Unix socket must be absolute")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", self.container):
            raise ValueError("invalid Docker container identity")
        if bool(self.compose_project) != bool(self.compose_service):
            raise ValueError("both Compose project and service are required")
        return self


class VLLMService(HTTPSettings):
    platform: Literal["vllm"] = "vllm"
    platform_version: Literal["openai-v1"] = "openai-v1"
    models: list[str] = Field(min_length=1)
    docker: DockerTarget | None = None


class Inventory(Strict):
    resources: dict[str, NXDevice | VLLMService]

    @field_validator("resources", mode="before")
    @classmethod
    def platforms(cls, value):
        if not isinstance(value, dict) or not value:
            raise ValueError("nonempty inventory required")
        return {
            key: (
                NXDevice if item.get("platform") == "nxos" else VLLMService
            ).model_validate(item)
            for key, item in value.items()
        }

    @classmethod
    def load(cls, path, base_dir):
        inventory = cls.model_validate(json.loads((base_dir / path).read_text()))
        for item in inventory.resources.values():
            for owner in (item, getattr(item, "docker", None)):
                if owner is None:
                    continue
                for field in ("ca_file", "known_hosts", "cert_file", "key_file"):
                    value = getattr(owner, field, None)
                    if value:
                        setattr(owner, field, str((base_dir / value).resolve()))
        return inventory


class InventorySettings(Strict):
    inventory_path: str
    severity_map: dict[str, str] = Field(default_factory=dict)
    priority_map: dict[str, str] = Field(default_factory=dict)


class ToolSettings(InventorySettings):
    enable_changes: bool = False


def sanitized(value, secrets=()):
    """Bound native details; drop credential/log/prompt/environment fields recursively."""
    forbidden = {
        "password",
        "passwd",
        "secret",
        "api_key",
        "apikey",
        "access_token",
        "refresh_token",
        "authorization",
        "credentials",
        "private_key",
        "env",
        "environment",
        "logs",
        "log",
        "prompt",
        "messages",
        "endpoint",
        "username_env",
        "password_env",
        "token_env",
    }
    if isinstance(value, dict):
        return {
            k: sanitized(v, secrets)
            for k, v in value.items()
            if k.lower().replace("-", "_") not in forbidden
        }
    if isinstance(value, list):
        return [sanitized(v, secrets) for v in value[:128]]
    if isinstance(value, str):
        for secret in secrets:
            if secret:
                value = value.replace(secret, "[redacted]")
        value = re.sub(
            r"(?i)(bearer\s+|(?:password|api[_-]?key|token|secret)\s*[:=]\s*)\S+",
            r"\1[redacted]",
            value,
        )
        return value[:2048]
    return value


def credential_values(inventory):
    values = []
    for item in inventory.resources.values():
        for field in ("username_env", "password_env", "token_env"):
            ref = getattr(item, field, None)
            if ref and os.environ.get(ref):
                values.append(os.environ[ref])
    return values


def step(name, resource, start, status="succeeded", facts=None):
    return ProcedureStep(
        step_id=name,
        target=[resource],
        started_at=start,
        finished_at=now(),
        status=status,
        raw_output=facts,
    )


def network_error(exc):
    """Classify transport failures without importing an optional client at module load."""
    return isinstance(exc, (OSError, TimeoutError)) or type(exc).__module__ in {
        "httpx",
        "httpcore",
    }
