"""Explicit trusted factories, dependency graph, and reverse lifecycle cleanup."""

import importlib
import inspect
import json
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

from pydantic import BaseModel, Field

from . import INTERFACE_VERSION
from .contracts import INTERFACES, AppContext, ToolSpec
from .models import Boundary, Limits
from .runtime import bounded_call


class FactoryReference(Boundary):
    factory: str
    settings: dict[str, Any] = Field(default_factory=dict)


class Configuration(Boundary):
    schema_version: Literal["1"] = "1"
    limits: Limits = Field(default_factory=Limits)
    event_store: FactoryReference
    tool_registry: FactoryReference
    context_builder: FactoryReference
    decision_provider: FactoryReference
    validation_policy: FactoryReference
    executor: FactoryReference
    verifier: FactoryReference
    incident_loop: FactoryReference


SLOTS = tuple(k for k in INTERFACES if k != "tool_plugin")


class ConfigurationError(ValueError):
    """A core-generated diagnostic safe to display without configuration values."""


def check_component(cls: type, subsystem: str) -> None:
    if (
        not isinstance(cls, type)
        or getattr(cls, "interface_version", None) != INTERFACE_VERSION
    ):
        raise ConfigurationError(
            f"{subsystem}: incompatible component interface version"
        )
    for name, arity in {**INTERFACES[subsystem], "aclose": 0}.items():
        method = getattr(cls, name, None)
        if not inspect.iscoroutinefunction(method):
            raise ConfigurationError(f"{subsystem}.{name} must be asynchronous")
        try:
            inspect.signature(method).bind(None, *([None] * arity))
        except TypeError as exc:
            raise ConfigurationError(
                f"{subsystem}.{name}: incompatible signature"
            ) from exc


def load_factory(ref: FactoryReference, subsystem: str):
    if ref.factory.count(":") != 1:
        raise ConfigurationError("factory reference must be module:factory")
    module, name = ref.factory.split(":")
    try:
        fn = getattr(importlib.import_module(module), name)
    except (ImportError, AttributeError) as exc:
        raise ConfigurationError(
            f"{subsystem}: factory module or callable could not be found"
        ) from exc
    if not callable(fn) or getattr(fn, "interface_version", None) != INTERFACE_VERSION:
        raise ConfigurationError(
            f"{ref.factory}: incompatible factory interface version"
        )
    if getattr(fn, "subsystem", None) != subsystem:
        raise ConfigurationError(
            f"{ref.factory}: wrong subsystem (expected {subsystem})"
        )
    check_component(getattr(fn, "component_type", None), subsystem)
    if not isinstance(getattr(fn, "settings_model", None), type) or not issubclass(
        fn.settings_model, BaseModel
    ):
        raise ConfigurationError("factory must declare a Pydantic settings model")
    if (
        not isinstance(getattr(fn, "dependencies", None), tuple)
        or any(not isinstance(dep, str) for dep in fn.dependencies)
        or len(set(fn.dependencies)) != len(fn.dependencies)
    ):
        raise ConfigurationError("dependencies must be a unique tuple")
    try:
        inspect.signature(fn).bind(None, None)
    except (ValueError, TypeError) as exc:
        raise ConfigurationError(
            "factory must accept settings and application context"
        ) from exc
    settings = fn.settings_model.model_validate(ref.settings)
    if subsystem == "tool_plugin":
        tools = getattr(fn.component_type, "tools", None)
        if not isinstance(tools, tuple) or not tools:
            raise ConfigurationError(
                "plugin class must declare a nonempty tuple of tools"
            )
        names = set()
        for tool in tools:
            if not isinstance(tool, ToolSpec) or not tool.name or tool.name in names:
                raise ConfigurationError("invalid or duplicate plugin tool declaration")
            names.add(tool.name)
            if (
                not isinstance(tool.argument_model, type)
                or not issubclass(tool.argument_model, BaseModel)
                or tool.argument_model.model_config.get("strict") is not True
                or tool.argument_model.model_config.get("extra") != "forbid"
            ):
                raise ConfigurationError(
                    "tool argument schemas must be strict and forbid extras"
                )
            if (
                tool.effect not in {"read_only", "change"}
                or tool.kind not in {"observation", "remediation", "recovery"}
                or not tool.platforms
                or not tool.verification_hooks
            ):
                raise ConfigurationError("invalid plugin capability declaration")
    if subsystem == "tool_registry" and hasattr(settings, "plugins"):
        for plugin in settings.plugins:
            loaded, _ = load_factory(plugin, "tool_plugin")
            if not set(loaded.dependencies) <= set(fn.dependencies):
                raise ConfigurationError(
                    "plugin requests services not declared by registry factory"
                )
    return fn, settings


def read_config(path: Path) -> Configuration:
    config = Configuration.model_validate(json.loads(path.read_text()))
    if config.schema_version != "1":
        raise ConfigurationError("unsupported configuration version")
    return config


def validate_config(config: Configuration):
    loaded = {slot: load_factory(getattr(config, slot), slot) for slot in SLOTS}
    order: list[str] = []
    visiting: set[str] = set()

    def visit(slot):
        if slot in order:
            return
        if slot in visiting:
            raise ConfigurationError(f"factory dependency cycle at {slot}")
        visiting.add(slot)
        for dependency in loaded[slot][0].dependencies:
            if dependency not in loaded:
                raise ConfigurationError(f"{slot}: missing service {dependency}")
            visit(dependency)
        visiting.remove(slot)
        order.append(slot)

    for slot in SLOTS:
        visit(slot)
    return loaded, order


class Application:
    def __init__(
        self,
        config: Configuration,
        base_dir: Path,
        requested: tuple[str, ...] | None = None,
    ):
        self.config = config
        self.base_dir = base_dir
        self.requested = requested
        self.services: dict[str, Any] = {}
        self.initialized: list[Any] = []

    async def __aenter__(self):
        loaded, order = validate_config(self.config)
        if self.requested is not None:
            needed = set()

            def include(slot):
                if slot not in loaded:
                    raise ConfigurationError("requested service is not configured")
                if slot in needed:
                    return
                needed.add(slot)
                for dep in loaded[slot][0].dependencies:
                    include(dep)

            for slot in self.requested:
                include(slot)
            order = [slot for slot in order if slot in needed]
        try:
            for slot in order:
                fn, settings = loaded[slot]
                ctx = AppContext(
                    MappingProxyType(
                        {dep: self.services[dep] for dep in fn.dependencies}
                    ),
                    self.config.limits.model_copy(deep=True),
                    self.base_dir,
                )
                # Factories are application code; constructors own cleanup if they raise.
                component = fn(settings, ctx)
                if inspect.isawaitable(component):
                    component = await component
                self.initialized.append(component)
                if not isinstance(component, fn.component_type):
                    raise ConfigurationError(
                        f"{slot}: factory returned unexpected component type"
                    )
                check_component(type(component), slot)
                self.services[slot] = component
        except BaseException:
            await self.close()
            raise
        return self

    async def close(self):
        errors = []
        while self.initialized:
            component = self.initialized.pop()
            try:
                await bounded_call(component.aclose, seconds=2)
            except BaseException as exc:
                errors.append(type(exc).__name__)
        if errors:
            raise RuntimeError("component cleanup failed: " + "; ".join(errors))

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
