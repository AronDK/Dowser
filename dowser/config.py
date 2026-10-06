"""Explicit trusted factories, dependency graph, and reverse lifecycle cleanup."""

import importlib
import inspect
import json
import math
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from . import INTERFACE_VERSION
from .contracts import INTERFACES, OPTIONAL_INTERFACES, AppContext, ToolSpec
from .models import Boundary, Limits
from .runtime import PersistentWorker, WorkerComponent, bounded_call


class FactoryReference(Boundary):
    factory: str
    settings: dict[str, Any] = Field(default_factory=dict)


class IntakeSettings(Boundary):
    pending_capacity: int = Field(default=32, ge=1)
    normalization_seconds: float = Field(default=15, gt=0, allow_inf_nan=False)
    scheduler_seconds: float = Field(default=15, gt=0, allow_inf_nan=False)
    checkpoint_attempts: int = Field(default=3, ge=1)
    checkpoint_seconds: float = Field(default=5, gt=0, allow_inf_nan=False)
    checkpoint_retry_delays: list[float] = Field(default_factory=lambda: [0.5, 1.0])

    @model_validator(mode="after")
    def valid_delays(self):
        if any(
            not math.isfinite(value) or value < 0
            for value in self.checkpoint_retry_delays
        ):
            raise ValueError("checkpoint delays must be finite and nonnegative")
        return self


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
    incident_source: FactoryReference | None = None
    normalizer: FactoryReference | None = None
    scheduler: FactoryReference | None = None
    intake: IntakeSettings = Field(default_factory=IntakeSettings)


SLOTS = tuple(k for k in INTERFACES if k != "tool_plugin")
INTAKE_SLOTS = ("incident_source", "normalizer", "scheduler")
EXECUTION_SLOTS = tuple(slot for slot in SLOTS if slot not in INTAKE_SLOTS)


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
    methods = {**INTERFACES[subsystem], "aclose": 0}
    methods.update(
        (name, arity)
        for name, arity in OPTIONAL_INTERFACES.get(subsystem, {}).items()
        if getattr(cls, name, None) is not None
    )
    for name, arity in methods.items():
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
    refs = {slot: getattr(config, slot) for slot in SLOTS}
    if config.incident_source is not None:
        refs["normalizer"] = refs["normalizer"] or FactoryReference(
            factory="dowser.intake:compatibility_normalizer"
        )
        refs["scheduler"] = refs["scheduler"] or FactoryReference(
            factory="dowser.intake:fifo_scheduler"
        )
    loaded = {
        slot: load_factory(ref, slot) for slot, ref in refs.items() if ref is not None
    }
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

    for slot in loaded:
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
            if not set(self.requested) & set(INTAKE_SLOTS):
                if needed & set(INTAKE_SLOTS):
                    raise ConfigurationError(
                        "requested execution services cannot depend on intake services"
                    )
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
                if slot in INTAKE_SLOTS:
                    seconds = {
                        "incident_source": self.config.intake.checkpoint_seconds,
                        "normalizer": self.config.intake.normalization_seconds,
                        "scheduler": self.config.intake.scheduler_seconds,
                    }[slot]
                    service = WorkerComponent(PersistentWorker(slot), seconds)
                    self.initialized.append(service)
                    component = await service.construct(fn, settings, ctx)
                else:
                    component = fn(settings, ctx)
                    if inspect.isawaitable(component):
                        component = await component
                    service = component
                    self.initialized.append(component)
                if not isinstance(component, fn.component_type):
                    raise ConfigurationError(
                        f"{slot}: factory returned unexpected component type"
                    )
                check_component(type(component), slot)
                self.services[slot] = service
        except BaseException:
            await self.close()
            raise
        return self

    async def close(self):
        errors = []
        while self.initialized:
            component = self.initialized.pop()
            try:
                if isinstance(component, WorkerComponent):
                    await component.aclose()
                else:
                    await bounded_call(component.aclose, seconds=2)
            except BaseException as exc:
                errors.append(type(exc).__name__)
        if errors:
            raise RuntimeError("component cleanup failed: " + "; ".join(errors))

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
