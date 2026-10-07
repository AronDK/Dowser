"""Typed failure details without arbitrary exception text or response bodies."""

from typing import Literal
from uuid import UUID

from pydantic import ConfigDict, Field

from .models import Boundary


class FailureDetail(Boundary):
    model_config = ConfigDict(strict=True, extra="forbid", validate_assignment=True)

    error_type: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,79}$")
    code: str = Field(pattern=r"^[a-z][a-z0-9_]{0,79}$")
    category: Literal[
        "configuration",
        "http",
        "transport",
        "response",
        "timeout",
        "runtime",
        "accounting",
    ]
    stage: Literal[
        "preparation",
        "credentials",
        "http",
        "json",
        "validation",
        "decision",
        "accounting",
    ]
    retryable: bool = False
    http_status: int | None = Field(default=None, ge=100, le=599)
    cause_type: str | None = Field(
        default=None, pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,79}$"
    )
    retry_exhausted: bool = False
    retry_deadline_exceeded: bool = False
    attempt: int | None = Field(default=None, ge=1)
    request_id: UUID | None = None
    provider_request_id: UUID | None = None
    probability_sum: float | None = Field(default=None, allow_inf_nan=False)
    chosen_probability: float | None = Field(default=None, allow_inf_nan=False)
    maximum_probability: float | None = Field(default=None, allow_inf_nan=False)
    expected_options: int | None = Field(default=None, ge=0)
    provided_options: int | None = Field(default=None, ge=0)
    schema_error_types: list[str] = Field(default_factory=list)
    schema_error_fields: list[
        Literal[
            "type",
            "choice",
            "probabilities",
            "confidence",
            "input_tokens",
            "output_tokens",
            "response_extra",
        ]
    ] = Field(default_factory=list)


class DiagnosticError(RuntimeError):
    def __init__(
        self,
        message,
        *,
        code="extension_error",
        category="runtime",
        stage="decision",
        **details,
    ):
        super().__init__(message)
        self.detail = FailureDetail(
            error_type=type(self).__name__,
            code=code,
            category=category,
            stage=stage,
            **details,
        )


def failure_details(error):
    if isinstance(error, DiagnosticError):
        return error.detail.model_dump(mode="json", exclude_none=True)
    accounting = type(error).__name__ in {"SpendingLimit", "CallLimit"}
    return FailureDetail(
        error_type=type(error).__name__,
        code="spending_limit"
        if type(error).__name__ == "SpendingLimit"
        else "call_limit"
        if type(error).__name__ == "CallLimit"
        else "deadline_exceeded"
        if isinstance(error, TimeoutError)
        else "extension_error",
        category="accounting"
        if accounting
        else "timeout"
        if isinstance(error, TimeoutError)
        else "runtime",
        stage="accounting" if accounting else "decision",
    ).model_dump(mode="json", exclude_none=True)
