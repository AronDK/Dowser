"""Optional typed SOM assessments: opinions never confer execution authority."""

from typing import Literal

from pydantic import Field, JsonValue, model_validator

from .models import Boundary, new_id


class AssessmentQuestion(Boundary):
    type: Literal["choice", "noul", "score"]
    instructions: str = Field(min_length=1)
    criteria: dict[str, JsonValue] | list[JsonValue] | None = None

    @model_validator(mode="after")
    def shape(self):
        if self.type == "choice" and (
            not isinstance(self.criteria, dict) or not 1 <= len(self.criteria) <= 253
        ):
            raise ValueError("Choice requires 1–253 named criteria")
        if self.type == "score" and (
            not isinstance(self.criteria, list) or not 1 <= len(self.criteria) <= 253
        ):
            raise ValueError("Score requires 1–253 ordered levels")
        if (
            self.type == "noul"
            and self.criteria is not None
            and (
                not isinstance(self.criteria, dict)
                or not set(self.criteria) <= {"true", "false"}
            )
        ):
            raise ValueError("Noul criteria must describe true and false")
        return self


class AssessmentRequest(Boundary):
    id: str = Field(default_factory=new_id)
    incident_id: str = Field(min_length=1)
    state: dict[str, JsonValue]
    questions: dict[str, AssessmentQuestion] = Field(min_length=1)


class ChoiceAssessment(Boundary):
    type: Literal["choice"]
    choice: str
    probabilities: dict[str, float]
    confidence: float = Field(ge=0, le=1, allow_inf_nan=False)


class NoulAssessment(Boundary):
    type: Literal["noul"]
    noul: float = Field(ge=0, le=1, allow_inf_nan=False)


class ScoreAssessment(Boundary):
    type: Literal["score"]
    score: float = Field(ge=0, allow_inf_nan=False)
    confidence: float = Field(ge=0, le=1, allow_inf_nan=False)
    legend: dict[str, JsonValue]
    probabilities: dict[str, float]


class AssessmentResult(Boundary):
    answers: dict[str, ChoiceAssessment | NoulAssessment | ScoreAssessment]
    score_metadata: dict[str, JsonValue] = Field(default_factory=dict)
