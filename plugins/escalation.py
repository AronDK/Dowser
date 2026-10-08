"""Configurable escalation penalty; raw SOM decisions remain auditable."""

from pydantic import Field

from dowser.contracts import Component, factory
from dowser.models import Boundary


class Settings(Boundary):
    enabled: bool = True
    penalty: float = Field(default=1.0, ge=1, allow_inf_nan=False)


class Policy(Component):
    controls = Settings()

    def __init__(self, settings):
        self.controls = settings

    async def apply(self, request, decision, alternatives):
        result = decision.model_copy(deep=True)
        probabilities = result.score_metadata.get(
            "raw_probabilities", result.score_metadata.get("probabilities", {})
        )
        audit = {
            "enabled": self.controls.enabled and "escalate" in probabilities,
            "penalty": self.controls.penalty,
            "native_operation": decision.operation,
            "native_candidate_id": decision.candidate_id,
            "adjusted": False,
            "confidence_applies_to": "native_model_response",
        }
        result.score_metadata["escalation_policy"] = audit
        if decision.operation != "escalate":
            return result
        if not self.controls.enabled:
            raise ValueError("disabled escalation returned by provider")
        if self.controls.penalty == 1:
            return result  # Preserve the native choice, even if scores disagree.
        if "escalate" not in probabilities:
            raise ValueError("escalation penalty requires option probabilities")
        allowed = {c.id for c in request.candidates}
        scored = []
        for label, alternative in alternatives.items():
            if (
                alternative.operation == "select"
                and alternative.candidate_id not in allowed
            ):
                raise ValueError("penalty alternative outside candidate scope")
            if alternative.operation in {"select", "wait"} and label in probabilities:
                scored.append((label, probabilities[label], alternative))
        if not scored:
            raise ValueError("escalation penalty has no scored alternatives")
        label, score, alternative = max(scored, key=lambda item: item[1])
        adjusted = probabilities["escalate"] / self.controls.penalty
        audit.update(
            native_escalation_score=probabilities["escalate"],
            penalized_escalation_score=adjusted,
            best_alternative=label,
            best_alternative_score=score,
        )
        if adjusted < score:
            changed = alternative.model_copy(deep=True)
            changed.reason = "Escalation penalty selected a supplied alternative"
            audit.update(
                adjusted=True,
                selected_operation=changed.operation,
                selected_candidate_id=changed.candidate_id,
            )
            changed.score_metadata = result.score_metadata
            return changed
        return result


@factory(subsystem="decision_policy", component_type=Policy, settings_model=Settings)
def escalation_policy(settings, context):
    return Policy(settings)
