"""Optional, source-grounded assessment coaching for the snapshot benchmark."""

from dowser.assessments import AssessmentQuestion, AssessmentRequest, AssessmentResult
from dowser.bench_data import dumps
from dowser.memory import compact, digest
from dowser.models import InvestigationMemory

QUESTION_VERSION = "causal-assessment/1"


class AssessmentCoach:
    def __init__(self, store, registry, threshold=0.5):
        self.store = store
        self.registry = registry
        self.threshold = threshold
        self.cache = {}

    async def enrich(self, request, provider):
        plugin = next(
            p for p in self.registry.plugins if hasattr(p, "knowledge_version")
        )
        working = max(
            (o for o in request.state.observations if o.kind == "investigation"),
            key=lambda o: o.payload["revision"],
        ).payload
        focused = working["focus"]
        entities = (
            [focused]
            if focused
            else list(
                dict.fromkeys(
                    c.args.get("entity")
                    for c in request.candidates
                    if c.args.get("entity")
                )
            )[:6]
        )
        hypotheses, relevance = [], {}
        for entity in entities:
            semantic = plugin.knowledge_version(entity)
            alert_digest = digest(request.state.alert)
            cache_key = digest([alert_digest, entity, semantic, QUESTION_VERSION])
            result = self.cache.get(cache_key)
            if result is None and self.store is not None:
                lookup = getattr(self.store, "assessment_cache", None)
                if lookup:
                    value = await lookup(request.incident_id, cache_key)
                    if value:
                        result = AssessmentResult.model_validate(value)
            facts = [
                f
                for f in (request.memory.facts if request.memory else [])
                if f.get("payload", {}).get("entity") == entity
            ]
            evidence = working["evidence"]["records"] if entity == focused else []
            refs = sorted(
                {ref for fact in facts for ref in fact.get("evidence_refs", [])}
            )
            if result is None:
                alerts = request.state.alert.get("public_alerts", [])
                # Bounds are explicit; absent/abridged evidence never implies sufficiency.
                state = {
                    "entity": entity,
                    "alert_digest": alert_digest,
                    "alerts": [compact(a, 300) for a in alerts[:6]],
                    "omitted_alerts": max(0, len(alerts) - 6),
                    "evidence": evidence,
                    "earlier_facts": [compact(f, 450) for f in facts[:3]],
                    "omitted_facts": max(0, len(facts) - 3),
                }
                questions = {
                    "relevance": AssessmentQuestion(
                        type="noul",
                        instructions="Could this observed entity plausibly contribute to the causal public alerts? Watchdog and InfoInhibitor are informational. Consider dependencies and alternatives; relevance does not establish a cause.",
                    )
                }
                if focused:
                    questions.update(
                        {
                            "sufficiency": AssessmentQuestion(
                                type="noul",
                                instructions="Is the cited, inspected evidence sufficient to assess this entity as an independent upstream cause? Missing, partial or abridged evidence and plausible alternatives reduce sufficiency. Do not infer absent observations.",
                            ),
                            "support": AssessmentQuestion(
                                type="noul",
                                instructions="Does the cited evidence support this entity as an independent upstream contributing cause? Retain uncertainty and alternative explanations; temporal association alone is insufficient.",
                            ),
                            "refutation": AssessmentQuestion(
                                type="noul",
                                instructions="Does the cited evidence refute this entity as an independent upstream cause, or support it being a downstream symptom? Contradictory supporting evidence must remain visible.",
                            ),
                        }
                    )
                if len(dumps(state).encode()) > 8192:
                    raise ValueError("required assessment evidence exceeds 8 KiB")
                result = await provider.assess(
                    AssessmentRequest(
                        incident_id=request.incident_id,
                        state=state,
                        questions=questions,
                    )
                )
                if self.store is not None:
                    await self.store.append(
                        request.incident_id,
                        "som_assessment",
                        {
                            "cache_key": cache_key,
                            "entity": entity,
                            "question_version": QUESTION_VERSION,
                            "alert_digest": alert_digest,
                            "semantic_digest": semantic,
                            "result": result.model_dump(mode="json"),
                        },
                    )
                self.cache[cache_key] = result
            values = {name: answer.noul for name, answer in result.answers.items()}
            relevance[entity] = values["relevance"]
            prior = next(
                (
                    h
                    for h in (request.memory.hypotheses if request.memory else [])
                    if h.get("entity") == entity
                ),
                {},
            )
            hypothesis = {
                "entity": entity,
                "source": "SOM assessment; interpretation of cited evidence",
                "assessment": values,
                "display_threshold": self.threshold,
                "supporting_facts": refs
                if values.get("support", 0) > self.threshold
                else [],
                "refuting_facts": refs
                if values.get("refutation", 0) > self.threshold
                else [],
                "contradictory": values.get("support", 0) > self.threshold
                and values.get("refutation", 0) > self.threshold,
                "coverage": sorted(
                    set(prior.get("coverage", []))
                    | {f["payload"]["kind"] for f in facts}
                ),
                "outcomes": prior.get("outcomes", {}),
                "unresolved_questions": [
                    name
                    for name, value in values.items()
                    if 0.4 <= value <= 0.6
                    or name == "sufficiency"
                    and value <= self.threshold
                ],
                "alert_digest": alert_digest,
                "semantic_digest": semantic,
                "question_version": QUESTION_VERSION,
            }
            hypotheses.append(hypothesis)
            if self.store is not None:
                await self.store.append(
                    request.incident_id,
                    "hypothesis_assessment",
                    {"entity": entity, "hypothesis": hypothesis},
                )
        view = request.model_copy(deep=True)
        view.memory = view.memory or InvestigationMemory()
        view.memory.hypotheses = hypotheses[:8]
        while (
            len(view.memory.model_dump_json().encode()) > 8192
            and view.memory.hypotheses
        ):
            view.memory.hypotheses.pop()
        while (
            view.memory.hypotheses
            and not provider.provider.context_check(
                provider.provider.prepare(view)[0]
            ).fits
        ):
            view.memory.hypotheses.pop()
        # Ranking changes presentation only. Every alternative retains its definition and permission checks.
        view.candidates.sort(
            key=lambda c: relevance.get(c.args.get("entity"), 0.5), reverse=True
        )
        return view
