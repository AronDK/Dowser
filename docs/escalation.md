# Escalation control and exit penalties

`escalate` means stop the incident, record its outcome, and return control to the
caller. It does not contact a human or invoke a second model. This is a supplied
choice in the adapter, rather than a required TypeSafe API feature.

ITBench-AA now disables model-selected escalation by default. The choice is
absent from the API criteria, and a response selecting it is rejected. Provider
failures, unavailable actions, context failures and watchdogs still terminate
execution. Existing benchmark results remain unchanged; these settings require
a new campaign/profile, and are recorded in its manifest.

## Configure a Jev provider

Use `allow_escalation: false` in provider settings to remove the option entirely.
For a configurable penalty, use the provider-owned plugin:

```json
{
  "factory": "plugins.jev:decision_provider",
  "settings": {
    "allow_escalation": true,
    "escalation_policy": {
      "factory": "plugins.escalation:escalation_policy",
      "settings": {"enabled": true, "penalty": 3.0}
    }
  }
}
```

`penalty` must be finite and at least 1. A value of 1 preserves the native choice,
including cases where rounded probabilities disagree with it. Higher values
make escalation harder: when Jev selects escalation, the plugin divides its raw
option score by the penalty and compares it with the best scored allowed action
or wait option. If that alternative scores strictly higher, it is selected;
ties preserve escalation. For example, escalation 0.6 with penalty 3 becomes
0.2; an investigation action scored 0.3 is preferred. Native action or wait
choices are never changed by this policy.

The policy adjustment runs in code after inference. It is not a TypeSafe model
parameter, and its adjusted scores are not calibrated probabilities. Original
choice, probabilities, confidence, usage and the final decision are retained in
decision metadata. Confidence continues to describe the native model response.
Action permissions, scope and verification still apply to adjusted selections.

Both `allow_escalation` and the plugin's `enabled` setting must permit escalation.
Generic Jev defaults remain enabled with penalty 1 for compatibility. A custom
trusted policy factory can implement the `decision_policy` extension: expose
`controls` with `enabled` and `penalty`, and async
`apply(request, decision, alternatives)` and `aclose()`. Alternatives map provider
labels to authorized `DecisionResult` objects. Dependencies must be declared by
the enclosing provider factory. Configuration validation does not construct the
plugin; its lifecycle belongs to its provider. No extra mandatory slot is added.

## Benchmark profiles

The default profile removes escalation. To explicitly enable a penalized exit in
a **new** campaign:

```sh
uv run dowser-bench run --campaign penalized-exit --phase pilot \
  --allow-model-escalation --escalation-penalty 3
```

Use the same flags when resuming that campaign. Configuration fingerprints prevent
silently changing the policy in a paid run. The prior native pilot's 10% accuracy
belongs to its original escalation-enabled profile and does not validate the new
profile. No paid rerun is implied by changing configuration.
