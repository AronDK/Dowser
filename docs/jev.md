# Jev decision provider

Install the optional dependencies:

```sh
uv sync --locked --extra jev
```

Store the TypeSafe key in a private `.env` in the working directory:

```dotenv
TYPESAFE_API_KEY=your-key
```

`.env` and `.env.*` are gitignored; `.env.example` is a public blank template.
Give the private file mode `0600`. An existing `TYPESAFE_API_KEY` environment
variable takes precedence. Dotenv interpolation is disabled. The provider reads
credentials only when making a decision, never during configuration validation.

Replace the decision-provider reference in your eight-slot configuration:

```json
{
  "decision_provider": {
    "factory": "plugins.jev:decision_provider",
    "settings": {
      "model": "jev-1.13.0",
      "credential_env": "TYPESAFE_API_KEY",
      "env_file": ".env",
      "timeout_seconds": 10,
      "max_retries": 2,
      "retry_initial_seconds": 0.5,
      "retry_max_seconds": 5
    }
  }
}
```

Then run the existing `dowser validate` and `dowser run`/`serve` commands. The
provider automatically loads the configured env file relative to the application
working directory; sourcing it into the shell is optional. Set `env_file: null`
when credentials must come exclusively from the process environment.

The adapter uses the native [TypeSafe API](https://docs.typesafe.ai/api), making
one Choice over the complete candidate snapshot plus wait/escalate. Neutral option
labels map back to supplied candidate IDs. Candidate definitions, instructions,
resources, desired state and observations stay in the prepared context. Probabilities,
confidence, returned model version, token usage and request latency are recorded in
`score_metadata`; confidence does not authorize execution or resolution. The existing
policy, executor and verifier retain their responsibilities.

`capabilities()` advertises one decision per round. The default maximum is 253
candidates, leaving two of the API's 255 Choice options for wait and escalation.
Independent questions in a Jev request do not describe an ordered execution plan.
Unknown choices, malformed distributions, model-version mismatches and transport
failures are rejected. HTTP 408, 429 and 5xx responses (including 529), and
transport errors, are retried at most twice by default, following TypeSafe SDK
retry defaults. Backoff starts at 0.5 seconds, doubles up to 5 seconds, and applies
25% jitter. Numeric/HTTP-date `Retry-After` and `Retry-After-ms` headers are honored.
The 10-second provider deadline includes every attempt and backoff wait; required
waits beyond it stop retries. Permanent HTTP errors and malformed responses are
not retried. There is no fallback provider. Exhausted retries follow the harness
escalation path.

`check_context()` performs no network access or inference. It uses a conservative
UTF-8 byte budget over the serialized request plus configurable headroom, capped
at the deployment's 32,000-token single-question limit. This is explicitly **not an
exact token count** and may reject requests that would fit the model. The provider
refuses to silently shorten the candidate snapshot. HTTP clients are created and
closed inside each calling worker loop. HTTPS is required; redirects and ambient
proxy settings are disabled.

Run the offline provider checks:

```sh
uv run --extra jev python -m unittest discover -s tests -v
```

The live local-canary test used during setup is private under `.local/`; it verifies
one read-only action through selection, policy, execution, parsing, verification and
durable termination. Its config, incident, SQLite history and sanitized result are
kept there. It is a connectivity/integration check, not a benchmark evaluation.

## Persistent diagnostics

Failures carry a typed `failure` object with a code, stage, category, HTTP status,
transport subtype, attempt number and valid UUID request IDs when available.
Response validation reports the failing check, schema error types, option counts
and probability totals or comparisons. Arbitrary exception messages, response
bodies, headers and credentials are excluded. Failed HTTP attempts emit JSON
diagnostics on stderr. Final provider failures persist in the incident history;
recovered retry failures persist in successful decision metadata.

The benchmark additionally stores every HTTP attempt and its diagnostics in the
spending ledger and exports `diagnostics.jsonl`. Each attempt has its own durable
64,000-token reservation. Valid usage is reconciled even when a response's decision
is rejected; unknown usage retains the reservation. Retries retain individual usage and cost records; new campaigns have no
call-count or monetary ceiling. See the [evaluation guide](itbench-aa.md).

The generic adapter defaults to `strict_probabilities: true` and a
`probability_sum_tolerance` of 0.0001. The benchmark explicitly sets strict mode
to false and tolerance to 0.02, following the official SDK's use of the returned
`choice` field. Near-one distributions are proportionally normalized for
reporting. Other sum deviations and ranking disagreements are warnings; the
supplied choice and raw probabilities are preserved. Unknown labels, missing
options, nonfinite/out-of-range values, malformed fields and model mismatches
remain fatal. Scores never grant execution authority.

The SDK source used to verify this behavior is revision
`f078f1e208a0d885154dc758344ae4fce77ac168` of
[typesafe-ai/typesafe-sdk-python](https://github.com/typesafe-ai/typesafe-sdk-python).

The configured account caps are 80 requests/s and 100,000 tokens/s. Client
admission uses a rolling one-second window with thread-safe state across worker
loops. Each attempt reserves the documented 64,000-token input ceiling plus
2,048 output tokens of headroom; valid reported input-plus-output usage replaces
the rate reservation. Unknown usage keeps it for the remainder of the rolling
window. Rate waits count toward the provider deadline. The benchmark shares one
window across all trials; financial reservations remain separate and durable.
