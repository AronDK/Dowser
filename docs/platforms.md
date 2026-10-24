# NX-OS, vLLM and Kubernetes plugins

The installed `plugins` package provides eight platform factories. Reuse Dowser's registry,
policy, executor, verifier, loop, SQLite store, JSONL source and scheduler around them.

| Factory | Role |
| --- | --- |
| `plugins.nxos:tool_plugin` | NX-OS diagnostics and restricted interface fixes |
| `plugins.nxos:normalizer` | NX-OS-only incident intake |
| `plugins.vllm:decision_provider` | SOM adapter with deployment-specific decision capacity |
| `plugins.vllm:tool_plugin` | Managed vLLM diagnostics and Docker procedures |
| `plugins.vllm:normalizer` | vLLM-only incident intake |
| `plugins.k8s:tool_plugin` | Kubernetes diagnostics and scoped deployment remediation |
| `plugins.k8s:normalizer` | Kubernetes-only incident intake |
| `plugins.normalizers:platform_normalizer` | Mixed-platform intake router |

## Install and configure

```sh
uv sync --locked --extra nxos --extra vllm --extra k8s
uv run dowser validate --config docs/examples/platforms.json
```

Copy [the configuration](examples/platforms.json), [inventory](examples/inventory.json)
and [JSONL input](examples/incidents.jsonl) to `.local/` and edit their paths/targets
for your deployment. Relative inventory paths resolve against Dowser's working
directory. CA, SSH known-hosts and Docker certificate paths resolve against that same
directory. Provider CA paths are also relative to the working directory. Keep private
inventory and credential files readable only by the service account. Credentials are
environment-variable references; their values are looked up only when used.

Inventory fixes the approved IDs, endpoints, subresources and Docker identities.
Constructors load local inventory but never open network clients. Constructor-free
`validate` checks settings and contracts without requiring the inventory file,
credentials or optional dependencies. It does not establish remote compatibility.
Execution clients/connections are created and closed within their worker call.
Missing optional dependencies produce a failed operation or provider escalation.

For SSH, change a device's transport fields to:

```json
{
  "transport": "ssh",
  "endpoint": "nexus.example.net",
  "port": 22,
  "known_hosts": ".local/nexus_known_hosts"
}
```

Supply an independently verified host key in that file. NX-API requires HTTPS with
verified TLS, using the system CA store or `ca_file`. Remote Docker requires HTTPS
with explicit `ca_file`, `cert_file`, `key_file`, and `unix_socket: null`. Local Docker
uses its configured absolute Unix socket. Neither transport follows redirects,
uses ambient proxy settings, falls back to another transport, or retries writes.

## Scoped incident input

Each JSONL line is a platform incident object, **not** a `RawIncident` envelope.
The bundled source wraps the line with `source_id` and a line-number event ID.
The example file contains both platforms. Use this format:

```json
{
  "event_id": "interface-alert-42",
  "platform": "nxos",
  "resource_id": "nexus-lab",
  "alert": {"type": "interface_down", "severity": "major", "priority": 3,
            "details": {"native_code": 17}},
  "scope": {"interfaces": ["Ethernet1/1"], "vlans": [200]},
  "desired_state": {"admin_state": "up", "oper_state": "up"}
}
```

IDs are stable `platform:sha256(source_id, identifier)` values. The identifier is
`incident_id`, then `event_id`, then the source event ID. Put native event IDs on
lines that may move within a file. Repeated terminal delivery follows the existing
checkpoint behavior and never replays actions. The router rejects unknown/conflicting
platforms, versions, target identities, and scope outside inventory. It preserves
native severity/priority/details after sanitization and supports optional severity
and priority maps. Credentials, environments, logs, prompts and endpoint fields are
excluded. Unknown top-level data is not copied into facts.

| Platform | Scope lists | Supported desired facts |
| --- | --- | --- |
| NX-OS | `interfaces`, `vlans`, `vrfs`, `prefixes` | `reachable`, `admin_state`, `oper_state`, `access_vlan`, `vlan_exists`, `vlan_state`, `route_present` |
| vLLM | `models` | `available: true` or `performance: true` |
| Kubernetes (`k8s`) | `namespaces`, `deployments`, `pods`, `services`, `nodes` | `reachable`, `node_ready`, `pod_phase`, `pod_ready`, `endpoints_ready`, `deployment_available`, `ready_replicas` |

Interface intent requires exactly one scoped physical Ethernet interface; VLAN intent
requires one VLAN; routing intent requires one IPv4 prefix and VRF. Service intent
requires one model alias. Scope omitted from an incident grants no subresource access.
NX-OS administrative/operational values are `up`/`down`; VLAN state is `active`/`suspend`.
Use one symptom category per incident: verification requires all its desired facts in
current evidence. This release cannot resolve a combined interface-and-routing incident
from separate tool executions.

`run` continues to accept the core `IncidentState` input. For platform tools, explicitly
set resource `platform`, `platform_version` (`10.4(x)` or `openai-v1`) and
`payload.scope`; `serve` performs the platform enrichment automatically.

## NX-OS tools

The parsing profile covers Nexus 9000 NX-OS 10.4(x), using fixed structured commands
and refusing unfamiliar versions/shapes. HTTPS uses `cli_show_array`; SSH uses
`show ... | json`. Both accept documented singleton/array row forms and use the same
facts parser. The offline fixtures are simulated; a new device build needs a
read-only compatibility check before enabling remediation.

| Tool | Arguments after `resource_id` | Result |
| --- | --- | --- |
| `nxos.inspect_device` | none | Reachability, chassis, NX-OS version |
| `nxos.inspect_interface` | `interface` | Admin/oper state, switchport mode/access VLAN, errors/discards, port-channel membership |
| `nxos.inspect_vlan` | `vlan` | Existence/state, membership filtered to incident interfaces |
| `nxos.inspect_routes` | `vrf`, `prefix` | Exact scoped IPv4 route presence |
| `nxos.ensure_interface_enabled` | `interface` | Live admin-down → `no shutdown`, followed by current interface evidence |
| `nxos.ensure_access_vlan` | `interface`, `vlan` | Existing active VLAN on an existing access port, followed by current interface evidence |

Only explicitly approved physical Ethernet ports can change. Protected interfaces and
port-channel members are rejected; access VLAN writes also reject trunks/routed ports.
Each mutation checks supported live version and interface/VLAN state inside execution,
records command acknowledgments, then reads post-change facts. Missing acknowledgments
produce unknown outcomes and terminate for reconciliation. A known rejected command
may leave partial effects. No automatic rollback, routing writes, startup-config save,
arbitrary CLI, or command replay is provided. Interface fixes affect running configuration.

[NX-API command types](https://www.cisco.com/c/en/us/td/docs/dcn/nx-os/nexus9000/104x/programmability/cisco-nexus-9000-series-nx-os-programmability-guide-104x/m-nx-api-developer-sandbox.html)
and [NX-OS JSON output](https://www.cisco.com/c/en/us/td/docs/dcn/nx-os/nexus9000/104x/programmability/cisco-nexus-9000-series-nx-os-programmability-guide-104x/m-n9k-nx-api-cli-101x.pdf)
describe the transport conventions. The parser recognizes `TABLE_interface` rows,
`oper_mode`/`access_vlan`, `TABLE_channel`/`TABLE_member`, `TABLE_vlanbrief`, and
`TABLE_vrf`/`TABLE_addrf`/`TABLE_prefix`; missing required tables do not imply absence.

## vLLM decision provider

The required profile supplies `model` (served alias), `context_window`, `output_tokens`,
`inference_timeout`, and deployment-validated `max_decisions_per_round`. There is no
core maximum: capacities 1, 2, 32 and 64 are covered offline. Choose capacity based on
validated model behavior. Serving concurrency and hardware batching do not set it.

`capabilities()` reports the profile without inference. `check_context()` uses
`/tokenize` with the exact prepared chat messages and generation prompt, reserving
output tokens. One `decide()` call makes one non-streaming `/v1/chat/completions`
request with a JSON schema whose candidate enum and array bound reflect the snapshot.
Local validation rejects malformed/truncated responses, unknown IDs, over-capacity
batches and misplaced wait/escalation decisions. Selected actions execute serially
through existing harness validation/policy/verification. Six registered tools, model
decision capacity and CLI commands inside one procedure are independent quantities.

Provider/tokenization failure escalates. There is no fallback inference or independent
remediation, even when the managed service is also the configured decision endpoint.
Separate decision and managed-service deployments if SOM availability must survive
managed-service failure.

[Serving APIs](https://docs.vllm.ai/en/latest/serving/online_serving/),
[tokenization protocol](https://docs.vllm.ai/en/v0.11.2/api/vllm/entrypoints/openai/protocol/),
and [structured outputs](https://docs.vllm.ai/en/latest/features/structured_outputs/)
provide the adapter APIs. The deployed model must support chat with a configured
template, `/tokenize`, and schema-constrained responses.

## Managed vLLM tools

| Tool | Arguments after `resource_id` | Result |
| --- | --- | --- |
| `vllm.inspect_service` | none | HTTP health/version and approved served models |
| `vllm.inspect_runtime` | none | Owned container identity/state, restart count, OOM flag |
| `vllm.sample_metrics` | `model` | Queue/cache/latency summaries over a recorded window |
| `vllm.probe_inference` | `model` | Health, expected model, fixed canary success and timing |
| `vllm.ensure_running` | `model` | Start an existing stopped owned container, then await readiness |
| `vllm.restart_service` | `model` | Restart a running owned container with fresh failure evidence, then await readiness |

Container IDs/names match exactly; Compose targets additionally require both exact
project/service labels. No container creation, image replacement, GPU changes, logs,
environment capture, shell execution, Kubernetes management or automatic tuning occurs.
Runtime reads do not verify model readiness. HTTP health alone cannot resolve availability.

Start/restart candidates require fresh scoped failure evidence and explicit availability
intent. Execution re-reads container ownership/state before changing anything. Readiness
allows 300 seconds and requires three consecutive checks five seconds apart; each check
requires health, the expected served model, and successful bounded canary inference.
An acknowledged mutation followed by readiness failure is partial and cannot pass
verification. Lost acknowledgment is unknown with no retry.

Metrics use the Prometheus parser and only retain `model_name`-scoped queue/cache gauges
and end-to-end latency sum/count. Defaults sample for ten seconds, every five seconds,
with at least three samples and one completed request. Latency is the window's mean,
not a percentile. Queue aggregates model engine gauges; cache reports maximum engine
utilization. Verification requires configured `metrics_thresholds` (`queue_max`,
`cache_max`, `latency_mean`), complete stable series, enough samples/requests, and a full
window. Missing metrics, engine-series changes, resets, or zero requests are inconclusive.
Other application text and model output are never persisted.

[Docker Engine v1.51](https://docs.docker.com/reference/api/engine/version/v1.51/) and
[vLLM metrics](https://docs.vllm.ai/en/latest/design/metrics/) define these interfaces.

## Kubernetes tools

| Tool | Arguments after `resource_id` | Result |
| --- | --- | --- |
| `k8s.inspect_cluster` | none | `/livez` reachability and validated `gitVersion` |
| `k8s.inspect_node` | `node` | Node `Ready` condition, pressure flags, unschedulable flag, kubelet version |
| `k8s.inspect_pod` | `namespace`, `pod` | Pod phase, `Ready` condition, total container restarts, `OOMKilled` flag, waiting reasons |
| `k8s.inspect_deployment` | `namespace`, `deployment` | Generation, observed generation, spec/ready/available/updated/unavailable replicas, `deployment_available` |
| `k8s.inspect_service` | `namespace`, `service` | Service type, ports, ready/not-ready endpoint addresses, `endpoints_ready` |
| `k8s.scale_deployment` | `namespace`, `deployment`, `replicas` | Scale a non-protected deployment within `max_replicas`, then await rollout readiness |
| `k8s.restart_deployment` | `namespace`, `deployment` | Patch `kubectl.kubernetes.io/restartedAt` after fresh failure evidence, then await rollout readiness |

Kubernetes targets require HTTPS with verified TLS (`ca_file` or system store) and either
`token_env` or paired `cert_file` and `key_file`. Protected namespaces (`protected_namespaces`)
and paused deployments reject mutations. Container environments, commands, logs and Secrets
are never read or stored. Mutations require an acknowledged HTTP `200` response and consecutive
rollout readiness checks (`readiness_checks`, default 3) before passing verification.

## Opting into changes

The [remediation example](examples/remediation.json) enables both plugin flags,
policy permission and one explicit change budget, with a 360-second tool ceiling and
900-second incident deadline. Keep the read-only configuration until inventory and
read-only compatibility checks have been verified for the deployment. The core still
counts one procedure as one action/change. Existing identical-attempt, freshness,
deadline and checkpoint semantics remain intact.

## Offline and optional lab verification

```sh
uv sync --locked --extra nxos --extra vllm
uv run python -m unittest discover -s tests -v
uv run python -m unittest discover -s .local/tests -v
uv build --out-dir .local/dist
```

The tracked platform suite blocks socket connections and simulates all transports.
The second command runs the existing private harness suite when present. Neither
requires devices, GPU, credentials, downloaded models or an inference service.

An optional lab smoke run uses a private copy of the default read-only config with an
available decision provider and incidents limited to approved resources. Run `validate`,
then `serve`, and inspect the recorded facts. NX-OS read tools and vLLM service/runtime/
metrics reads remain read-only; `probe_inference` runs the fixed bounded canary. Such
lab runs are opt-in and were not run as part of offline verification. Unknown NX-OS
JSON shapes fail closed and require profile/fixture review before any writes are enabled.
