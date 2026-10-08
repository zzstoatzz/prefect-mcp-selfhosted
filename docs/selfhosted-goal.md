# active goal

Operate a continuously, stochastically chaos-tested Prefect instance on isolated,
production-equivalent infrastructure with real application workflows. Preserve
production clients and data and keep measured resource, cost, latency, throughput,
and recovery baselines within explicit limits.

Required faults: API termination and response loss; PostgreSQL connection loss
and restart; Redis outage and replacement; event replay and checkpoint integrity;
worker reconnects. Verify completed work, preservation of acknowledged events,
replay-safe mutations, no duplicate side effects, bounded recovery, automatic
cleanup, one experiment at a time, reproducible reports, and stop conditions.
Enable the recurring runner only after the suite and guardrails are verified.

## extension requested 2026-10-08: our Prefect MCP

Fork the upstream Prefect MCP server for our self-hosted Prefect, connect it to
this assistant environment, and iteratively improve and deploy it based on real
operational use. This is part of the goal, not a replacement for chaos testing.

Completion requires:

- A maintained personal fork with an upstream remote and reproducible deployment.
- A distinct authenticated MCP connection whose identity proves it targets our
  hosted instance, with explicit separation from the isolated chaos instance.
- Useful end-to-end diagnostics for runs, logs, events, workers, deployments,
  and failures, verified against actual data rather than mocks alone.
- Replica-aware operational evidence: version skew, readiness versus throughput,
  backend recovery, and client-visible effects of rolling changes and outages.
- Bounded operational tools where needed, with explicit targets, safe mutation
  replay, clear failures, and verified effects.
- Integration of actual MCP calls into the chaos acceptance suite, including
  reconnect and replica failover, without lost acknowledgements or duplicate work.
- Iterative deployed improvements demonstrated by concrete operational tasks,
  under the same resource and performance guardrails as the original goal.

Home hosting via Tailscale remains a later migration option. Retiring the current
VM also requires accounting for its other hosted services and ingress routes.

## current connection

Codex's `prefect-selfhosted` entry now uses Streamable HTTP at
`http://100.96.216.23:9011/mcp`. This is a private tailnet endpoint on HeavyPad,
not an Internet listener. Tailscale encrypts the network transport. The server
binds only the tailnet IPv4 address and checks the socket peer's identity through
`tailscale whois`; only `zzstoatzz@github` is accepted. Forwarded identity headers
are ignored in this mode, and HTTP Prefect target/credential overrides are
rejected. Existing Tailscale Serve and Funnel routes were not changed.

The `prefect-mcp-selfhosted.service` user unit runs the fork from
`~/.local/share/prefect-mcp-selfhosted`, with a half-CPU and 384 MiB ceiling.
The launcher reads the worker's existing `~/.config/prod-worker/env`, verifies
the exact HTTPS API URL, and passes only the selected API credentials into the
server. No new secret is copied into Codex configuration. After the existing
credential store updates that file, restarting this service reloads it.

Update by pushing this branch, pulling the hosted checkout, running
`uv sync --frozen --no-dev`, installing the committed unit if it changed, then
restarting and verifying real MCP reads. The HTTP transport is stateless;
execution-plan mutations and Cloud tools are not registered. The original local
stdio launcher remains available as a fallback.

The first additional operational tool, `get_server_status`, samples health,
database readiness, `/admin/version` compatibility, and `/version` build identity.
It reports per-probe latency and failures, observed version skew, and explicitly
unknown replica coverage. Gateway sampling must not be treated as a replica
inventory or proof that work completes. It accepts no target URL; it uses the
configured connection.

Verified on 2026-10-08 through a real stdio MCP client: three production samples
returned readiness and build `993424a75aa8b2e46d3bff11dd6c81885ff9fb4d`, with
compatibility `3.8.2`. The new tool also runs against an actual ephemeral Prefect
API in the test suite. At that revision, all 186 tests, Ruff, and type checks passed.

Hosted revision `bbf52b5` passes all 189 tests, Ruff, and type checks. From the
laptop, actual HTTP MCP calls verified 13 tools, production identity, readiness,
build version, flow runs, logs, work pools, deployments, task runs, and events.
A target override returned HTTP 403. The running service was observed at
approximately 159 MiB with zero restarts. This demonstrates the deployed
connection, not refresh of an already-open chat's cached tool inventory.
