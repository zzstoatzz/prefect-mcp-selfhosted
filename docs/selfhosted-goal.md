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
