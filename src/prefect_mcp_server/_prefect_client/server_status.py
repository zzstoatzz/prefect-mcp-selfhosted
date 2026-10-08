"""Bounded observations of the configured self-hosted API."""

import asyncio
from time import perf_counter

import httpx

from prefect_mcp_server._prefect_client.client import get_prefect_client


async def get_server_status(samples: int = 2) -> dict[str, object]:
    if not 1 <= samples <= 8:
        raise ValueError("samples must be between 1 and 8")
    observations = []
    async with get_prefect_client() as client:
        api_url = str(client.api_url)
        if "/accounts/" in api_url and "/workspaces/" in api_url:
            raise ValueError("get_server_status requires a self-hosted Prefect API")
        for _ in range(samples):
            observation = {}
            for name, path in (
                ("health", "/health"),
                ("readiness", "/ready"),
                ("compatibility_version", "/admin/version"),
                ("api_version", "/version"),
            ):
                started = perf_counter()
                try:
                    response = await asyncio.wait_for(
                        client._client.get(path), timeout=3
                    )
                    observation[name] = {
                        "status": response.status_code,
                        "value": response.json(),
                    }
                except httpx.HTTPStatusError as exc:
                    observation[name] = {"status": exc.response.status_code}
                except (httpx.HTTPError, TimeoutError, ValueError) as exc:
                    observation[name] = {"error": type(exc).__name__}
                observation[name]["latency_ms"] = round(
                    (perf_counter() - started) * 1000, 2
                )
            observations.append(observation)
    versions = sorted(
        {
            value
            for observation in observations
            if isinstance(value := observation["api_version"].get("value"), str)
        }
    )
    return {
        "api_url": api_url,
        "observations": observations,
        "observed_api_versions": versions,
        "version_skew_observed": len(versions) > 1,
        "replica_coverage": "unknown: gateway samples do not enumerate replicas",
        "throughput_verified": False,
    }
