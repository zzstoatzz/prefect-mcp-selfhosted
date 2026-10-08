import asyncio
import ipaddress
import json
import os
from pathlib import Path
from urllib.parse import urlparse

import uvicorn
from starlette.datastructures import Headers
from starlette.middleware import Middleware
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from prefect_mcp_server.chaos_status import inspect_chaos
from prefect_mcp_server.server import build_prefect_mcp_server, tool_annotations


class TailscaleIdentity:
    def __init__(self, app: ASGIApp, login: str, via_proxy: bool):
        if not login:
            raise ValueError("an allowed Tailscale login is required")
        self.app = app
        self.login = login
        self.via_proxy = via_proxy

    async def peer_login(self, address: str) -> str | None:
        if ipaddress.ip_address(address) not in ipaddress.ip_network("100.64.0.0/10"):
            return None
        try:
            process = await asyncio.create_subprocess_exec(
                "tailscale",
                "whois",
                "--json",
                address,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except OSError:
            return None
        try:
            output, _ = await asyncio.wait_for(process.communicate(), 2)
            if process.returncode != 0:
                return None
            return json.loads(output).get("UserProfile", {}).get("LoginName")
        except (TimeoutError, ValueError):
            return None
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            headers = Headers(scope=scope)
            peer = scope.get("client")
            login = (
                headers.get("tailscale-user-login")
                if self.via_proxy
                else await self.peer_login(peer[0])
                if peer
                else None
            )
            if login != self.login or any(
                key.startswith("x-prefect-") for key in headers
            ):
                await PlainTextResponse("Forbidden", status_code=403)(
                    scope, receive, send
                )
                return
        await self.app(scope, receive, send)


def build_app(login: str, via_proxy: bool = False, chaos_state: Path | None = None):
    if not login:
        raise ValueError("an allowed Tailscale login is required")
    server = build_prefect_mcp_server(
        name="Prefect Self-hosted",
        include_docs_proxy=False,
        include_cloud_tools=False,
        include_cloud_oauth_tools=False,
        include_execution_plan_tools=False,
    )
    if chaos_state is not None:

        def get_chaos_status() -> dict[str, object]:
            """Inspect recorded lab failures, recent results, and rolling fault budget.

            Read-only and restricted to the operator-configured state directory.
            A budget opening does not clear a failure latch or authorize a fault.
            Reports describe the isolated lab, not production or live timer state.
            """
            return inspect_chaos(chaos_state)

        server.tool(annotations=tool_annotations(get_chaos_status, read_only=True))(
            get_chaos_status
        )
    return server.http_app(
        stateless_http=True,
        json_response=True,
        allowed_hosts=[os.environ.get("PREFECT_MCP_BIND_ADDRESS", "127.0.0.1")],
        middleware=[Middleware(TailscaleIdentity, login=login, via_proxy=via_proxy)],
    )


def main():
    target = urlparse(os.environ.get("PREFECT_API_URL", ""))
    if target.scheme != "https" or target.path.rstrip("/") != "/api":
        raise SystemExit("a pinned HTTPS Prefect API URL is required")
    if (
        target.username
        or target.password
        or not os.environ.get("PREFECT_API_AUTH_STRING")
    ):
        raise SystemExit("Prefect credentials must be supplied in the environment")
    bind = os.environ.get("PREFECT_MCP_BIND_ADDRESS", "127.0.0.1")
    if bind != "127.0.0.1" and ipaddress.ip_address(bind) not in ipaddress.ip_network(
        "100.64.0.0/10"
    ):
        raise SystemExit("HTTP must bind to loopback or the Tailscale IPv4 interface")
    app = build_app(
        os.environ.get("PREFECT_MCP_TAILSCALE_LOGIN", ""),
        via_proxy=bind == "127.0.0.1",
        chaos_state=Path(os.environ["PREFECT_MCP_CHAOS_STATE"])
        if os.environ.get("PREFECT_MCP_CHAOS_STATE")
        else None,
    )
    uvicorn.run(app, host=bind, port=9011, proxy_headers=False, access_log=False)


if __name__ == "__main__":
    main()
