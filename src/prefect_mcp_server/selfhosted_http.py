import os
from urllib.parse import urlparse

import uvicorn
from starlette.datastructures import Headers
from starlette.middleware import Middleware
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from prefect_mcp_server.server import build_prefect_mcp_server


class TailscaleIdentity:
    def __init__(self, app: ASGIApp, login: str):
        if not login:
            raise ValueError("an allowed Tailscale login is required")
        self.app = app
        self.login = login

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            headers = Headers(scope=scope)
            if headers.get("tailscale-user-login") != self.login or any(
                key.startswith("x-prefect-") for key in headers
            ):
                await PlainTextResponse("Forbidden", status_code=403)(
                    scope, receive, send
                )
                return
        await self.app(scope, receive, send)


def build_app(login: str):
    if not login:
        raise ValueError("an allowed Tailscale login is required")
    server = build_prefect_mcp_server(
        name="Prefect Self-hosted",
        include_docs_proxy=False,
        include_cloud_tools=False,
        include_cloud_oauth_tools=False,
        include_execution_plan_tools=False,
    )
    return server.http_app(
        stateless_http=True,
        json_response=True,
        middleware=[Middleware(TailscaleIdentity, login=login)],
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
    app = build_app(os.environ.get("PREFECT_MCP_TAILSCALE_LOGIN", ""))
    uvicorn.run(app, host="127.0.0.1", port=9011, proxy_headers=False, access_log=False)


if __name__ == "__main__":
    main()
