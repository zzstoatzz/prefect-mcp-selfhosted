import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

from prefect_mcp_server.selfhosted_http import build_app


def test_identity_required():
    with pytest.raises(ValueError):
        build_app("")


async def test_http_identity_and_pinned_target(test_flow):
    app = build_app("operator@example.com", via_proxy=True)

    def factory(
        headers: dict[str, str] | None = None,
        timeout: httpx.Timeout | None = None,
        auth: httpx.Auth | None = None,
        follow_redirects: bool = False,
    ) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            headers=headers,
            timeout=timeout,
            auth=auth,
            follow_redirects=follow_redirects,
        )

    async with app.lifespan(app):
        async with factory() as http:
            for headers in (
                {},
                {"tailscale-user-login": "someone-else@example.com"},
                {
                    "tailscale-user-login": "operator@example.com",
                    "x-prefect-api-url": "http://localhost:9/api",
                },
                {
                    "tailscale-user-login": "operator@example.com",
                    "x-prefect-api-auth-string": "override:credential",
                },
            ):
                response = await http.post(
                    "http://localhost/mcp", headers=headers, json={}
                )
                assert response.status_code == 403
        transport = StreamableHttpTransport(
            "http://localhost/mcp",
            headers={"tailscale-user-login": "operator@example.com"},
            httpx_client_factory=factory,
        )
        async with Client(transport) as client:
            tools = await client.list_tools()
            assert not any(tool.name.startswith("execution_plans_") for tool in tools)
            result = await client.call_tool(
                "get_flows", {"filter": {"id": {"any_": [str(test_flow)]}}}
            )
            assert result.structured_content is not None
            data = result.structured_content.get("result") or result.structured_content
            assert data["success"] is True
            assert data["count"] == 1
            assert data["flows"][0]["id"] == str(test_flow)


async def test_direct_mode_ignores_forged_identity():
    app = build_app("operator@example.com")
    async with app.lifespan(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as client:
            response = await client.post(
                "http://localhost/mcp",
                headers={
                    "tailscale-user-login": "operator@example.com",
                    "x-forwarded-for": "100.96.216.23",
                },
                json={},
            )
            assert response.status_code == 403
