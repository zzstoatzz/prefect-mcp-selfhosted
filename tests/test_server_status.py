import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest
from fastmcp import Client
from prefect.settings import PREFECT_API_URL, temporary_settings

from prefect_mcp_server._prefect_client.server_status import get_server_status
from prefect_mcp_server.server import build_prefect_mcp_server


async def test_cloud_server_omits_selfhosted_status():
    server = build_prefect_mcp_server(
        include_docs_proxy=False, include_cloud_tools=True
    )
    async with Client(server) as client:
        assert "get_server_status" not in {
            tool.name for tool in await client.list_tools()
        }


@pytest.mark.parametrize("samples", [0, 9])
async def test_invalid_sample_count(samples):
    with pytest.raises(ValueError, match="between 1 and 8"):
        await get_server_status(samples=samples)


async def test_real_http_failures_preserve_other_observations():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.endswith("/health"):
                self.send_response(404)
                data = b"not available"
            elif self.path.endswith("/ready"):
                self.send_response(200)
                data = b"not json"
            else:
                self.send_response(200)
                data = json.dumps("test-version").encode()
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args) -> None:
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=http.serve_forever, daemon=True)
    thread.start()
    try:
        with temporary_settings(
            {PREFECT_API_URL: f"http://127.0.0.1:{http.server_port}/api"}
        ):
            server = build_prefect_mcp_server(include_docs_proxy=False)
            async with Client(server) as client:
                response = await client.call_tool("get_server_status", {"samples": 1})
                result = response.structured_content
        observation = result["observations"][0]
        assert observation["health"]["status"] == 404
        assert observation["readiness"]["error"] == "JSONDecodeError"
        assert observation["api_version"]["value"] == "test-version"
        assert result["throughput_verified"] is False
    finally:
        http.shutdown()
        http.server_close()
        thread.join()
