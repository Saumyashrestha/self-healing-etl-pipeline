# mcp_client.py
# This file lives alongside your Server B (root main.py, port 8001).
# Its only job: open a connection to Server A (the MCP toolbox on port 8000)
# and let Server B ask it to run a tool, then hand back the real result.

from mcp.client.sse import sse_client
from mcp.client.session import ClientSession

MCP_SERVER_URL = "http://127.0.0.1:8000/sse"


async def call_mcp_tool(tool_name: str, arguments: dict) -> str:
    """
    Opens a connection to Server A, asks it to run one tool, and returns
    the text result. This is the ONE function Server B should use instead
    of importing get_table_metrics / execute_table_maintenance directly.
    """
    async with sse_client(MCP_SERVER_URL) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            # This is the MCP "handshake" - required once per connection
            await session.initialize()

            # This is the actual "please run this tool" request
            result = await session.call_tool(tool_name, arguments=arguments)

            # MCP tool results come back as a list of content blocks.
            # Your tools all return plain strings, so we just join the text parts.
            text_parts = [block.text for block in result.content if hasattr(block, "text")]
            return "\n".join(text_parts)