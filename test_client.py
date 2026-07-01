import asyncio
from mcp import ClientSession
from mcp.client.sse import sse_client

async def test_my_tools():
    url = "http://127.0.0.1:8000/mcp/sse"
    
    print(f"Connecting to {url}...")
    async with sse_client(url) as streams:
        # streams[0] is for reading, streams[1] is for writing
        async with ClientSession(streams[0], streams[1]) as session:
            await session.initialize()
            
            # 1. Check if the server exposes our tools
            tools = await session.list_tools()
            print("\n--- Available Tools ---")
            for tool in tools.tools:
                print(f"- {tool.name}: {tool.description}")

            # 2. Test calling the health tool directly
            print("\n--- Testing get_table_health ---")
            try:
                # We are testing the 'orders' table. Make sure this table exists in your local warehouse.
                result = await session.call_tool("get_table_health", arguments={"table_name": "orders"})
                print(f"Result: {result.content[0].text}")
            except Exception as e:
                print(f"Tool call failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_my_tools())