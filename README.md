## Database MCP Server

Contains a custom made MYSQL MCP Server for doing database operations using Claude

Just Add the following configuration in Claude

```json
{
    "mcpServers": {
        "db-mcp": {
            "command": "uv",
            "args": [
                "--directory",
                "/path/to/db-mcp",
                "run",
                "main.py"
            ]
        }
    }
}
```