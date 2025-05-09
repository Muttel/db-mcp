## Database MCP Server

Contains a custom made MYSQL MCP Server for doing database operations using Claude

- [Example Chat](https://claude.ai/share/eb54af96-6612-44db-9ef7-23d29c75f1fc)

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