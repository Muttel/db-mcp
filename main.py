from config.mcp_config import mcp
import controllers

if __name__ == "__main__":
    mcp.run(transport='stdio')