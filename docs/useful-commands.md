# Useful Commands

Quick reference for common development commands.

## Playwright MCP (Docker)

Browser automation for Claude Code, running in a Docker container.

```bash
# Start the Playwright MCP container
cd ~/.claude/playwright-mcp && docker compose up -d

# Stop the Playwright MCP container
cd ~/.claude/playwright-mcp && docker compose down

# Rebuild after config changes
cd ~/.claude/playwright-mcp && docker compose up -d --build

# View container logs
docker logs playwright-mcp

# Check container status
docker ps --filter name=playwright-mcp
```

**Note:** The container auto-restarts unless explicitly stopped. Restart Claude Code after starting/stopping to reconnect the MCP.
