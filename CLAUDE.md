# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

McPass is a minimal MCP (Model Context Protocol) HTTP server that wraps the BOSS query-evaluation engine. It exposes a single MCP tool (`evaluate`) that accepts a BOSS expression string and returns the result. The entire server is one C++17 source file (`Source/McPass.cpp`).

## Build

BOSS is fetched and built automatically — no manual setup required:

```sh
cmake -B build
cmake --build build
```

By default the build clones `symbol-store/BOSS` at `main` via CMake `ExternalProject` and builds it into `build/boss-build/`. Override with:
- `-DBOSS_GIT_TAG=<branch|tag|commit>` — build a different BOSS revision.
- `-DBOSS_SOURCE_DIR=/path/to/BOSS` — reuse an existing local BOSS checkout (built in `<dir>/Release`) instead of cloning; useful for offline work or BOSS co-development.
- `-DBOSS_BUILD_ENGINES="ns/Engine:branch;..."` — forwarded to the BOSS sub-build so BOSS clones+builds those engines (add `-DGITHUB_TOKEN=...`, or set `$GITHUB_TOKEN`, for private engine repos).

Like BOSS, the `libmicrohttpd` 0.9.77 and `nlohmann/json` 3.11.3 dependencies are fetched automatically via `ExternalProject` into `~/.cmake-downloads/McPass` and built into `build/deps/`.

## Architecture

- **`Source/McPass.cpp`** --- the entire server. Key layers top to bottom:
  - `handle_mcp_request()` --- parses JSON-RPC 2.0 and dispatches `initialize`, `tools/list`, and `tools/call` (the only real tool: `evaluate`). Returns empty string for notification methods (-> 204 No Content).
  - `handle_http_request()` / `request_completed()` --- libmicrohttpd callbacks. Accumulates POST body in `RequestState`, then calls `handle_mcp_request`. Only `/` and `/mcp` paths accept POST; everything else is 404.
  - `run_server()` --- starts `MHD_Daemon` in select-mode. On macOS, tries `launch_activate_socket` first (launchd on-demand activation on port 5080); falls back to binding `127.0.0.1:5080` directly. Runs the `select` loop until SIGTERM/SIGINT.
  - `main()` --- initialises a BOSS context via `boss::initialize_boss_context()`, then calls `run_server` with a lambda that calls `boss::evaluate_expression`.

- **`ExpressionParser.hpp`** --- included from BOSS's `Source/` tree (not in this repo; not an installed public header, so the build references the BOSS source tree directly). Provided by the fetched BOSS checkout under `build/boss-src/Source`, or `${BOSS_SOURCE_DIR}/Source` when overriding. Provides `boss::initialize_boss_context`, `boss::BossContextGuard`, `boss::evaluate_expression`, and `boss::EvalResult`.

- **`launchd/com.boss.mcp.plist`** --- macOS launchd agent for on-demand socket activation on port 5080. Install to `~/Library/LaunchAgents/` and update the binary path before loading. The socket's `Bonjour` key makes launchd advertise it over multicast DNS as `_mcp._tcp.local` (loopback-only reachability), even while the process is dormant.

## MCP endpoint

`POST http://localhost:5080/mcp` (or `/`)
Protocol version: `2024-11-05`
Single tool: `evaluate` with one required string argument `expression`.

The on-demand launchd instance is discoverable via Bonjour/mDNS as `_mcp._tcp.local` (port 5080, loopback-only). Note that Claude has no native mDNS discovery, so register it with a fixed URL: `claude mcp add --transport http mcpike http://localhost:5080/mcp`.
