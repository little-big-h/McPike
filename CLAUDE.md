# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

McPass is a minimal MCP (Model Context Protocol) HTTP server that wraps the BOSS query-evaluation engine. It exposes a single MCP tool (`evaluate`) that accepts a BOSS expression string and returns the result. The entire server is one C++17 source file (`Source/McPass.cpp`).

## Build

McPass requires an external BOSS source tree. Set `BOSS_SOURCE_DIR` to its root; the build expects a compiled BOSS library at `${BOSS_SOURCE_DIR}/Release`.

```sh
cmake -B build -DBOSS_SOURCE_DIR=/path/to/BOSS
cmake --build build
```

Dependencies (`libmicrohttpd` 0.9.77 and `nlohmann/json` 3.11.3) are fetched automatically via CMake `ExternalProject` into `~/.cmake-downloads/McPass` and built into `build/deps/`.

## Architecture

- **`Source/McPass.cpp`** --- the entire server. Key layers top to bottom:
  - `handle_mcp_request()` --- parses JSON-RPC 2.0 and dispatches `initialize`, `tools/list`, and `tools/call` (the only real tool: `evaluate`). Returns empty string for notification methods (-> 204 No Content).
  - `handle_http_request()` / `request_completed()` --- libmicrohttpd callbacks. Accumulates POST body in `RequestState`, then calls `handle_mcp_request`. Only `/` and `/mcp` paths accept POST; everything else is 404.
  - `run_server()` --- starts `MHD_Daemon` in select-mode. On macOS, tries `launch_activate_socket` first (launchd on-demand activation on port 5080); falls back to binding `127.0.0.1:5080` directly. Runs the `select` loop until SIGTERM/SIGINT.
  - `main()` --- initialises a BOSS context via `boss::initialize_boss_context()`, then calls `run_server` with a lambda that calls `boss::evaluate_expression`.

- **`ExpressionParser.hpp`** --- included from `${BOSS_SOURCE_DIR}/Source` (not in this repo). Provides `boss::initialize_boss_context`, `boss::BossContextGuard`, `boss::evaluate_expression`, and `boss::EvalResult`.

- **`launchd/com.boss.mcp.plist`** --- macOS launchd agent for on-demand socket activation on port 5080. Install to `~/Library/LaunchAgents/` and update the binary path before loading.

## MCP endpoint

`POST http://localhost:5080/mcp` (or `/`)
Protocol version: `2024-11-05`
Single tool: `evaluate` with one required string argument `expression`.
