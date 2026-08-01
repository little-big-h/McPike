# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

McPike is a minimal MCP (Model Context Protocol) HTTP server that wraps the BOSS query-evaluation engine. It exposes a single MCP tool (`evaluate`) that accepts a BOSS expression string and returns the result. The entire server is one C++17 source file (`Source/McPike.cpp`).

## Build

BOSS is fetched and built automatically — no manual setup required:

```sh
cmake -B build
cmake --build build
```

By default the build clones `symbol-store/BOSS` at `main` via CMake `ExternalProject` and builds it into `build/boss-build/`. Override with:
- `-DBOSS_GIT_TAG=<branch|tag|commit>` — build a different BOSS revision.
- `-DBOSS_SOURCE_DIR=/path/to/BOSS` — reuse an existing local BOSS checkout (built in `<dir>/Release`) instead of cloning; useful for offline work or BOSS co-development.
- `-DBOSS_DEFAULT_ENGINES="FITSDKEngine;ArrowComputeEngine"` — semicolon-separated engine names, forwarded to the BOSS sub-build so BOSS builds those engines. Entries may also use the repo-spec form `Namespace/Engine[:Branch]` (e.g. `another-org/MyEngine:dev`) to pull a default engine from outside the `symbol-store` org — McPike forwards the bare name to BOSS's `BOSS_DEFAULT_ENGINES` and routes the full spec via `BOSS_BUILD_ENGINES`, and BOSS is patched (`cmake/PatchBOSSDefaultEngines.cmake`) to skip its `symbol-store/`-prefix auto-add when the engine is already represented. (`-DBOSS_BUILD_ENGINES="ns/Engine:branch;..."` is also forwarded directly for engines that should be built but not default-loaded; add `-DGITHUB_TOKEN=...`, or set `$GITHUB_TOKEN`, for private engine repos.)

Like BOSS, the `libmicrohttpd` 0.9.77 and `nlohmann/json` 3.11.3 dependencies are fetched automatically via `ExternalProject` into `~/.cmake-downloads/McPike` and built into `build/deps/`.

## Architecture

- **`Source/McPike.cpp`** --- the entire server. Key layers top to bottom:
  - `handleMCPRequest()` --- parses JSON-RPC 2.0 and dispatches `initialize`, `ping`, `logging/setLevel`, `tools/list`, and `tools/call` (the only real tool: `evaluate`). Returns empty string for notifications (-> 202 Accepted). A failed BOSS evaluation comes back as a normal result with `isError: true`, not as a JSON-RPC error --- the model can act on the message, and a protocol error would hide it.
  - `handleHTTPRequest()` / `requestCompleted()` --- libmicrohttpd callbacks. Accumulates the POST body in `RequestState` (capped at 4 MB -> 413), validates a present `Origin` against loopback (-> 403) and a present `MCP-Protocol-Version` against the supported set (-> 400), then calls `handleMCPRequest`. `POST /mcp` is the MCP endpoint; any other method on it is 405. `GET /sse` opens the legacy HTTP+SSE log stream; every other `GET` is handled by the browser layer, which evaluates the URL path as an S-expression.
  - `run_server()` --- starts `MHD_Daemon` in select-mode. On macOS, tries `launch_activate_socket` first (launchd on-demand activation on port 5080); falls back to binding `127.0.0.1:5080` directly. Runs the `select` loop until SIGTERM/SIGINT.
  - `main()` --- initialises a BOSS context via `boss::initialize_boss_context()`, then calls `run_server` with a lambda that calls `boss::evaluate_expression`.

- **`ExpressionParser.hpp`** --- included from BOSS's `Source/` tree (not in this repo; not an installed public header, so the build references the BOSS source tree directly). Provided by the fetched BOSS checkout under `build/boss-src/Source`, or `${BOSS_SOURCE_DIR}/Source` when overriding. Provides `boss::initialize_boss_context`, `boss::BossContextGuard`, `boss::evaluate_expression`, and `boss::EvalResult`.

- **`launchd/com.boss.mcp.plist`** --- macOS launchd agent for on-demand socket activation on port 5080. Install to `~/Library/LaunchAgents/` and update the binary path before loading. The socket's `Bonjour` key makes launchd advertise it over multicast DNS as `_mcp._tcp.local` (loopback-only reachability), even while the process is dormant.

## MCP endpoint

`POST http://localhost:5080/mcp` --- the single MCP endpoint (`/` is the browser layer, not MCP).
Protocol versions: `2025-11-25`, `2025-06-18`, `2025-03-26`, `2024-11-05` --- `initialize` echoes the client's requested version when it is one of these, otherwise answers with the newest.
Single tool: `evaluate` with one required string argument `expression`.

The on-demand launchd instance is discoverable via Bonjour/mDNS as `_mcp._tcp.local` (port 5080, loopback-only). Note that Claude has no native mDNS discovery, so register it with a fixed URL: `claude mcp add --transport http mcpike http://localhost:5080/mcp`.

### Upgrading to the 2026-07-28 revision

The [`2026-07-28` revision](https://modelcontextprotocol.io/specification/2026-07-28/changelog) splits MCP into two eras: *legacy* versions open with an `initialize` handshake, while *modern* ones are stateless and carry version, identity and capabilities in each request's `_meta`. McPike currently implements the legacy era only, because a modern-only server rejects legacy clients and they have no fall-forward path — and the clients we serve have not switched yet.

Everything already in place is era-neutral: `resultType` on every result, `isError` for tool failures, `ttlMs`/`cacheScope` on `tools/list`, `202` for notifications, `405` for `GET`/`DELETE` on `/mcp`, `Origin` validation, and `MCP-Protocol-Version` parsing. What remains, to be added as a second branch in `handleMCPRequest` (dispatch on the presence of `params._meta["io.modelcontextprotocol/protocolVersion"]`, which the spec explicitly allows a dual-era server to serve on the same endpoint):

- `server/discover` — mandatory in the modern era; returns `supportedVersions`, `capabilities`, `instructions`, and `serverInfo` under the result's `_meta`.
- Validation of the required `Mcp-Method` and `Mcp-Name` headers against the request body, rejecting a mismatch with `-32020`.
- The MCP-reserved error codes `-32020` (HeaderMismatch), `-32021` (MissingRequiredClientCapability) and `-32022` (UnsupportedProtocolVersion, whose `data` carries `{supported, requested}`).
- HTTP `404` alongside `-32601` for an unknown method — that pairing is how a client tells a modern server from a legacy one.
- Dropping `ping` and `logging/setLevel` on that branch (both removed from the protocol), and emitting `notifications/message` only for requests that asked for it via `_meta["io.modelcontextprotocol/logLevel"]` — the current unconditional broadcast in `tools/call` would violate that rule.

Not applicable to this server, recorded so it need not be re-derived: MRTR / `input_required` (nothing here needs input back from the client), `subscriptions/listen` (nothing to push but the log channel), the Tasks and Apps extensions, pagination cursors (one tool), and the authorization changes (loopback only).

Note also that HTTP+SSE — the transport behind `GET /sse` — is now formally Deprecated under the new twelve-month feature-lifecycle policy, along with Roots, Sampling and Logging. Still functional, but removable in a later revision, so `/sse` wants revisiting before that window closes.
