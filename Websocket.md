# McPike WebSocket endpoint

A persistent channel for evaluating BOSS queries over a WebSocket, as an
alternative to the one-shot `POST /mcp` MCP call.

## Connect

- URL: `ws://localhost:5080/ws` (the launchd instance listens on 5080; a
  directly-run server uses `--port <n>`).
- Standard RFC 6455 handshake; no subprotocol, no auth. A non-WebSocket request
  to `/ws` gets HTTP 400.

## Protocol

- **Send**: one WebSocket **text frame per query**: the raw BOSS expression
  string, e.g. `(Plus 1 2 3)`.
- **Receive**: one text frame per query, carrying the evaluation result.
- **Ordering**: McPike evaluates the queries of one connection one at a time, on
  a thread dedicated to that connection, and replies in the order it received
  them.
- **Errors**: McPike returns an errored evaluation as a normal text frame whose
  text starts with `;; error:\n` (e.g. `;; error:\nERROR: match: ...`). A
  successful result has no such prefix.
- **State**: the evaluation environment of a connection persists across that
  connection's queries. Separate connections do not share an environment.

## Shutdown

When McPike receives SIGTERM or SIGINT, it sends every connected client the text
frame `(ServerShuttingDown)`, then a close frame with code 1001 ("going away"),
and exits. McPike does not wait for queries that are still being evaluated, and
it sends no reply for them.

## MonitoringEngine holds queries until the user approves them

When MonitoringEngine is in the engine pipeline, it holds every query that is
not on its whitelist until the user approves or rejects that query in the
monitor. **McPike sends no reply for a held query until the user approves it.**
A client that sends a query and waits therefore appears to hang until then.

The monitor sends its report commands over the same WebSocket endpoint:

- `(EvaluateInEngines (List (Monitoring report)) refresh)`: replies with the
  query at the front of the pending queue. If the queue is empty,
  MonitoringEngine waits until a query arrives and then replies.
- `(EvaluateInEngines (List (Monitoring report)) approve)`: approves the front
  query, so McPike evaluates it and sends its reply, then replies with the next
  pending query, waiting for one if necessary.
- `(EvaluateInEngines (List (Monitoring report)) reject)`: rejects the front
  query, then replies like `approve`.

Send these commands over a WebSocket, not as HTTP GETs of the equivalent
`/EvaluateInEngines/List/Monitoring/:/report/////<command>` URLs. A waiting HTTP
request blocks one of McPike's libmicrohttpd workers until a query arrives, and
a few such requests block every worker. A waiting WebSocket command blocks only
the thread dedicated to that WebSocket connection.

## Sessions: one monitor at a time

An engine opens a session on a WebSocket connection by answering a query with
`(Session <id> <reply>)`. McPike records the connection and the id, and sends the
client only `<reply>`. The client never sees the id.

McPike keeps one session connection at a time:

- When a second connection opens a session, McPike closes the first connection
  with WebSocket close code 4000 and evaluates `(ConnectionClosed <old id>)`
  through the default engine pipeline.
- When the session connection closes for any other reason, McPike evaluates
  `(ConnectionClosed <id>)` in the same way.

The monitor sends `(EvaluateInEngines (List (Monitoring report)) connect)` as
the first frame on every new socket. MonitoringEngine answers
`(Session <id> (Connected))`, so the client receives `(Connected)`. When a
newer monitor connects, MonitoringEngine ends the older monitor's waiting
command with the reply `(ConnectionClosed)`, and McPike then closes the older
monitor's socket with code 4000. The older monitor shows that another monitor
has taken over and does not reconnect until the user clicks Refresh.
