#include "ExpressionParser.hpp"

#include <arpa/inet.h>
#include <csignal>
#include <functional>
#include <iostream>
#include <microhttpd.h>
#include <netinet/in.h>
#include <nlohmann/json.hpp>
#include <string>
#include <sys/select.h>

#ifdef __APPLE__
#include <launch.h>
#endif

using Evaluator = std::function<boss::EvalResult(std::string const&)>;
using boss::EvalResult;

namespace {

static volatile sig_atomic_t g_stop = 0;
void signal_handler(int) { g_stop = 1; }

std::string handle_mcp_request(std::string const& json_body, Evaluator const& evaluator) {
  auto const req = nlohmann::json::parse(json_body, nullptr, false);
  auto const null_id = nlohmann::json(nullptr);
  auto const& id = req.is_discarded() ? null_id : req.value("id", null_id);

  auto make_result = [&](nlohmann::json result) {
    return nlohmann::json {{"jsonrpc", "2.0"}, {"id", id}, {"result", std::move(result)}}.dump();
  };
  auto make_error = [&](int code, std::string_view message) {
    return nlohmann::json {
        {"jsonrpc", "2.0"}, {"id", id}, {"error", {{"code", code}, {"message", message}}}}
        .dump();
  };

  if(req.is_discarded())
    return make_error(-32700, "Parse error");

  std::string const method = req.value("method", "");

  if(method == "notifications/initialized" || method == "initialized")
    return {};

  if(method == "initialize") {
    return make_result({{"protocolVersion", "2024-11-05"},
                        {"capabilities", {{"tools", nlohmann::json::object()}}},
                        {"serverInfo", {{"name", "boss"}, {"version", "1.0"}}}});
  }

  if(method == "tools/list") {
    nlohmann::json tool;
    tool["name"] = "evaluate";
    tool["description"] = R"(Evaluate a BOSS expression and return the result.

**Loading FIT workout data:**
- `(LoadFIT "/path/to/dir")` — summary table, one row per `.fit` file; columns: `file`, `time_created`, `start_time`, `sport`, `total_elapsed_time` (seconds), `total_distance` (metres), `total_calories`
- `(LoadFIT "/path/to/file.fit" msgtype)` — single file with message type
- `(LoadFIT "/path/to/dir" msgtype)` — all files in directory with message type

Message types (Garmin FIT protocol spec columns):
- `session` — one row per workout; per-workout aggregates: `avg/max_heart_rate`, `avg/max_speed`, `avg/max_power`, `avg_cadence`, `total_calories`, `total_distance`, `total_elapsed_time`, `total_ascent`, `num_laps`, GPS bounding box, `training_load_peak`, `sport`, `timestamp`, etc.
- `record` — one row per second; time-series GPS/sensor data: `timestamp`, `position_lat/long`, `altitude`, `heart_rate`, `cadence`, `speed`, `power`, `distance`, etc. **Large — use only for single-file analysis.**
- `lap` — per-lap summaries
- `activity` — activity-level metadata

**Path conventions:** Paths must be absolute. `~` is not expanded (BOSS does not invoke shell expansion). Use `/Users/<name>/...` on macOS, `/home/<name>/...` on Linux.

**Unicode in filenames:** Raw UTF-8 and JSON `\uXXXX` escapes are both accepted and equivalent — use whichever your client emits naturally. The real hazard is *invisible* Unicode: filenames produced by Apple devices commonly contain U+00A0 (non-breaking space) where a regular space appears to be — for instance, between "Apple" and "Watch" in Apple Watch export filenames. NBSP renders identically to a regular space everywhere, including in the `file` column returned by the directory-summary query, so it cannot be detected by sight. If `LoadFIT` reports `cannot open file` on a path that *visually* matches the directory listing, write the suspect gaps explicitly as `\u00a0` and retry. The same caution applies to U+200B (zero-width space), U+00AD (soft hyphen), and the Unicode dash variants `‐`/`‑`/`–`/`—`. Discover the row with `(Slice (OrderBy (LoadFIT ".../dir") (List (Desc time_created))) 0 1)`.

**Operators:**
- `(Schema table)` — list column names
- `(Project table col... (As expr alias)...)` — select/derive columns; **`As` takes expression first, alias second**; arithmetic uses full Arrow compute function names: `Multiply`, `Divide`, `Add`, `Subtract` — **not** `*`/`/`/`+`/`-`
- `(Filter table pred)` — filter rows; supports full Arrow compute set: `Equal`, `Greater`, `GreaterEqual`, `Less`, `LessEqual`, `And`, `Or`, `Not`, `IsNull`, `IsValid`, etc.
- `(GroupBy table (AggFn col) [key...])` — aggregate; **`AggFn` takes a column name (symbol), not an expression** — derive columns with `Project` first; supports full Arrow aggregation set: `Sum`, `Mean`, `Max`, `Min`, `Count`, `CountAll`, `StdDev`, `Variance`, etc.
- `(OrderBy table (List col... (Desc col)...))` — sort; wrap in `(Desc col)` for descending
- `(Slice table offset count)` — zero-based row slice; **use instead of `Head` to limit results** (`Head` does not reduce output size)
- `(Cumulate table (AggFn col))` — running/prefix aggregate
- `(Pairwise table out-col in-col lag)` — sliding delta: `out[i] = in[i+lag] - in[i]`
- `(Join left (List key...) right (List key...))` / `LeftJoin` / `AntiJoin` — hash joins; colliding non-key columns get `_l`/`_r` suffixes
- `(Name table sym)` / `(ByName sym)` — store/retrieve named tables; **persists across calls**
- `(Load "/path/to.csv")` — load a CSV file
- `(Table (col val...)...)` — construct a literal in-memory table
- `(Materialize table)` — force chunked Arrow arrays into a single contiguous buffer
- `(ToStatus table)` — evaluate pipeline, return `"OK"` (useful for profiling)

**Key patterns:**
```
; Most recent N workouts
(Slice (OrderBy (LoadFIT ".../dir") (List (Desc time_created))) 0 5)

; Per-sport average of a derived metric (derive first, then aggregate)
(GroupBy
  (Project (LoadFIT ".../dir")
    (As (Divide total_calories (Divide total_elapsed_time 60.0)) cal_per_min)
    sport)
  (Mean cal_per_min)
  sport)

; Cache a large load for reuse across calls
(Name (LoadFIT ".../dir" session) workouts)
(GroupBy (ByName workouts) (Mean total_calories) sport)
```

**Avoid returning unaggregated full-directory loads** — they exceed the result size limit. Always wrap in `GroupBy`, `Slice`, or `Filter` before returning.

---

# Parameter description (`expression` field)

A BOSS s-expression. String literals accept raw UTF-8 or JSON `\uXXXX` escapes interchangeably. Be mindful that filenames may contain invisible characters (e.g. NBSP in Apple Watch exports) that cannot be reproduced by re-typing — see the tool description for handling.)";
    tool["inputSchema"]["type"] = "object";
    tool["inputSchema"]["properties"]["expression"]["type"] = "string";
    tool["inputSchema"]["properties"]["expression"]["description"] = R"(A BOSS s-expression. String literals accept raw UTF-8 or JSON \uXXXX escapes interchangeably. Be mindful that filenames may contain invisible characters (e.g. NBSP in Apple Watch exports) that cannot be reproduced by re-typing — see the tool description for handling.)";
    tool["inputSchema"]["required"] = nlohmann::json::array({"expression"});
    return make_result({{"tools", nlohmann::json::array({tool})}});
  }

  if(method == "tools/call") {
    if(!req.contains("params"))
      return make_error(-32602, "Missing params");
    auto const& params = req["params"];
    std::string const tool_name = params.value("name", "");
    if(tool_name != "evaluate")
      return make_error(-32602, "Unknown tool: " + tool_name);
    if(!params.contains("arguments"))
      return make_error(-32602, "Missing arguments");
    std::string const expression = params["arguments"].value("expression", "");
    if(expression.empty())
      return make_error(-32602, "Missing required argument: expression");
    EvalResult const eval_result = evaluator(expression);
    if(eval_result.is_error)
      return make_error(-32603, eval_result.text);
    return make_result({{"content", {{{"type", "text"}, {"text", eval_result.text}}}}});
  }

  return make_error(-32601, "Method not found: " + method);
}

struct RequestState {
  std::string body;
};

MHD_Result handle_http_request(void* cls, MHD_Connection* connection, const char* url,
                               const char* method, const char* /*version*/, const char* upload_data,
                               size_t* upload_data_size, void** req_cls) {
  auto* evaluator = static_cast<Evaluator*>(cls);

  if(*req_cls == nullptr) {
    *req_cls = new RequestState();
    return MHD_YES;
  }

  auto* state = static_cast<RequestState*>(*req_cls);

  if(*upload_data_size > 0) {
    state->body.append(upload_data, *upload_data_size);
    *upload_data_size = 0;
    return MHD_YES;
  }

  std::string response_body;
  unsigned int status_code = MHD_HTTP_OK;

  bool const is_mcp_path = (std::string_view(url) == "/mcp" || std::string_view(url) == "/");
  if(std::string_view(method) == "POST" && is_mcp_path) {
    std::string mcp_response = handle_mcp_request(state->body, *evaluator);
    if(mcp_response.empty()) {
      status_code = MHD_HTTP_NO_CONTENT;
    } else {
      response_body = std::move(mcp_response);
    }
  } else {
    status_code = MHD_HTTP_NOT_FOUND;
    response_body = "Not Found";
  }

  MHD_Response* response = MHD_create_response_from_buffer(
      response_body.size(), static_cast<void*>(response_body.data()), MHD_RESPMEM_MUST_COPY);
  if(!response)
    return MHD_NO;

  char const* content_type =
      (status_code == MHD_HTTP_NOT_FOUND) ? "text/plain" : "application/json";
  MHD_add_response_header(response, "Content-Type", content_type);
  MHD_Result const queue_result = MHD_queue_response(connection, status_code, response);
  MHD_destroy_response(response);
  return queue_result;
}

void request_completed(void* /*cls*/, MHD_Connection* /*connection*/, void** req_cls,
                       MHD_RequestTerminationCode /*toe*/) {
  delete static_cast<RequestState*>(*req_cls);
  *req_cls = nullptr;
}

void run_server(Evaluator const& evaluator, int port) {
  signal(SIGTERM, signal_handler);
  signal(SIGINT, signal_handler);

  MHD_Daemon* daemon = nullptr;

#ifdef __APPLE__
  int* launchd_fds = nullptr;
  size_t launchd_fd_count = 0;
  if(launch_activate_socket("Listeners", &launchd_fds, &launchd_fd_count) == 0 &&
     launchd_fd_count > 0) {
    int const launchd_fd = launchd_fds[0];
    free(launchd_fds);
    daemon =
        MHD_start_daemon(0, 0, nullptr, nullptr, &handle_http_request,
                         const_cast<Evaluator*>(&evaluator), MHD_OPTION_LISTEN_SOCKET, launchd_fd,
                         MHD_OPTION_NOTIFY_COMPLETED, &request_completed, nullptr, MHD_OPTION_END);
  } else {
    free(launchd_fds);
  }
#endif

  if(!daemon) {
    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(static_cast<uint16_t>(port));
    daemon =
        MHD_start_daemon(0, static_cast<uint16_t>(port), nullptr, nullptr, &handle_http_request,
                         const_cast<Evaluator*>(&evaluator), MHD_OPTION_SOCK_ADDR,
                         reinterpret_cast<sockaddr*>(&addr), MHD_OPTION_NOTIFY_COMPLETED,
                         &request_completed, nullptr, MHD_OPTION_END);
    if(daemon)
      std::cout << "Listening on 127.0.0.1:" << port << "\n";
  }

  if(!daemon) {
    std::cerr << "Failed to start MHD daemon\n";
    return;
  }

  while(!g_stop) {
    fd_set read_fds;
    fd_set write_fds;
    fd_set except_fds;
    FD_ZERO(&read_fds);
    FD_ZERO(&write_fds);
    FD_ZERO(&except_fds);
    int max_fd = 0;

    if(MHD_get_fdset(daemon, &read_fds, &write_fds, &except_fds, &max_fd) != MHD_YES) {
      std::cerr << "MHD_get_fdset failed\n";
      break;
    }

    MHD_UNSIGNED_LONG_LONG mhd_timeout = 0;
    struct timeval tv {};
    if(MHD_get_timeout(daemon, &mhd_timeout) == MHD_YES) {
      tv.tv_sec = static_cast<time_t>(mhd_timeout / 1000);
      tv.tv_usec = static_cast<suseconds_t>((mhd_timeout % 1000) * 1000);
    } else {
      tv.tv_sec = 1;
      tv.tv_usec = 0;
    }

    int const select_result = select(max_fd + 1, &read_fds, &write_fds, &except_fds, &tv);
    if(select_result >= 0)
      MHD_run_from_select(daemon, &read_fds, &write_fds, &except_fds);
  }

  MHD_stop_daemon(daemon);
}

} // namespace

int main() {
  sexp ctx = boss::initialize_boss_context();
  if(!ctx)
    return 1;
  boss::BossContextGuard const ctx_guard {ctx};
  sexp env = sexp_context_env(ctx);
  constexpr int kMcpPort = 5080;
  run_server(
      [ctx, env](std::string const& expr) { return boss::evaluate_expression(ctx, env, expr); },
      kMcpPort);
  return 0;
}
