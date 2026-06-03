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
  auto const null_id = nlohmann::json(nullptr);

  nlohmann::json req;
  try {
    req = nlohmann::json::parse(json_body);
  } catch(nlohmann::json::parse_error const& e) {
    return nlohmann::json{{"jsonrpc", "2.0"}, {"id", null_id},
                         {"error", {{"code", -32700}, {"message", e.what()}}}}.dump(-1, ' ', true);
  }

  auto const& id = req.value("id", null_id);

  auto make_result = [&](nlohmann::json result) {
    return nlohmann::json {{"jsonrpc", "2.0"}, {"id", id}, {"result", std::move(result)}}.dump(-1, ' ', true);
  };
  auto make_error = [&](int code, std::string_view message) {
    return nlohmann::json {
        {"jsonrpc", "2.0"}, {"id", id}, {"error", {{"code", code}, {"message", message}}}}
        .dump(-1, ' ', true);
  };

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
    EvalResult const ops = evaluator("(GetEngineDescription)");
    std::string ops_text;
    if(!ops.is_error) {
      try { ops_text = nlohmann::json::parse(ops.text).get<std::string>(); }
      catch(...) { ops_text = ops.text; }
    }
    tool["description"] = std::string(R"(Evaluate a BOSS expression and return the result.

# Engines and their operators

Note that this list is likely long and will be truncated at 2Kb length. To Get the full list, send the expression `(GetEngineDescription)` for evaluation.

)") + ops_text ;
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
