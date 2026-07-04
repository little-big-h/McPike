#include "ExpressionParser.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <csignal>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <microhttpd.h>
#include <mutex>
#include <netinet/in.h>
#include <nlohmann/json.hpp>
#include <optional>
#include <pthread.h>
#include <string>
#include <thread>
#include <vector>

#ifdef __APPLE__
#include <launch.h>
#endif

using Evaluator = std::function<boss::EvalResult(std::string const&)>;
using boss::EvalResult;

namespace {

/// @brief Hub for the server-sent-events (SSE) clients used by MCP's HTTP+SSE
///        transport.
///
/// Owns the set of connected clients and broadcasts JSON-RPC notifications to
/// them. Thread-safe: MHD invokes the stream/free callbacks from its worker-pool
/// threads while openStream() / broadcast() run on request-handling threads;
/// @ref mutex serializes all access.
class SSEBroadcaster {
public:
  /// @brief Register a new SSE client and build its MHD streaming response.
  ///
  /// From here MHD owns the per-connection state and calls connectionFree() when
  /// the connection ends.
  /// @param mhdConnection connection to stream server-sent events over.
  /// @return the response to queue, or nullptr if MHD could not create it.
  MHD_Response* openStream(MHD_Connection* mhdConnection) {
    auto* connection = new Connection {this, mhdConnection};
    auto* response = MHD_create_response_from_callback(MHD_SIZE_UNKNOWN, 1024, &streamCallback,
                                                       connection, &connectionFree);
    if(response == nullptr) {
      delete connection;
      return nullptr;
    }
    MHD_add_response_header(response, "Content-Type", "text/event-stream");
    MHD_add_response_header(response, "Cache-Control", "no-cache");
    std::lock_guard<std::mutex> lock {mutex};
    connections.push_back(connection);
    return response;
  }

  /// @brief Broadcast a JSON-RPC notification to every connected client.
  /// @param notification the JSON-RPC notification to push to each client.
  void broadcast(nlohmann::json const& notification) {
    std::lock_guard<std::mutex> lock {mutex};
    if(connections.empty())
      return;
    auto const wire = "data: " + notification.dump(-1, ' ', true) + "\n\n";
    for(auto* connection : connections) {
      connection->pending.push_back(wire);
      MHD_resume_connection(connection->mhdConnection);
    }
  }

private:
  struct Connection {
    SSEBroadcaster* owner;
    MHD_Connection* mhdConnection;
    std::deque<std::string> pending;
    bool endpointSent = false;
  };

  /// @brief MHD content-reader callback: fill @p buffer with the next bytes for a
  ///        client.
  ///
  /// Drains the client's pending queue and suspends the connection when empty.
  /// @param cls the Connection* passed as the callback's closure.
  /// @param buffer output buffer to fill.
  /// @param maxBytes capacity of @p buffer.
  /// @return bytes written, or an MHD_CONTENT_READER_* sentinel.
  static ssize_t streamCallback(void* cls, uint64_t /*position*/, char* buffer, size_t maxBytes) {
    auto* connection = static_cast<Connection*>(cls);
    std::lock_guard<std::mutex> lock {connection->owner->mutex};
    if(!connection->endpointSent) {
      static constexpr auto event = std::string_view {"event: endpoint\ndata: /mcp\n\n"};
      if(maxBytes < event.size())
        return MHD_CONTENT_READER_END_WITH_ERROR;
      std::memcpy(buffer, event.data(), event.size());
      connection->endpointSent = true;
      return static_cast<ssize_t>(event.size());
    }
    if(connection->pending.empty()) {
      MHD_suspend_connection(connection->mhdConnection);
      return 0;
    }
    auto& event = connection->pending.front();
    auto const bytes = std::min(event.size(), maxBytes);
    std::memcpy(buffer, event.data(), bytes);
    if(bytes == event.size())
      connection->pending.pop_front();
    else
      event.erase(0, bytes);
    return static_cast<ssize_t>(bytes);
  }

  /// @brief MHD free callback: unregister and destroy a client whose connection
  ///        ended.
  /// @param cls the Connection* passed as the callback's closure.
  static void connectionFree(void* cls) {
    auto* connection = static_cast<Connection*>(cls);
    auto* owner = connection->owner;
    {
      std::lock_guard<std::mutex> lock {owner->mutex};
      auto const it = std::find(owner->connections.begin(), owner->connections.end(), connection);
      if(it != owner->connections.end())
        owner->connections.erase(it);
    }
    delete connection;
  }

  /// Guards @ref connections and each Connection's pending/endpointSent state, all
  /// reached concurrently from MHD worker-pool threads: broadcast() iterates the
  /// registry and appends to every queue; streamCallback() drains one queue;
  /// connectionFree() erases a closing client; openStream() appends a new one.
  /// Without it these overlap into data races on the vector (including iterator
  /// invalidation mid-broadcast) and on the per-connection deques.
  std::mutex mutex;
  std::vector<Connection*> connections; ///< Live SSE clients (freed by MHD via connectionFree()).
} sseBroadcaster;

namespace ContentType {
constexpr auto plain = "text/plain";
constexpr auto json = "application/json";
constexpr auto html = "text/html; charset=utf-8";
} // namespace ContentType

namespace JSONRPCError {
constexpr auto parseError = -32700;
constexpr auto methodNotFound = -32601;
constexpr auto invalidParams = -32602;
constexpr auto internalError = -32603;
} // namespace JSONRPCError

std::optional<std::string> tryUnwrapJSONString(std::string const& jsonString) {
  try {
    auto parsed = nlohmann::json::parse(jsonString);
    return parsed.is_string()
               ? std::optional<std::string> {std::move(parsed.get_ref<std::string&>())}
               : std::nullopt;
  } catch(nlohmann::json::parse_error const&) {
    return std::nullopt;
  }
}

std::string handleMCPRequest(std::string const& jsonBody, Evaluator const& evaluator) {
  auto const nullID = nlohmann::json(nullptr);

  auto request = nlohmann::json {};
  try {
    request = nlohmann::json::parse(jsonBody);
  } catch(nlohmann::json::parse_error const& parseError) {
    return nlohmann::json {
        {"jsonrpc", "2.0"},
        {"id", nullID},
        {"error", {{"code", JSONRPCError::parseError}, {"message", parseError.what()}}}}
        .dump(-1, ' ', true);
  }

  auto const& id = request.value("id", nullID);

  auto makeResult = [&](nlohmann::json result) {
    return nlohmann::json {{"jsonrpc", "2.0"}, {"id", id}, {"result", std::move(result)}}.dump(
        -1, ' ', true);
  };
  auto makeError = [&](int code, std::string_view message) {
    return nlohmann::json {
        {"jsonrpc", "2.0"}, {"id", id}, {"error", {{"code", code}, {"message", message}}}}
        .dump(-1, ' ', true);
  };

  if(!request.contains("id"))
    return {};

  auto const method = request.value("method", "");

  if(method == "ping")
    return makeResult(nlohmann::json::object());

  if(method == "logging/setLevel")
    return makeResult(nlohmann::json::object());

  if(method == "initialize") {
    return makeResult(
        {{"protocolVersion", "2024-11-05"},
         {"capabilities",
          {{"tools", nlohmann::json::object()}, {"logging", nlohmann::json::object()}}},
         {"serverInfo", {{"name", "boss"}, {"version", "1.0"}}}});
  }

  if(method == "tools/list") {
    auto tool = nlohmann::json {};
    tool["name"] = "evaluate";
    auto const engineDescriptionResult = evaluator("(GetEngineDescription)");
    auto const engineDescription = engineDescriptionResult.is_error
                                       ? std::string {}
                                       : tryUnwrapJSONString(engineDescriptionResult.text)
                                             .value_or(engineDescriptionResult.text);
    tool["description"] = std::string(R"(Evaluate a BOSS expression and return the result.

# Engines and their operators

Note that this list is likely long and will be truncated at 2Kb length. To Get the full list, send the expression `(GetEngineDescription)` for evaluation.

)") + engineDescription;
    tool["inputSchema"]["type"] = "object";
    tool["inputSchema"]["properties"]["expression"]["type"] = "string";
    tool["inputSchema"]["properties"]["expression"]["description"] =
        R"(A BOSS s-expression. String literals accept raw UTF-8 or JSON \uXXXX escapes interchangeably. Be mindful that filenames may contain invisible characters (e.g. NBSP in Apple Watch exports) that cannot be reproduced by re-typing — see the tool description for handling.)";
    tool["inputSchema"]["required"] = nlohmann::json::array({"expression"});
    return makeResult({{"tools", nlohmann::json::array({tool})}});
  }

  if(method == "tools/call") {
    if(!request.contains("params"))
      return makeError(JSONRPCError::invalidParams, "Missing params");
    auto const& params = request["params"];
    auto const toolName = params.value("name", "");
    if(toolName != "evaluate")
      return makeError(JSONRPCError::invalidParams, "Unknown tool: " + toolName);
    if(!params.contains("arguments"))
      return makeError(JSONRPCError::invalidParams, "Missing arguments");
    auto const expression = params["arguments"].value("expression", "");
    if(expression.empty())
      return makeError(JSONRPCError::invalidParams, "Missing required argument: expression");
    sseBroadcaster.broadcast({{"jsonrpc", "2.0"},
                              {"method", "notifications/message"},
                              {"params", {{"level", "debug"}, {"data", expression + " -> ... "}}}});
    auto const evaluationResult = evaluator(expression);
    sseBroadcaster.broadcast(
        {{"jsonrpc", "2.0"},
         {"method", "notifications/message"},
         {"params", {{"level", "debug"}, {"data", " -> " + evaluationResult.text}}}});
    if(evaluationResult.is_error)
      return makeError(JSONRPCError::internalError, evaluationResult.text);
    return makeResult({{"content", {{{"type", "text"}, {"text", evaluationResult.text}}}}});
  }

  return makeError(JSONRPCError::methodNotFound, "Method not found: " + method);
}

auto handleBrowserRequest(std::string_view path, Evaluator const& evaluator) {
  auto expression = std::string {"(EvaluateInEngines (List \"libMonitoringEngine.so\")"};
  auto cursor = size_t {0};
  while(cursor < path.size()) {
    if(path[cursor] == '/') {
      ++cursor;
      continue;
    }
    auto const end = std::min(path.find('/', cursor), path.size());
    auto component = std::string(path.substr(cursor, end - cursor));
    auto const decodedLength = MHD_http_unescape(component.data());
    component.resize(decodedLength);
    expression += " |";
    for(char character : component) {
      if(character == '|' || character == '\\')
        expression += '\\';
      expression += character;
    }
    expression += '|';
    cursor = end;
  }
  expression += ')';

  auto result = evaluator(expression);
  struct Response {
    unsigned int status;
    char const* contentType;
    std::string body;
  };
  if(result.is_error)
    return Response {MHD_HTTP_INTERNAL_SERVER_ERROR, ContentType::plain, std::move(result.text)};
  auto html = tryUnwrapJSONString(result.text);
  return html ? Response {MHD_HTTP_OK, ContentType::html, std::move(*html)}
              : Response {MHD_HTTP_INTERNAL_SERVER_ERROR, ContentType::plain,
                          "Monitoring engine returned a non-string: " + std::move(result.text)};
}

struct RequestState {
  std::string body;
};

MHD_Result handleHTTPRequest(void* userData, MHD_Connection* connection, const char* url,
                             const char* method, const char* /*version*/, const char* uploadData,
                             size_t* uploadDataSize, void** connectionContext) {
  auto* evaluator = static_cast<Evaluator*>(userData);

  if(*connectionContext == nullptr) {
    *connectionContext = new RequestState();
    return MHD_YES;
  }

  auto* state = static_cast<RequestState*>(*connectionContext);

  if(*uploadDataSize > 0) {
    state->body.append(uploadData, *uploadDataSize);
    *uploadDataSize = 0;
    return MHD_YES;
  }

  auto responseBody = std::string {};
  auto statusCode = MHD_HTTP_OK;
  auto contentType = ContentType::plain;

  auto const urlView = std::string_view {url};
  auto const path = urlView.substr(0, urlView.find('?'));
  auto const methodView = std::string_view {method};

  if(methodView == "POST" && path == "/mcp") {
    auto mcpResponse = handleMCPRequest(state->body, *evaluator);
    if(mcpResponse.empty()) {
      statusCode = MHD_HTTP_NO_CONTENT;
    } else {
      responseBody = std::move(mcpResponse);
      contentType = ContentType::json;
    }
  } else if(methodView == "GET" && path == "/sse") {
    auto* sseResponse = sseBroadcaster.openStream(connection);
    if(sseResponse == nullptr)
      return MHD_NO;
    auto const sseQueueResult = MHD_queue_response(connection, MHD_HTTP_OK, sseResponse);
    MHD_destroy_response(sseResponse);
    return sseQueueResult;
  } else if(methodView == "GET") {
    auto browserResponse = handleBrowserRequest(path, *evaluator);
    statusCode = browserResponse.status;
    contentType = browserResponse.contentType;
    responseBody = std::move(browserResponse.body);
  } else {
    statusCode = MHD_HTTP_NOT_FOUND;
    responseBody = "Not Found";
  }

  auto* response = MHD_create_response_from_buffer(
      responseBody.size(), static_cast<void*>(responseBody.data()), MHD_RESPMEM_MUST_COPY);
  if(!response)
    return MHD_NO;

  MHD_add_response_header(response, "Content-Type", contentType);
  auto const queueResult = MHD_queue_response(connection, statusCode, response);
  MHD_destroy_response(response);
  return queueResult;
}

void requestCompleted(void* /*userData*/, MHD_Connection* /*connection*/, void** connectionContext,
                      MHD_RequestTerminationCode /*toe*/) {
  delete static_cast<RequestState*>(*connectionContext);
  *connectionContext = nullptr;
}

/// @brief Per-worker-thread chibi-scheme context for evaluating BOSS expressions.
///
/// Under BOSS's build flags chibi's heap and symbol table are per-context
/// (SEXP_USE_GLOBAL_HEAP/SYMBOLS == 0), so distinct contexts never race on the
/// GC/allocator/symbol table; only BOSS's singleton bootstrap engine is shared,
/// and it synchronizes its own state. Giving every worker its own context lets
/// requests evaluate in parallel instead of serializing on one global context.
/// See BOSS docs/threading-audit.md §3. main() warms up one context up front so
/// chibi's one-time global init cannot race across the first worker threads.
struct WorkerContext {
  boss::BossContextGuard guard {boss::initialize_boss_context()};
  sexp context() const { return guard.ctx; }
  sexp environment() const { return guard.ctx != nullptr ? sexp_context_env(guard.ctx) : nullptr; }
};

void runServer(Evaluator const& evaluator, int port) {
  auto stopSignals = sigset_t {};
  sigemptyset(&stopSignals);
  sigaddset(&stopSignals, SIGTERM);
  sigaddset(&stopSignals, SIGINT);
  pthread_sigmask(SIG_BLOCK, &stopSignals, nullptr);

  auto const daemonFlags = MHD_USE_INTERNAL_POLLING_THREAD | MHD_USE_SUSPEND_RESUME | MHD_USE_ITC;
  auto const poolSize = std::max(1u, std::thread::hardware_concurrency());

  MHD_Daemon* daemon = nullptr;

#ifdef __APPLE__
  int* launchdFileDescriptors = nullptr;
  auto launchdFileDescriptorCount = size_t {0};
  if(launch_activate_socket("Listeners", &launchdFileDescriptors, &launchdFileDescriptorCount) ==
         0 &&
     launchdFileDescriptorCount > 0) {
    auto const launchdFileDescriptor = launchdFileDescriptors[0];
    free(launchdFileDescriptors);
    daemon = MHD_start_daemon(daemonFlags, 0, nullptr, nullptr, &handleHTTPRequest,
                              const_cast<Evaluator*>(&evaluator), MHD_OPTION_LISTEN_SOCKET,
                              launchdFileDescriptor, MHD_OPTION_NOTIFY_COMPLETED, &requestCompleted,
                              nullptr, MHD_OPTION_THREAD_POOL_SIZE, poolSize, MHD_OPTION_END);
  } else {
    free(launchdFileDescriptors);
  }
#endif

  if(!daemon) {
    auto address = sockaddr_in {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr("127.0.0.1");
    address.sin_port = htons(static_cast<uint16_t>(port));
    daemon = MHD_start_daemon(daemonFlags, static_cast<uint16_t>(port), nullptr, nullptr,
                              &handleHTTPRequest, const_cast<Evaluator*>(&evaluator),
                              MHD_OPTION_SOCK_ADDR, reinterpret_cast<sockaddr*>(&address),
                              MHD_OPTION_NOTIFY_COMPLETED, &requestCompleted, nullptr,
                              MHD_OPTION_THREAD_POOL_SIZE, poolSize, MHD_OPTION_END);
    if(daemon)
      std::cout << "Listening on 127.0.0.1:" << port << "\n";
  }

  if(!daemon) {
    std::cerr << "Failed to start microhttpd daemon\n";
    return;
  }

  // Wait for a genuine shutdown signal. sigwait() only stores a signal number
  // in receivedSignal when one of the awaited signals is actually delivered.
  // On macOS an attached debugger (SIGSTOP/SIGCONT via ptrace) interrupts the
  // wait: sigwait then returns with errno==EINTR — and, crucially, may return 0
  // (success) while leaving receivedSignal unset (0). Treating that as a signal
  // is what made the server exit the instant a debugger attached. So trust
  // receivedSignal, not the return value: only shut down for SIGTERM/SIGINT.
  for(;;) {
    auto receivedSignal = 0;
    errno = 0;
    auto const result = sigwait(&stopSignals, &receivedSignal);
    if(receivedSignal == SIGTERM || receivedSignal == SIGINT)
      break;
    // Spurious wake-up (e.g. a debugger) or EINTR: keep waiting. Bail out only
    // on an unexpected, persistent error so we never busy-spin.
    auto const error = result == 0 ? errno : result;
    if(error == 0 || error == EINTR)
      continue;
    std::cerr << "sigwait failed: " << std::strerror(error) << "\n";
    break;
  }

  MHD_stop_daemon(daemon);
}

} // namespace

int main() {
  // Run chibi's one-time global init on the main thread before any worker exists
  // (so the first workers can't race on it) and fail fast if BOSS can't start.
  // The context serves only that one-time init, so it is destroyed right away;
  // each worker lazily builds and owns its own (see WorkerContext).
  {
    auto const warmup = boss::initialize_boss_context();
    if(!warmup)
      return 1;
    boss::BossContextGuard const guard {warmup};
  }

  constexpr auto mcpPort = 5080;
  runServer(
      [](std::string const& expression) -> boss::EvalResult {
        thread_local WorkerContext worker;
        if(worker.context() == nullptr)
          return {true, "failed to initialise a BOSS context for this worker thread"};
        return boss::evaluate_expression(worker.context(), worker.environment(), expression);
      },
      mcpPort);
  return 0;
}
