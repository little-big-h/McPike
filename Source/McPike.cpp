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
constexpr auto binary = "application/octet-stream";
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

/// One node of the S-expression tree decoded from a browser URL path. A node
/// renders as `(symbol children...)` when @ref forceList is set (it was opened
/// by a descend) or when it has children; otherwise it renders as a bare atom.
struct PathNode {
  std::string symbol;
  bool forceList;
  std::vector<size_t> children; // indices into the owning pool
};

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

/// The current worker thread's lazily-created BOSS context. Shared by the MCP (string)
/// and browser (tree) request paths so each worker thread owns exactly one context.
WorkerContext& currentWorker() {
  thread_local WorkerContext worker;
  return worker;
}

/// Turn one decoded path segment into a chibi datum, letting chibi's reader infer the
/// literal type (number, boolean, char, string, symbol). If the segment is not exactly one
/// self-delimiting datum -- e.g. a symbol containing spaces or parens -- it is interned
/// verbatim as a symbol, so no quoting or escaping is ever needed.
sexp leafToSexp(sexp ctx, std::string const& segment) {
  sexp_gc_var3(port, datum, rest);
  sexp_gc_preserve3(ctx, port, datum, rest);
  port = sexp_open_input_string(ctx, sexp_c_string(ctx, segment.c_str(), -1));
  datum = sexp_read(ctx, port);
  rest = sexp_read(ctx, port); // must reach EOF for the segment to be a single atom
  auto const clean =
      !sexp_exceptionp(datum) && datum != SEXP_EOF && !sexp_pairp(datum) && rest == SEXP_EOF;
  sexp const result = clean ? datum : sexp_intern(ctx, segment.c_str(), -1);
  sexp_gc_release3(ctx);
  return result;
}

/// Build a chibi datum directly from the decoded path tree: a list node becomes
/// (head child...), a leaf becomes its reader-typed value. This is the tree-first
/// counterpart to serialising a string and having BOSS re-parse it.
sexp pathNodeToSexp(sexp ctx, std::vector<PathNode> const& pool, size_t index) {
  auto const& node = pool[index];
  if(!node.forceList && node.children.empty())
    return leafToSexp(ctx, node.symbol);
  sexp_gc_var2(lst, item);
  sexp_gc_preserve2(ctx, lst, item);
  lst = SEXP_NULL;
  for(auto child = node.children.rbegin(); child != node.children.rend(); ++child) {
    item = pathNodeToSexp(ctx, pool, *child);
    lst = sexp_cons(ctx, item, lst);
  }
  item = sexp_intern(ctx, node.symbol.c_str(), -1); // heads are always symbols
  lst = sexp_cons(ctx, item, lst);
  sexp_gc_release2(ctx);
  return lst;
}

struct DatumResult {
  bool isError;
  bool isBinary;
  std::string body;
};

/**
 * @brief Evaluate a pre-built datum and produce the browser response body.
 *
 * A (Binary :spans <bytevector>...) result is returned as its span bytevectors
 * concatenated verbatim (raw binary); anything else is serialised to text via
 * boss-print. The text/error tail mirrors boss::evaluate_expression (keep them in
 * sync); boss::eval_expr wraps the datum in (boss-eval ...), exactly as the string
 * path does after sexp_read.
 */
DatumResult evaluateDatum(sexp ctx, sexp env, sexp datum, bool pretty = true) {
  boss::concurrency::ConcurrencyTripwire const tripwire(ctx, "evaluateDatum");
  sexp_gc_var4(result, out_port, result_str, arg_list);
  sexp_gc_preserve4(ctx, result, out_port, result_str, arg_list);
  result = boss::eval_expr(ctx, env, datum);

  if(sexp_exceptionp(result)) {
    out_port = sexp_open_output_string(ctx);
    sexp_print_exception(ctx, result, out_port);
    result_str = sexp_get_output_string(ctx, out_port);
    auto text = std::string {sexp_string_data(result_str), sexp_string_size(result_str)};
    sexp_gc_release4(ctx);
    return {true, false, std::move(text)};
  }

  if(sexp_pairp(result) && sexp_car(result) == sexp_intern(ctx, "Binary", -1)) {
    sexp_gc_var3(convert, args, bossObj);
    sexp_gc_preserve3(ctx, convert, args, bossObj);
    convert =
        sexp_env_ref(ctx, env, sexp_intern(ctx, "convert-to-boss-expression", -1), SEXP_FALSE);
    args = sexp_list1(ctx, result);
    bossObj = sexp_apply(ctx, convert, args);
    auto bytes = std::string {};
    auto const& complex =
        std::get<boss::ComplexExpression>(static_cast<boss::Expression::SuperType const&>(
            static_cast<BOSSExpression*>(sexp_cpointer_value(bossObj))->delegate));
    for(auto const& spanArgument : complex.getSpanArguments())
      std::visit(
          [&bytes](auto const& span) {
            using ElementType = typename std::decay_t<decltype(span)>::element_type;
            if constexpr(std::is_pointer_v<decltype(span.begin())> &&
                         std::is_trivially_copyable_v<ElementType>)
              bytes.append(reinterpret_cast<char const*>(span.begin()),
                           span.size() * sizeof(ElementType));
          },
          spanArgument);
    sexp_gc_release3(ctx);
    sexp_gc_release4(ctx);
    return {false, true, std::move(bytes)};
  }

  auto text = std::string {};
  if(result != SEXP_VOID) {
    if(pretty) {
      sexp const print_proc =
          sexp_env_ref(ctx, env, sexp_intern(ctx, "boss-print", -1), SEXP_FALSE);
      arg_list = sexp_list1(ctx, result);
      result_str = sexp_apply(ctx, print_proc, arg_list);
    } else {
      result_str = sexp_write_to_string(ctx, result);
    }
    text = std::string {sexp_string_data(result_str), sexp_string_size(result_str)};
  }
  sexp_gc_release4(ctx);
  return {false, false, std::move(text)};
}

/**
 * @brief Handle a browser (GET) request whose URL path encodes a nested BOSS
 *        S-expression, evaluate it, and return the result as HTML.
 *
 * The path is read as a depth-first walk of an expression tree. Percent-decoded
 * path segments are the symbols; the run of '/' characters between two segments
 * encodes the structural move from one to the next. With N the number of
 * slashes in a separator:
 *
 *   - N == 1  (/)    descend: the next symbol opens a fresh child
 *                    sub-expression of the current innermost list and becomes
 *                    the new innermost list.
 *   - N == 2  (//)   stay: the next symbol is appended as a bare atom to the
 *                    current innermost list; the open list is unchanged.
 *   - N >= 3         ascend (N-2) levels: pop N-2 open lists, then open the
 *                    next symbol as a sub-expression of the list innermost
 *                    afterwards.
 *
 * The first segment is the root. A symbol opened by a descend always prints as
 * a list, so descents nest even without further children; a symbol opened by an
 * ascend prints as a list only once it gains children, else as a bare atom; a
 * symbol added by a stay is always a bare atom. Hence:
 *
 *   /a/b/c/d    -> (a (b (c (d))))   four descends
 *   /a/b/c//d   -> (a (b (c d)))     ...then stay
 *   /a/b/c///d  -> (a (b (c) d))     ...then ascend one level
 *
 * Because an ascend re-opens the parent as the insertion point, sibling
 * sub-expressions are expressible: ascend back up, then descend into the next
 * head.
 *
 *   /Select/From//Flights///Where/GT//Altitude//1000
 *       -> (Select (From Flights) (Where (GT Altitude 1000)))
 *
 * ## Argument control operators
 *
 * Writing many sibling atoms as //x//y//z is verbose. A segment that decodes to
 * exactly ':' or to ':' followed by ASCII digits is a control token: it
 * contributes no symbol (its surrounding slashes are ignored) and instead
 * adjusts how the following segments attach to the current innermost list:
 *
 *   - :k  (k>0)  the next k symbols are flat atoms appended to the current
 *               innermost list regardless of their separators, then normal
 *               parsing resumes.
 *   - :   (bare) every following symbol is a flat atom until either an ascending
 *               separator (N>=3) is seen (which leaves flat mode and then
 *               performs the ascent) or the path ends.
 *   - :0         mark the current innermost node as an argument-less expression,
 *               so it renders as a list '(sym)' even without children. This is
 *               the only way to write a childless list that is not the deepest
 *               node of a descend chain (a plain descend already yields one:
 *               /a/b -> (a (b))).
 *
 *   /a/:2/b/c/d -> (a b c (d))   b,c flat; d resumes descending
 *   /f/:/a/b/c  -> (f a b c)     all remaining symbols flat
 *   /f/:0       -> (f)           argument-less expression (vs /f -> f)
 *   /Select/From//Flights///Where/GT/:2/Altitude/1000
 *       -> (Select (From Flights) (Where (GT Altitude 1000)))
 *   /Select/From//Flights///Where/:0
 *       -> (Select (From Flights) (Where)) *
 * Only a segment-initial ':' followed by nothing or digits is structural;
 * colons elsewhere in a segment are ordinary symbol characters, and a segment
 * like ':foo' (colon then non-digits) is an ordinary keyword symbol, so those
 * still round-trip.
 *
 * Leaves are typed by chibi's own reader (see @ref leafToSexp): numbers, booleans and
 * "quoted strings" decode to their BOSS types, clean words to symbols, and anything with
 * spaces or reader-special characters is interned verbatim as a symbol -- no quoting
 * needed. The decoded tree is handed to BOSS as a datum (@ref pathNodeToSexp,
 * @ref evaluateDatum), never re-serialised to a string and re-parsed.
 *
 * @param path      the request URL path (its query string already stripped).
 */
auto handleBrowserRequest(std::string_view path) {
  auto pool = std::vector<PathNode> {};
  auto stack = std::vector<size_t> {}; // indices of open lists; back() is innermost
  auto flatRemaining = size_t {0};     // >0: this many further segments are flat atoms
  auto flatUnbounded = false;          // all further segments are flat atoms (until an ascend)

  auto const addChild = [&](size_t parent, std::string symbol, bool forceList, bool push) {
    pool.push_back(PathNode {std::move(symbol), forceList, {}});
    auto const index = pool.size() - 1;
    pool[parent].children.push_back(index);
    if(push)
      stack.push_back(index);
  };

  auto cursor = size_t {0};
  while(cursor < path.size()) {
    if(path[cursor] == '/') {
      ++cursor;
      continue;
    }
    // Count the run of slashes that preceded this segment.
    auto slashes = size_t {0};
    for(auto scan = cursor; scan > 0 && path[scan - 1] == '/'; --scan)
      ++slashes;

    auto const end = std::min(path.find('/', cursor), path.size());
    auto segment = std::string(path.substr(cursor, end - cursor));
    auto const decodedLength = MHD_http_unescape(segment.data());
    segment.resize(decodedLength);
    cursor = end;

    // A segment-initial ':' (bare, or followed only by ASCII digits) selects
    // flat-argument mode rather than contributing a symbol.
    auto isControl = !segment.empty() && segment[0] == ':';
    for(size_t i = 1; isControl && i < segment.size(); ++i)
      isControl = segment[i] >= '0' && segment[i] <= '9';
    if(isControl) {
      if(segment.size() == 1) { // bare ':' -> flat mode until an ascend or the path ends
        flatUnbounded = true;
        flatRemaining = 0;
      } else {
        auto count = size_t {0};
        for(size_t i = 1; i < segment.size(); ++i)
          count = count * 10 + size_t(segment[i] - '0');
        flatUnbounded = false;
        flatRemaining = 0;
        if(count == 0) { // ':0' -> the current node is an argument-less expression
          if(!stack.empty())
            pool[stack.back()].forceList = true;
        } else {
          flatRemaining = count;
        }
      }
      continue;
    }

    if(stack.empty()) { // the first real segment is the root
      pool.push_back(PathNode {std::move(segment), false, {}});
      stack.push_back(pool.size() - 1);
      continue;
    }

    if(flatUnbounded && slashes < 3) {
      addChild(stack.back(), std::move(segment), false, false);
      continue;
    }
    if(flatRemaining > 0) {
      addChild(stack.back(), std::move(segment), false, false);
      --flatRemaining;
      continue;
    }
    flatUnbounded = false; // an ascend (or a spent count) leaves flat mode

    if(slashes <= 1) { // descend: open a child sub-expression
      addChild(stack.back(), std::move(segment), true, true);
    } else if(slashes == 2) { // stay: append a bare atom at the current level
      addChild(stack.back(), std::move(segment), false, false);
    } else { // ascend (slashes - 2) levels, then open a sub-expression there
      for(auto pops = slashes - 2; pops > 0 && stack.size() > 1; --pops)
        stack.pop_back();
      addChild(stack.back(), std::move(segment), false, true);
    }
  }

  struct Response {
    unsigned int status;
    char const* contentType;
    std::string body;
  };
  auto& worker = currentWorker();
  auto const ctx = worker.context();
  auto const env = worker.environment();
  if(ctx == nullptr)
    return Response {MHD_HTTP_INTERNAL_SERVER_ERROR, ContentType::plain,
                     "failed to initialise a BOSS context for this worker thread"};
  if(pool.empty())
    return Response {MHD_HTTP_BAD_REQUEST, ContentType::plain, "empty request path"};

  sexp_gc_var1(datum);
  sexp_gc_preserve1(ctx, datum);
  datum = pathNodeToSexp(ctx, pool, 0);
  auto result = evaluateDatum(ctx, env, datum);
  sexp_gc_release1(ctx);

  if(result.isError)
    return Response {MHD_HTTP_INTERNAL_SERVER_ERROR, ContentType::plain, std::move(result.body)};
  if(result.isBinary)
    return Response {MHD_HTTP_OK, ContentType::binary, std::move(result.body)};
  auto html = tryUnwrapJSONString(result.body);
  return html ? Response {MHD_HTTP_OK, ContentType::html, std::move(*html)}
              : Response {MHD_HTTP_INTERNAL_SERVER_ERROR, ContentType::plain,
                          "Monitoring engine returned a non-string: " + std::move(result.body)};
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
    auto browserResponse = handleBrowserRequest(path);
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
        auto& worker = currentWorker();
        if(worker.context() == nullptr)
          return {true, "failed to initialise a BOSS context for this worker thread"};
        return boss::evaluate_expression(worker.context(), worker.environment(), expression);
      },
      mcpPort);
  return 0;
}
