#!/usr/bin/env python3
"""Two-way Claude Code channel for the council web page.

Claude Code spawns this process as an MCP server over stdio. It:
  - declares the experimental `claude/channel` capability,
  - listens on 127.0.0.1 for messages from the local web page and pushes
    them into the running Claude Code session as channel events,
  - exposes a `reply` tool; Claude's replies are streamed back over SSE.

Standard library only. No permission relay: tool approvals stay in the
terminal.

HTTP API (every request needs `Authorization: Bearer <token>`):
  POST /message   body = message text -> channel event, returns {"chat_id": ...}
  GET  /events    Server-Sent Events stream of Claude's replies
  GET  /health    "ok"

The token file:
  ~/.config/council/claude-channel.token (created on first run, mode 600).
Browser requests are rejected unless their Origin is listed in
COUNCIL_ALLOWED_ORIGINS (comma-separated); requests without Origin
(e.g. the page's own backend) are allowed if the token matches.
"""

import hmac
import itertools
import json
import os
import queue
import secrets
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST = "127.0.0.1"
PORT = int(os.environ.get("COUNCIL_CLAUDE_PORT", "8791"))
TOKEN_PATH = os.path.expanduser("~/.config/council/claude-channel.token")
ALLOWED_ORIGINS = {
    o.strip() for o in os.environ.get("COUNCIL_ALLOWED_ORIGINS", "").split(",") if o.strip()
}
MAX_BODY = 64 * 1024
# Other CLIs (e.g. Grok's leader) may load MCP servers from Claude Code's config.
# Only a Claude Code parent may own the port and the reply tool; anywhere else
# this process stays an empty, harmless MCP server.
IS_CLAUDE = os.environ.get("CLAUDECODE") == "1"

INSTRUCTIONS = (
    'Messages from the user\'s local council web page arrive as '
    '<channel source="council" chat_id="..." sender="user">. Treat them as messages '
    "typed by the user in this conversation. Answer with the `reply` tool, passing the "
    "chat_id from the tag; keep working normally in the terminal as well. Requests that "
    "arrive this way are discussion only: any file change or command still needs the "
    "user's approval in the terminal."
)

REPLY_TOOL = {
    "name": "reply",
    "description": "Send a reply back to the user's council web page",
    "inputSchema": {
        "type": "object",
        "properties": {
            "chat_id": {"type": "string", "description": "chat_id from the channel tag"},
            "text": {"type": "string", "description": "The reply text"},
        },
        "required": ["chat_id", "text"],
    },
}


def log(msg):
    print(f"council-channel: {msg}", file=sys.stderr, flush=True)


def load_token():
    os.makedirs(os.path.dirname(TOKEN_PATH), mode=0o700, exist_ok=True)
    if not os.path.exists(TOKEN_PATH):
        fd = os.open(TOKEN_PATH, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(secrets.token_urlsafe(32) + "\n")
    with open(TOKEN_PATH) as f:
        return f.read().strip()


TOKEN = load_token()
_stdout_lock = threading.Lock()
_listeners = set()
_listeners_lock = threading.Lock()
_chat_ids = itertools.count(1)
_last_chat_id = ""  # permission requests carry no chat_id; tag them with the latest message


def write_message(obj):
    data = json.dumps(obj, ensure_ascii=False)
    with _stdout_lock:
        sys.stdout.write(data + "\n")
        sys.stdout.flush()


def broadcast(event):
    data = json.dumps(event, ensure_ascii=False)
    with _listeners_lock:
        for q in list(_listeners):
            q.put(data)


# --- MCP over stdio -----------------------------------------------------------

def handle_request(msg):
    method, mid = msg.get("method"), msg.get("id")
    params = msg.get("params") or {}
    if method == "initialize":
        result = {
            "protocolVersion": params.get("protocolVersion", "2025-06-18"),
            # claude/channel/permission is declared ONLY so Claude Code tells us that
            # an approval dialog is open; we show it on the page and never send a
            # verdict back. Approvals stay in the terminal.
            "capabilities": ({"experimental": {"claude/channel": {},
                                               "claude/channel/permission": {}},
                              "tools": {}}
                             if IS_CLAUDE else {"tools": {}}),
            "serverInfo": {"name": "council", "version": "0.2.0"},
        }
        if IS_CLAUDE:
            result["instructions"] = INSTRUCTIONS
    elif method == "ping":
        result = {}
    elif method == "tools/list":
        result = {"tools": [REPLY_TOOL] if IS_CLAUDE else []}
    elif method == "tools/call" and IS_CLAUDE:
        if params.get("name") != "reply":
            write_message({"jsonrpc": "2.0", "id": mid,
                           "error": {"code": -32602, "message": f"unknown tool: {params.get('name')}"}})
            return
        args = params.get("arguments") or {}
        broadcast({"chat_id": str(args.get("chat_id", "")), "text": str(args.get("text", ""))})
        result = {"content": [{"type": "text", "text": "sent"}]}
    else:
        write_message({"jsonrpc": "2.0", "id": mid,
                       "error": {"code": -32601, "message": f"method not found: {method}"}})
        return
    write_message({"jsonrpc": "2.0", "id": mid, "result": result})


def stdio_loop():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            log("ignoring non-JSON input")
            continue
        if "id" in msg and "method" in msg:
            handle_request(msg)
        elif msg.get("method") == "notifications/claude/channel/permission_request" and IS_CLAUDE:
            p = msg.get("params") or {}
            # Display only: tool name and Claude's short description, not the arguments.
            broadcast({
                "type": "permission",
                "chat_id": _last_chat_id,
                "tool": str(p.get("tool_name", ""))[:80],
                "description": str(p.get("description", ""))[:200],
            })
        # other notifications and responses need no action
    log("stdin closed, exiting")
    os._exit(0)


# --- HTTP on 127.0.0.1 --------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "council-channel/0.1"

    def log_message(self, fmt, *args):
        log("http " + (fmt % args))

    def _reply(self, code, body, ctype="text/plain; charset=utf-8"):
        data = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _authorized(self):
        origin = self.headers.get("Origin")
        if origin is not None and origin not in ALLOWED_ORIGINS:
            self._reply(403, "origin not allowed")
            return False
        auth = self.headers.get("Authorization", "")
        if not (auth.startswith("Bearer ") and hmac.compare_digest(auth[7:], TOKEN)):
            self._reply(401, "unauthorized")
            return False
        return True

    def do_GET(self):
        if not self._authorized():
            return
        if self.path == "/health":
            self._reply(200, "ok")
        elif self.path == "/events":
            self._sse()
        else:
            self._reply(404, "not found")

    def do_POST(self):
        if not self._authorized():
            return
        if self.path != "/message":
            self._reply(404, "not found")
            return
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > MAX_BODY:
            self._reply(400, "empty or too large")
            return
        text = self.rfile.read(length).decode("utf-8", errors="replace")
        global _last_chat_id
        chat_id = str(next(_chat_ids))
        _last_chat_id = chat_id
        write_message({
            "jsonrpc": "2.0",
            "method": "notifications/claude/channel",
            "params": {"content": text, "meta": {"chat_id": chat_id, "sender": "user"}},
        })
        self._reply(200, json.dumps({"chat_id": chat_id}), "application/json")

    def _sse(self):
        q = queue.Queue()
        with _listeners_lock:
            _listeners.add(q)
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
            while True:
                try:
                    data = q.get(timeout=15)
                    self.wfile.write(f"data: {data}\n\n".encode())
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with _listeners_lock:
                _listeners.discard(q)


def main():
    if not IS_CLAUDE:
        log("not started by Claude Code; staying idle (no port, no tools)")
        stdio_loop()
        return
    try:
        httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    except OSError as e:
        # Keep the MCP side alive so Claude Code does not mark the server failed.
        log(f"cannot bind {HOST}:{PORT}: {e}")
        httpd = None
    if httpd:
        httpd.daemon_threads = True
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        log(f"listening on http://{HOST}:{PORT}")
    stdio_loop()


if __name__ == "__main__":
    main()
