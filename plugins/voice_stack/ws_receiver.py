"""Hermes ⇄ Home Assistant WebSocket receiver.

This module exposes the `/api/hermes/ws` endpoint that the Home Assistant
custom integration connects to. It is intentionally small and dependency-light:
Home Assistant sends JSON events/actions, and this receiver dispatches voice
control actions to the local voice stack tools.
"""

from __future__ import annotations

import asyncio
import atexit
import errno
import hmac
import json
import logging
import mimetypes
import os
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional

try:
    from aiohttp import WSMsgType, web
    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only in minimal installs
    WSMsgType = None  # type: ignore[assignment]
    web = None  # type: ignore[assignment]
    AIOHTTP_AVAILABLE = False

if TYPE_CHECKING:  # pragma: no cover
    from aiohttp import web as aiohttp_web

logger = logging.getLogger(__name__)

DEFAULT_WS_HOST = "0.0.0.0"
DEFAULT_WS_PORT = 7860
DEFAULT_WS_PATH = "/api/hermes/ws"

_WS_SERVER: Optional["HermesHAWebSocketServer | _AdoptedReceiver"] = None
_WS_LOCK = threading.Lock()
_WS_WATCHDOG: Optional[threading.Thread] = None
_WS_WATCHDOG_STOP = threading.Event()
_START_TIME = time.monotonic()
_HEALTH_PROBE_TTL_SECONDS = 5.0
_HEALTH_PROBE_CACHE: dict[str, tuple[float, bool]] = {}
_MESSAGE_COUNTERS: dict[str, int] = {}
_COUNTER_LOCK = threading.Lock()
_VOICE_ACTION_RESERVED_KEYS = {"type", "action", "args"}
_ASSIST_HISTORY_LOCK = threading.Lock()
_ASSIST_HISTORY: dict[str, list[dict[str, Any]]] = {}
_AUDIO_ROUTE_PATH = "/api/hermes/audio/{filename}"
_ASSIST_AUX_TASK_KEY = "voice_stack_assist"
_ATEXIT_HOOK_REGISTERED = False


def _is_gateway_process() -> bool:
    """True when this Python process is the Hermes gateway daemon."""
    return os.environ.get("_HERMES_GATEWAY") == "1"


def _gateway_daemon_running() -> bool:
    """Best-effort check for a separate gateway process (dashboard/CLI guard)."""
    try:
        from gateway.status import is_gateway_running

        return bool(is_gateway_running())
    except Exception:
        return False


def _should_bind_ws_receiver() -> bool:
    """Return True when this process should own the HA WebSocket listen socket.

    The gateway daemon always binds. Dashboard, chat, and other short-lived
    Hermes processes skip binding when a gateway is already running so restarts
    do not fight over port 7860.
    """
    if os.getenv("HERMES_HA_WS_ENABLED", "1").strip().lower() in {"0", "false", "no", "off"}:
        return False
    if os.getenv("HERMES_HA_WS_FORCE_BIND", "").strip().lower() in {"1", "true", "yes", "on"}:
        return True
    if _is_gateway_process():
        return True
    if _gateway_daemon_running():
        return False
    return True


def _register_shutdown_hook() -> None:
    """Release the listen socket before process exit (gateway restart)."""
    global _ATEXIT_HOOK_REGISTERED
    if _ATEXIT_HOOK_REGISTERED:
        return
    atexit.register(stop_ws_receiver)
    _ATEXIT_HOOK_REGISTERED = True


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return max(0.0, float(raw))
    except ValueError:
        return default


def _retry_base_seconds() -> float:
    return _env_float("HERMES_HA_WS_RETRY_SECONDS", 2.0)


def _retry_max_seconds() -> float:
    return max(_retry_base_seconds(), _env_float("HERMES_HA_WS_MAX_RETRY_SECONDS", 60.0))


def _watchdog_interval_seconds() -> float:
    return max(5.0, _env_float("HERMES_HA_WS_WATCHDOG_SECONDS", 30.0))


def _health_probe_host(host: str) -> str:
    normalized = (host or DEFAULT_WS_HOST).strip() or DEFAULT_WS_HOST
    if normalized in {"0.0.0.0", "::"}:
        return "127.0.0.1"
    return normalized


def _probe_existing_receiver(host: str, port: int) -> bool:
    """Return True when another process already serves the HA receiver health endpoint."""
    probe_host = _health_probe_host(host)
    cache_key = f"{probe_host}:{int(port)}"
    now = time.monotonic()
    cached = _HEALTH_PROBE_CACHE.get(cache_key)
    if cached and (now - cached[0]) < _HEALTH_PROBE_TTL_SECONDS:
        return cached[1]

    url = f"http://{probe_host}:{int(port)}/health"
    healthy = False
    try:
        with urllib.request.urlopen(url, timeout=2.0) as response:
            payload = json.loads(response.read().decode("utf-8"))
            healthy = (
                isinstance(payload, dict)
                and payload.get("service") == "hermes-ha-ws"
                and payload.get("running") is True
            )
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError, ValueError):
        healthy = False

    _HEALTH_PROBE_CACHE[cache_key] = (now, healthy)
    return healthy


class _AdoptedReceiver:
    """Represents a receiver owned by another Hermes process on the same port."""

    def __init__(self, host: str, port: int, path: str) -> None:
        self.host = host
        self.port = int(port)
        self.path = path
        self.adopted = True

    @property
    def active_connections(self) -> int:
        return 0

    @property
    def total_connections(self) -> int:
        return 0

    @property
    def running(self) -> bool:
        return _probe_existing_receiver(self.host, self.port)


def _record_message(msg_type: str) -> None:
    """Track receiver message counts for health/status responses."""
    key = msg_type or "<missing>"
    with _COUNTER_LOCK:
        _MESSAGE_COUNTERS[key] = _MESSAGE_COUNTERS.get(key, 0) + 1


def _message_counters_snapshot() -> dict[str, int]:
    with _COUNTER_LOCK:
        return dict(_MESSAGE_COUNTERS)


def receiver_status(server: Optional["HermesHAWebSocketServer | _AdoptedReceiver"] = None) -> dict[str, Any]:
    """Return process-local receiver health data safe for HA status probes."""
    active_connections = 0
    total_connections = 0
    running = False
    bound: dict[str, Any] = {}
    adopted = False
    target = server or _WS_SERVER
    if target is not None:
        active_connections = target.active_connections
        total_connections = target.total_connections
        running = target.running
        bound = {"host": target.host, "port": target.port, "path": target.path}
        adopted = bool(getattr(target, "adopted", False))
    return {
        "ok": True,
        "service": "hermes-ha-ws",
        "running": running,
        "adopted": adopted,
        "uptime_seconds": round(time.monotonic() - _START_TIME, 1),
        "auth_required": bool(_configured_token()),
        "active_connections": active_connections,
        "total_connections": total_connections,
        "message_counters": _message_counters_snapshot(),
        **bound,
    }


def _with_request_id(payload: dict[str, Any], response: dict[str, Any]) -> dict[str, Any]:
    """Preserve caller request IDs for HA-side correlation."""
    if "id" in payload and "id" not in response:
        response = {**response, "id": payload["id"]}
    return response


def _json_loads_maybe(value: Any) -> dict[str, Any]:
    """Parse tool-handler JSON strings into dicts; wrap non-JSON values."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
            return {"value": parsed}
        except json.JSONDecodeError:
            return {"message": value}
    return {"value": value}


def _create_session_db_for_assist():
    """Best-effort SessionDB for HA Assist queries."""
    try:
        from hermes_state import SessionDB
        return SessionDB()
    except Exception as exc:
        logger.debug("SQLite session store not available for HA Assist: %s", exc)
        return None


def _build_assist_system_prompt() -> str:
    """Return a concise, voice-friendly prompt with optional HA context."""
    try:
        from .pipeline import build_voice_system_prompt
    except Exception:
        return (
            "You are Hermes answering Home Assistant Assist requests. "
            "Respond concisely, naturally, and clearly. Use Home Assistant tools when needed."
        )

    entities = None
    try:
        from ..home_assistant.ha_assistant import search_entities
        result = search_entities()
        if isinstance(result, dict):
            raw_entities = result.get("entities")
            if isinstance(raw_entities, list):
                entities = raw_entities[:30]
    except Exception:
        entities = None

    return build_voice_system_prompt(areas=None, entities=entities)


def _assist_auxiliary_configured(cfg: dict[str, Any]) -> bool:
    """Return True when the user explicitly configured the Assist aux task."""
    auxiliary = cfg.get("auxiliary", {}) if isinstance(cfg, dict) else {}
    task_cfg = auxiliary.get(_ASSIST_AUX_TASK_KEY, {}) if isinstance(auxiliary, dict) else {}
    if not isinstance(task_cfg, dict):
        return False
    return any(
        key in task_cfg
        for key in ("provider", "model", "base_url", "api_key", "api_mode")
    )


def _resolve_assist_runtime(cfg: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Resolve runtime + model for HA Assist.

    Default behavior preserves the existing main-model routing. When the user
    explicitly configures ``auxiliary.voice_stack_assist`` in Hermes config,
    Assist queries switch to that auxiliary provider/model instead.
    """
    from hermes_cli.runtime_provider import resolve_runtime_provider

    model_cfg = cfg.get("model", {}) if isinstance(cfg, dict) else {}
    effective_model = ""
    effective_provider = None
    if isinstance(model_cfg, dict):
        effective_model = str(model_cfg.get("default") or model_cfg.get("model") or "").strip()
        raw_provider = str(model_cfg.get("provider") or "").strip().lower()
        effective_provider = raw_provider or None

    if not _assist_auxiliary_configured(cfg):
        runtime = resolve_runtime_provider(
            requested=effective_provider,
            target_model=effective_model or None,
        )
        return runtime, effective_model

    from agent.auxiliary_client import _resolve_task_provider_model

    requested, resolved_model, resolved_base_url, resolved_api_key, _resolved_api_mode = _resolve_task_provider_model(
        _ASSIST_AUX_TASK_KEY
    )
    runtime = resolve_runtime_provider(
        requested=requested,
        explicit_base_url=resolved_base_url,
        explicit_api_key=resolved_api_key,
        target_model=resolved_model or effective_model or None,
    )
    return runtime, str(resolved_model or effective_model or "").strip()


def run_local_assist_query(
    text: str,
    *,
    conversation_id: Optional[str] = None,
    language: str = "en",
) -> dict[str, Any]:
    """Run one Assist text query through Hermes and return an assist_response payload."""
    conversation_id = (conversation_id or "").strip() or f"ha-{int(time.time() * 1000)}"
    original_text = str(text or "")
    clean_text = original_text.strip()
    if not clean_text:
        prompt = "I didn't catch that. Could you repeat?"
        return {
            "type": "assist_response",
            "ok": False,
            "conversation_id": conversation_id,
            "text": prompt,
            "speech": {"plain": {"speech": prompt}},
        }

    from hermes_cli.config import load_config
    from hermes_cli.fallback_config import get_fallback_chain
    from hermes_cli.tools_config import _get_platform_tools
    from run_agent import AIAgent

    cfg = load_config()
    runtime, effective_model = _resolve_assist_runtime(cfg)

    toolsets = set(_get_platform_tools(cfg, "cli"))
    toolsets.add("homeassistant")
    session_db = _create_session_db_for_assist()
    fallback_chain = get_fallback_chain(cfg)
    system_prompt = _build_assist_system_prompt()

    with _ASSIST_HISTORY_LOCK:
        history = list(_ASSIST_HISTORY.get(conversation_id) or [])

    agent = AIAgent(
        api_key=runtime.get("api_key"),
        base_url=runtime.get("base_url"),
        provider=runtime.get("provider"),
        api_mode=runtime.get("api_mode"),
        model=effective_model,
        enabled_toolsets=sorted(toolsets),
        quiet_mode=True,
        platform="cli",
        session_db=session_db,
        credential_pool=runtime.get("credential_pool"),
        fallback_model=fallback_chain or None,
        ephemeral_system_prompt=system_prompt,
    )
    agent.suppress_status_output = True
    agent.stream_delta_callback = None
    agent.tool_gen_callback = None

    if language and language.lower() != "en":
        clean_text = f"Respond in language code {language}.\n\nUser request: {clean_text}"

    result = agent.run_conversation(
        user_message=clean_text,
        conversation_history=history,
        task_id=f"ha-assist-{conversation_id}",
        persist_user_message=original_text,
    )

    final_text = str(result.get("final_response") or result.get("error") or "").strip()
    if not final_text:
        final_text = "I processed your request but got no response."

    messages = result.get("messages")
    if isinstance(messages, list):
        with _ASSIST_HISTORY_LOCK:
            _ASSIST_HISTORY[conversation_id] = messages

    return {
        "type": "assist_response",
        "ok": not bool(result.get("failed")),
        "conversation_id": conversation_id,
        "text": final_text,
        "speech": {"plain": {"speech": final_text}},
    }


async def handle_assist_query_async(payload: dict[str, Any]) -> dict[str, Any]:
    """Handle an Assist text query without blocking the aiohttp event loop."""
    text = str(payload.get("text") or "").strip()
    conversation_id = str(payload.get("conversation_id") or "").strip() or None
    language = str(payload.get("language") or "en").strip() or "en"
    response = await asyncio.to_thread(
        run_local_assist_query,
        text,
        conversation_id=conversation_id,
        language=language,
    )
    return _with_request_id(payload, response)


def _configured_token() -> str:
    """Return the optional bearer token accepted by the HA WebSocket endpoint."""
    return (
        os.getenv("HERMES_HA_WS_TOKEN")
        or os.getenv("API_SERVER_KEY")
        or os.getenv("HERMES_API_KEY")
        or ""
    ).strip()


def _auth_ok(headers: Mapping[str, str]) -> bool:
    """Validate Authorization when a receiver token is configured."""
    token = _configured_token()
    if not token:
        return True
    auth = headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return False
    supplied = auth[7:].strip()
    return hmac.compare_digest(supplied, token)


def build_audio_stream_url(audio_path: str) -> str:
    """Return an externally reachable URL for a synthesized audio file."""
    filename = Path(audio_path).name
    base_url = (
        os.getenv("HERMES_HA_MEDIA_BASE_URL", "").strip()
        or os.getenv("HERMES_HA_WS_PUBLIC_BASE_URL", "").strip()
        or os.getenv("HERMES_HA_WS_PUBLIC_URL", "").strip()
    )
    if not base_url:
        host = os.getenv("HERMES_HA_WS_HOST", DEFAULT_WS_HOST).strip() or DEFAULT_WS_HOST
        port = int(os.getenv("HERMES_HA_WS_PORT", str(DEFAULT_WS_PORT)).strip() or DEFAULT_WS_PORT)
        if host in {"0.0.0.0", "::", "127.0.0.1", "localhost"}:
            host = "127.0.0.1"
        base_url = f"http://{host}:{port}"
    base_url = base_url.rstrip("/")
    url = f"{base_url}/api/hermes/audio/{filename}"
    token = _configured_token()
    if token:
        url = f"{url}?token={token}"
    return url


def handle_voice_action(payload: dict[str, Any]) -> dict[str, Any]:
    """Dispatch a HA-originated voice action to voice_stack handlers.

    Supported actions:
    - enable  -> voice_enable
    - disable -> voice_disable
    - status  -> voice_status
    """
    action = str(payload.get("action", "")).strip().lower()
    args = dict(payload.get("args") or {})
    for key, value in payload.items():
        if key not in _VOICE_ACTION_RESERVED_KEYS and key not in args:
            args[key] = value

    from . import (
        _handle_voice_disable,
        _handle_voice_enable,
        _handle_voice_status,
    )

    handlers: dict[str, Callable[[dict], str]] = {
        "enable": _handle_voice_enable,
        "disable": _handle_voice_disable,
        "status": _handle_voice_status,
    }
    handler = handlers.get(action)
    if handler is None:
        return {
            "ok": False,
            "error": f"Unsupported voice action: {action or '<missing>'}",
            "supported_actions": sorted(handlers),
        }

    try:
        result = _json_loads_maybe(handler(args))
        ok = bool(result.get("ok", True)) if "error" not in result else False
        return {"ok": ok, "action": action, "result": result}
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        logger.exception("voice_action %s failed", action)
        return {"ok": False, "action": action, "error": str(exc)}


def handle_ha_ws_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Handle one JSON payload from Home Assistant."""
    msg_type = str(payload.get("type", "")).strip().lower()
    _record_message(msg_type)

    if msg_type == "voice_action":
        result = handle_voice_action(payload)
        return _with_request_id(payload, {"type": "voice_action_result", **result})

    if msg_type == "assist_query":
        return _with_request_id(payload, {
            "type": "error",
            "ok": False,
            "error": "assist_query must be handled asynchronously",
        })

    if msg_type == "state_changed":
        # P0 receiver behaviour: acknowledge state pushes so HA knows Hermes
        # accepted the event. Context ingestion can be layered on this later.
        return _with_request_id(payload, {
            "type": "ack",
            "ok": True,
            "received": "state_changed",
            "entity_id": payload.get("entity_id"),
        })

    if msg_type == "ping":
        return _with_request_id(payload, {"type": "pong", "ok": True})

    if msg_type == "status":
        return _with_request_id(payload, {"type": "status", **receiver_status()})

    return _with_request_id(payload, {"type": "error", "ok": False, "error": f"Unsupported message type: {msg_type or '<missing>'}"})


class HermesHAWebSocketServer:
    """Small aiohttp WebSocket server for HA-originated Hermes messages."""

    def __init__(self, host: str = DEFAULT_WS_HOST, port: int = DEFAULT_WS_PORT, path: str = DEFAULT_WS_PATH) -> None:
        if not AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp is required for Hermes HA WebSocket receiver")
        self.host = host
        self.port = int(port)
        self.path = path
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._runner: Optional["aiohttp_web.AppRunner"] = None
        self._started = threading.Event()
        self._stopped = threading.Event()
        self._stop_requested = threading.Event()
        self._active_connections = 0
        self._total_connections = 0
        self._connections_lock = threading.Lock()
        self._last_error: Optional[str] = None
        self._listening = False

    @property
    def active_connections(self) -> int:
        with self._connections_lock:
            return self._active_connections

    @property
    def total_connections(self) -> int:
        with self._connections_lock:
            return self._total_connections

    def _connection_opened(self) -> None:
        with self._connections_lock:
            self._active_connections += 1
            self._total_connections += 1

    def _connection_closed(self) -> None:
        with self._connections_lock:
            self._active_connections = max(0, self._active_connections - 1)

    @property
    def running(self) -> bool:
        return (
            self._listening
            and self._thread is not None
            and self._thread.is_alive()
        )

    def start(self) -> bool:
        """Start the receiver in a daemon thread. Returns False if already running."""
        if self.running:
            return False
        self._stopped.clear()
        self._stop_requested.clear()
        self._started.clear()
        self._thread = threading.Thread(target=self._run_thread, name="hermes-ha-ws", daemon=True)
        self._thread.start()
        self._started.wait(timeout=5.0)
        return self.running

    def stop(self) -> None:
        """Stop the receiver."""
        self._stop_requested.set()
        if not self._loop or not self.running:
            return
        future = asyncio.run_coroutine_threadsafe(self._shutdown(), self._loop)
        try:
            future.result(timeout=5.0)
        except Exception as exc:  # pragma: no cover - defensive shutdown path
            logger.warning("Hermes HA WebSocket shutdown failed: %s", exc)
        if self._thread:
            self._thread.join(timeout=5.0)

    def _run_thread(self) -> None:
        backoff = _retry_base_seconds()
        while not self._stop_requested.is_set():
            loop = asyncio.new_event_loop()
            self._loop = loop
            asyncio.set_event_loop(loop)
            started_ok = False
            try:
                loop.run_until_complete(self._start_async())
                started_ok = True
                self._listening = True
                self._started.set()
                self._last_error = None
                backoff = _retry_base_seconds()
                logger.info(
                    "Hermes HA WebSocket receiver listening on %s:%s%s",
                    self.host,
                    self.port,
                    self.path,
                )
                loop.run_forever()
            except OSError as exc:
                self._last_error = str(exc)
                if exc.errno in {errno.EADDRINUSE, errno.EACCES} and _probe_existing_receiver(self.host, self.port):
                    logger.info(
                        "Hermes HA WebSocket port %s:%s already served by another process; adopting external receiver",
                        self.host,
                        self.port,
                    )
                    self._started.set()
                    break
                logger.warning(
                    "Hermes HA WebSocket receiver failed to start: %s (retry in %.1fs)",
                    exc,
                    backoff,
                )
                self._started.set()
            except Exception as exc:
                self._last_error = str(exc)
                logger.warning(
                    "Hermes HA WebSocket receiver failed to start: %s (retry in %.1fs)",
                    exc,
                    backoff,
                )
                self._started.set()
            finally:
                self._listening = False
                try:
                    loop.run_until_complete(self._shutdown())
                except Exception:
                    pass
                loop.close()
                self._stopped.set()

            if self._stop_requested.is_set():
                break
            if started_ok:
                logger.warning(
                    "Hermes HA WebSocket receiver stopped unexpectedly; retrying in %.1fs",
                    backoff,
                )
            time.sleep(backoff)
            backoff = min(backoff * 2, _retry_max_seconds())
            self._stopped.clear()
            self._started.clear()

    async def _start_async(self) -> None:
        assert web is not None
        app = web.Application()
        app.router.add_get(self.path, self._handle_ws)
        app.router.add_get("/health", self._handle_health)
        app.router.add_get(_AUDIO_ROUTE_PATH, self._handle_audio)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port, reuse_address=True)
        await site.start()
        self._runner = runner

    async def _shutdown(self) -> None:
        runner = self._runner
        self._runner = None
        if runner is not None:
            await runner.cleanup()
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)

    async def _handle_health(self, request: "aiohttp_web.Request") -> "aiohttp_web.Response":
        assert web is not None
        return web.json_response({"type": "status", **receiver_status(self)})

    async def _handle_audio(self, request: "aiohttp_web.Request") -> "aiohttp_web.StreamResponse":
        assert web is not None
        token = _configured_token()
        if token:
            provided = str(request.query.get("token", ""))
            if not hmac.compare_digest(provided, token):
                raise web.HTTPUnauthorized(text="Invalid audio token")

        filename = Path(str(request.match_info.get("filename", ""))).name
        if not filename:
            raise web.HTTPNotFound(text="Missing audio filename")

        allowed_dirs = [
            Path.home() / ".hermes" / "voice_cache",
            Path.home() / ".hermes" / "audio_cache",
        ]
        audio_path = next(
            ((directory / filename) for directory in allowed_dirs if (directory / filename).is_file()),
            None,
        )
        if audio_path is None:
            raise web.HTTPNotFound(text="Audio file not found")

        response = web.FileResponse(path=audio_path)
        guessed_type, _ = mimetypes.guess_type(str(audio_path))
        if guessed_type:
            response.content_type = guessed_type
        return response

    async def _handle_ws(self, request: "aiohttp_web.Request") -> "aiohttp_web.WebSocketResponse":
        assert web is not None
        assert WSMsgType is not None
        if not _auth_ok(request.headers):
            raise web.HTTPUnauthorized(text="Invalid bearer token")

        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        self._connection_opened()
        await ws.send_json({"type": "hello", "ok": True, "service": "hermes-ha-ws"})

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        payload = json.loads(msg.data)
                        if not isinstance(payload, dict):
                            raise ValueError("payload must be a JSON object")
                        msg_type = str(payload.get("type", "")).strip().lower()
                        if msg_type == "assist_query":
                            _record_message(msg_type)
                            response = await handle_assist_query_async(payload)
                        else:
                            response = handle_ha_ws_payload(payload)
                    except Exception as exc:
                        response = {"type": "error", "ok": False, "error": str(exc)}
                    await ws.send_json(response)
                elif msg.type == WSMsgType.ERROR:
                    logger.debug("HA WebSocket closed with error: %s", ws.exception())
                    break
        finally:
            self._connection_closed()
        return ws


def _ensure_watchdog() -> None:
    """Start a background watchdog that revives a dead local receiver."""
    global _WS_WATCHDOG
    with _WS_LOCK:
        if _WS_WATCHDOG and _WS_WATCHDOG.is_alive():
            return
        _WS_WATCHDOG_STOP.clear()
        _WS_WATCHDOG = threading.Thread(target=_watchdog_loop, name="hermes-ha-ws-watchdog", daemon=True)
        _WS_WATCHDOG.start()


def _watchdog_loop() -> None:
    global _WS_SERVER
    while not _WS_WATCHDOG_STOP.wait(timeout=_watchdog_interval_seconds()):
        if not _should_bind_ws_receiver():
            continue
        with _WS_LOCK:
            server = _WS_SERVER
        if server is None:
            start_ws_receiver()
            continue
        if isinstance(server, _AdoptedReceiver):
            if not server.running:
                logger.warning("Adopted Hermes HA WebSocket receiver is unhealthy; retrying local start")
                with _WS_LOCK:
                    _WS_SERVER = None
                start_ws_receiver()
            continue
        thread = getattr(server, "_thread", None)
        if thread is not None and not thread.is_alive() and not server.running:
            logger.warning("Hermes HA WebSocket receiver thread died; restarting")
            with _WS_LOCK:
                _WS_SERVER = None
            start_ws_receiver()


def _adopt_external_receiver(host: str, port: int, path: str, *, reason: str) -> "_AdoptedReceiver":
    logger.info(
        "Hermes HA WebSocket receiver on %s:%s managed externally (%s)",
        host,
        port,
        reason,
    )
    adopted = _AdoptedReceiver(host, port, path)
    global _WS_SERVER
    _WS_SERVER = adopted
    return adopted


def start_ws_receiver(host: Optional[str] = None, port: Optional[int] = None, path: Optional[str] = None) -> Optional["HermesHAWebSocketServer | _AdoptedReceiver"]:
    """Start the singleton HA WebSocket receiver if enabled."""
    global _WS_SERVER
    if not _should_bind_ws_receiver():
        resolved_host = host or os.getenv("HERMES_HA_WS_HOST", DEFAULT_WS_HOST)
        resolved_port = int(port or os.getenv("HERMES_HA_WS_PORT", str(DEFAULT_WS_PORT)))
        resolved_path = path or os.getenv("HERMES_HA_WS_PATH", DEFAULT_WS_PATH)
        with _WS_LOCK:
            if _WS_SERVER and _WS_SERVER.running:
                return _WS_SERVER
            if _probe_existing_receiver(resolved_host, resolved_port):
                return _adopt_external_receiver(
                    resolved_host,
                    resolved_port,
                    resolved_path,
                    reason="gateway owns bind in this profile",
                )
        logger.info(
            "Hermes HA WebSocket receiver bind skipped in this process; "
            "start `hermes gateway` to serve Home Assistant on port %s",
            resolved_port,
        )
        return None
    if not AIOHTTP_AVAILABLE:
        logger.warning("Hermes HA WebSocket receiver unavailable: aiohttp is not installed")
        return None

    resolved_host = host or os.getenv("HERMES_HA_WS_HOST", DEFAULT_WS_HOST)
    resolved_port = int(port or os.getenv("HERMES_HA_WS_PORT", str(DEFAULT_WS_PORT)))
    resolved_path = path or os.getenv("HERMES_HA_WS_PATH", DEFAULT_WS_PATH)

    with _WS_LOCK:
        if _WS_SERVER and _WS_SERVER.running:
            return _WS_SERVER
        if _probe_existing_receiver(resolved_host, resolved_port):
            adopted = _adopt_external_receiver(
                resolved_host,
                resolved_port,
                resolved_path,
                reason="listener already active",
            )
            _ensure_watchdog()
            return adopted
        _WS_SERVER = HermesHAWebSocketServer(resolved_host, resolved_port, resolved_path)
        _WS_SERVER.start()
        if _WS_SERVER.running:
            _register_shutdown_hook()
            _ensure_watchdog()
            return _WS_SERVER
        if _probe_existing_receiver(resolved_host, resolved_port):
            adopted = _adopt_external_receiver(
                resolved_host,
                resolved_port,
                resolved_path,
                reason="local bind failed but external listener is healthy",
            )
            _ensure_watchdog()
            return adopted
        return None


def stop_ws_receiver() -> None:
    """Stop the singleton receiver."""
    global _WS_SERVER
    _WS_WATCHDOG_STOP.set()
    with _WS_LOCK:
        server = _WS_SERVER
        _WS_SERVER = None
    if server is not None and isinstance(server, HermesHAWebSocketServer):
        server.stop()
