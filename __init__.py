"""Kanban root-family quiescence → creator-session review plugin."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any

from .core import StateStore
from .runtime import (
    DEFAULT_PARENT_CHANNEL_ID,
    DEFAULT_POLL_SECONDS,
    DEFAULT_ROUTE_STATE,
    DEFAULT_STATE_DB,
    OriginReviewRuntime,
)

logger = logging.getLogger(__name__)
_BOOTSTRAP_TIMEOUT_SECONDS = 120.0
_BOOTSTRAP_INTERVAL_SECONDS = 0.25
_watch_task: asyncio.Task | None = None
_runtime: OriginReviewRuntime | None = None
_bootstrap_thread: threading.Thread | None = None


def _get_gateway_runner() -> Any | None:
    try:
        from gateway.run import _gateway_runner_ref

        return _gateway_runner_ref()
    except Exception:
        return None


def _is_gateway_process() -> bool:
    if os.environ.get("_HERMES_GATEWAY") == "1":
        return True
    args = [str(arg).strip().lower() for arg in sys.argv[1:]]
    return "gateway" in args and "run" in args


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name, "").strip()
    return Path(raw).expanduser() if raw else default


def _done(task: asyncio.Task) -> None:
    global _watch_task, _runtime
    try:
        if not task.cancelled():
            exc = task.exception()
            if exc is not None:
                logger.warning("kanban origin review watcher exited: %s", exc)
    finally:
        if _runtime is not None:
            _runtime.state_store.close()
        _runtime = None
        _watch_task = None


def _start_runtime_on_loop(runner: Any) -> None:
    """Construct the state store and watcher on the Gateway event-loop thread."""
    global _watch_task, _runtime
    if _watch_task is not None and not _watch_task.done():
        return
    try:
        poll_seconds = float(
            os.environ.get("HERMES_KANBAN_ORIGIN_REVIEW_POLL_SECONDS", DEFAULT_POLL_SECONDS)
        )
        state_store = StateStore(
            _env_path("HERMES_KANBAN_ORIGIN_REVIEW_STATE_DB", DEFAULT_STATE_DB)
        )
        _runtime = OriginReviewRuntime(
            runner=runner,
            state_store=state_store,
            route_state_path=_env_path(
                "HERMES_KANBAN_ORIGIN_REVIEW_ROUTE_STATE", DEFAULT_ROUTE_STATE
            ),
            parent_channel_id=os.environ.get(
                "HERMES_KANBAN_ORIGIN_REVIEW_PARENT_CHANNEL_ID",
                DEFAULT_PARENT_CHANNEL_ID,
            ).strip(),
            poll_seconds=poll_seconds,
        )
        loop = asyncio.get_running_loop()
        _watch_task = loop.create_task(_runtime.run())
        background = getattr(runner, "_background_tasks", None)
        if isinstance(background, set):
            background.add(_watch_task)
            _watch_task.add_done_callback(background.discard)
        _watch_task.add_done_callback(_done)
        logger.warning(
            "kanban origin review watcher registered (poll=%.1fs)", poll_seconds
        )
    except Exception:
        if _runtime is not None:
            _runtime.state_store.close()
        _runtime = None
        _watch_task = None
        logger.warning("kanban origin review registration failed", exc_info=True)


def _schedule_for_runner(runner: Any | None) -> bool:
    if runner is None:
        return False
    loop = getattr(runner, "_gateway_loop", None)
    if loop is None or not loop.is_running():
        return False
    loop.call_soon_threadsafe(_start_runtime_on_loop, runner)
    return True


def _bootstrap_from_gateway_hook(*, gateway: Any | None = None, **_: Any) -> None:
    """Use the dispatch-owned runner when import-time weakref discovery misses."""
    if gateway is not None:
        _schedule_for_runner(gateway)
    return None


def _bootstrap_until_runner() -> None:
    global _bootstrap_thread
    deadline = time.monotonic() + _BOOTSTRAP_TIMEOUT_SECONDS
    try:
        while time.monotonic() < deadline:
            if _schedule_for_runner(_get_gateway_runner()):
                return
            time.sleep(_BOOTSTRAP_INTERVAL_SECONDS)
        logger.warning("kanban origin review disabled: Gateway runner unavailable")
    finally:
        _bootstrap_thread = None


def register(ctx) -> None:
    """Register one watcher, tolerating plugins loading before GatewayRunner."""
    global _bootstrap_thread

    # PluginManager is also used by short-lived CLI commands. Never import
    # gateway.run or spawn a bootstrap thread outside the actual Gateway process.
    if not _is_gateway_process():
        logger.debug("kanban origin review inactive outside Gateway process")
        return
    ctx.register_hook("pre_gateway_dispatch", _bootstrap_from_gateway_hook)
    if _watch_task is not None and not _watch_task.done():
        return
    if _schedule_for_runner(_get_gateway_runner()):
        return
    if _bootstrap_thread is not None and _bootstrap_thread.is_alive():
        return
    _bootstrap_thread = threading.Thread(
        target=_bootstrap_until_runner,
        name="kanban-origin-review-bootstrap",
        daemon=True,
    )
    _bootstrap_thread.start()
