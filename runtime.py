from __future__ import annotations

import asyncio
import errno
import hashlib
import inspect
import json
import logging
import os
import sqlite3
import stat
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping
from urllib.parse import quote

from .core import (
    HEARTBEAT_LIVE_SECONDS,
    StateStore,
    Trigger,
    WakeTarget,
    build_root_families,
    classify_family,
)

# Creator-session provenance envelope.

PROVENANCE_OPEN_TAG = "[creator-session-provenance/v1]"
PROVENANCE_CLOSE_TAG = "[/creator-session-provenance/v1]"
PROVENANCE_REQUIRED_KEYS = ("profile", "source")
PROVENANCE_OPTIONAL_KEYS = (
    "session_id",
    "session_key",
    "conversation_id",
    "chat_id",
    "thread_id",
)
PROVENANCE_ALL_KEYS = PROVENANCE_REQUIRED_KEYS + PROVENANCE_OPTIONAL_KEYS
SUPPORTED_PROFILE = "default"
SUPPORTED_SOURCE = "discord"


@dataclass(frozen=True)
class CreatorClaim:
    """One strictly parsed creator provenance claim."""

    profile: str
    source: str
    session_id: str = ""
    session_key: str = ""
    conversation_id: str = ""
    chat_id: str = ""
    thread_id: str = ""
    provenance_sha256: str = ""


@dataclass(frozen=True)
class OriginDecision:
    """Typed origin resolution; READY retains the selected adapter identity."""

    status: str  # READY | HOLD | SKIP
    reason_code: str | None = None
    target: WakeTarget | None = None
    adapter: Any | None = None

    @property
    def ready(self) -> bool:
        return self.status == "READY"


def _canonical_claim_bytes(claim: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(claim),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def parse_creator_provenance(body: str | None) -> CreatorClaim | OriginDecision:
    """Strict full-envelope parser over the exact root body.

    Returns the single CreatorClaim or an OriginDecision HOLD/SKIP; never raises
    for unusable provenance and never consults anything but the body string.
    Profile and source authority fields are preserved byte-for-byte after JSON
    decoding; only optional selector fields are trimmed for matching.
    """
    text = body if isinstance(body, str) else ""
    opens = text.count(PROVENANCE_OPEN_TAG)
    closes = text.count(PROVENANCE_CLOSE_TAG)
    if opens == 0 and closes == 0:
        return OriginDecision("HOLD", "HOLD_PROVENANCE_MISSING")
    if opens > 1 or closes > 1:
        return OriginDecision("HOLD", "HOLD_PROVENANCE_MULTIPLE")
    if opens != 1 or closes != 1:
        return OriginDecision("HOLD", "HOLD_PROVENANCE_MALFORMED")
    # Exactly one opening and one closing tag from here.
    if text.index(PROVENANCE_OPEN_TAG) > text.index(PROVENANCE_CLOSE_TAG):
        return OriginDecision("HOLD", "HOLD_PROVENANCE_MALFORMED")
    inner = text.split(PROVENANCE_OPEN_TAG, 1)[1].split(PROVENANCE_CLOSE_TAG, 1)[0]
    payload_text = inner.strip()
    try:
        payload = json.loads(payload_text)
    except (TypeError, ValueError):
        return OriginDecision("HOLD", "HOLD_PROVENANCE_MALFORMED")
    if not isinstance(payload, dict):
        return OriginDecision("HOLD", "HOLD_PROVENANCE_MALFORMED")
    keys = set(payload.keys())
    unknown = keys - set(PROVENANCE_ALL_KEYS)
    if unknown:
        return OriginDecision("HOLD", "HOLD_PROVENANCE_MALFORMED")
    missing = [key for key in PROVENANCE_REQUIRED_KEYS if key not in keys]
    if missing:
        return OriginDecision("HOLD", "HOLD_PROVENANCE_MALFORMED")
    # Duplicate JSON keys are rejected (json.loads would silently collapse them).
    if _json_has_duplicate_keys(payload_text):
        return OriginDecision("HOLD", "HOLD_PROVENANCE_MALFORMED")
    for key in PROVENANCE_ALL_KEYS:
        if key in keys:
            value = payload[key]
            if not isinstance(value, str) or not value.strip():
                return OriginDecision("HOLD", "HOLD_PROVENANCE_MALFORMED")
    # Preserve both authority fields exactly; matching does not normalize them.
    profile = payload["profile"]
    source = payload["source"]
    canonical_payload: dict[str, str] = {"profile": profile, "source": source}
    for key in PROVENANCE_OPTIONAL_KEYS:
        value = payload.get(key, "")
        if value:
            canonical_payload[key] = value
    claim = CreatorClaim(
        profile=profile,
        source=source,
        session_id=canonical_payload.get("session_id", ""),
        session_key=canonical_payload.get("session_key", ""),
        conversation_id=canonical_payload.get("conversation_id", ""),
        chat_id=canonical_payload.get("chat_id", ""),
        thread_id=canonical_payload.get("thread_id", ""),
        provenance_sha256=hashlib.sha256(_canonical_claim_bytes(canonical_payload)).hexdigest(),
    )
    return claim


def _json_has_duplicate_keys(text: str) -> bool:
    """True when any object in the JSON text repeats a key (json.loads silently collapses)."""

    def pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        seen: set[str] = set()
        for key, _ in pairs:
            if key in seen:
                raise ValueError(f"duplicate JSON key: {key}")
            seen.add(key)
        return dict(pairs)

    try:
        json.loads(text, object_pairs_hook=pairs_hook)
    except ValueError as exc:
        if "duplicate JSON key" in str(exc):
            return True
        return True  # invalid JSON caught here is also malformed
    return False


def _canonical_stored_profile(value: Any) -> str:
    """Use the host default only when the stored profile is omitted."""
    if value is None or value == "":
        return "default"
    return value if isinstance(value, str) else ""

logger = logging.getLogger(__name__)

DENIED_SQL_ACTIONS = {
    getattr(sqlite3, name)
    for name in (
        "SQLITE_INSERT",
        "SQLITE_UPDATE",
        "SQLITE_DELETE",
        "SQLITE_CREATE_INDEX",
        "SQLITE_CREATE_TABLE",
        "SQLITE_CREATE_TEMP_INDEX",
        "SQLITE_CREATE_TEMP_TABLE",
        "SQLITE_CREATE_TEMP_TRIGGER",
        "SQLITE_CREATE_TEMP_VIEW",
        "SQLITE_CREATE_TRIGGER",
        "SQLITE_CREATE_VIEW",
        "SQLITE_DROP_INDEX",
        "SQLITE_DROP_TABLE",
        "SQLITE_DROP_TEMP_INDEX",
        "SQLITE_DROP_TEMP_TABLE",
        "SQLITE_DROP_TEMP_TRIGGER",
        "SQLITE_DROP_TEMP_VIEW",
        "SQLITE_DROP_TRIGGER",
        "SQLITE_DROP_VIEW",
        "SQLITE_ALTER_TABLE",
        "SQLITE_REINDEX",
        "SQLITE_ANALYZE",
        "SQLITE_ATTACH",
        "SQLITE_DETACH",
        "SQLITE_TRANSACTION",
        "SQLITE_SAVEPOINT",
    )
    if hasattr(sqlite3, name)
}

DEFAULT_ROUTE_STATE = Path.home() / ".hermes/state/kanban-discord-visibility/state.json"
DEFAULT_STATE_DB = Path.home() / ".hermes/state/kanban-origin-review/state.sqlite3"
DEFAULT_PARENT_CHANNEL_ID = ""
DEFAULT_POLL_SECONDS = 180.0


def _read_owned_regular_json(path: Path) -> dict[str, Any]:
    st = path.lstat()
    if not stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode):
        raise RuntimeError(f"unsafe JSON state path: {path}")
    if st.st_uid != os.getuid() or (st.st_mode & 0o002):
        raise RuntimeError(f"unsafe JSON state ownership/mode: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise RuntimeError(f"invalid JSON object: {path}")
    return data


def load_board_threads(path: Path = DEFAULT_ROUTE_STATE) -> dict[str, str]:
    if not path.exists():
        return {}
    data = _read_owned_regular_json(path)
    result: dict[str, str] = {}
    boards = data.get("boards")
    if not isinstance(boards, dict):
        return result
    for board, raw in boards.items():
        if not isinstance(board, str) or not isinstance(raw, dict):
            continue
        thread_id = str(raw.get("board_thread_id") or "").strip()
        if thread_id.isdigit():
            result[board] = thread_id
    return result


def discover_board_paths() -> dict[str, Path]:
    from hermes_cli import kanban_db as kb

    result: dict[str, Path] = {}
    for meta in kb.list_boards(include_archived=False):
        slug = str(meta.get("slug") or "").strip()
        if not slug:
            continue
        raw_path = meta.get("db_path")
        result[slug] = Path(raw_path).expanduser() if raw_path else kb.kanban_db_path(slug)
    return result


class _BoundReadOnlyConnection(sqlite3.Connection):
    """SQLite connection that owns its isolated snapshot directory."""

    _snapshot_tmp: tempfile.TemporaryDirectory[str] | None = None

    def close(self) -> None:
        snapshot_tmp, self._snapshot_tmp = self._snapshot_tmp, None
        try:
            super().close()
        finally:
            if snapshot_tmp is not None:
                snapshot_tmp.cleanup()


def _readonly_authorizer(
    action: int,
    arg1: str | None,
    arg2: str | None,
    _database: str | None,
    _source: str | None,
) -> int:
    if action == getattr(sqlite3, "SQLITE_PRAGMA", -1):
        pragma = str(arg1 or "").lower()
        if pragma == "table_info" or (pragma == "query_only" and arg2 is None):
            return sqlite3.SQLITE_OK
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_DENY if action in DENIED_SQL_ACTIONS else sqlite3.SQLITE_OK


def _open_nofollow_binding(path: Path) -> tuple[Path, int, int]:
    absolute = Path(os.path.abspath(os.path.expanduser(str(path))))
    if not absolute.is_absolute() or len(absolute.parts) < 2:
        raise RuntimeError(f"invalid SQLite path: {path}")
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if not nofollow:
        raise RuntimeError("O_NOFOLLOW is required for read-only SQLite binding")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | nofollow | getattr(os, "O_CLOEXEC", 0)
    file_flags = os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0)
    directory_fd = os.open(absolute.anchor, directory_flags)
    try:
        for component in absolute.parts[1:-1]:
            try:
                next_fd = os.open(component, directory_flags, dir_fd=directory_fd)
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise RuntimeError(f"symlinked SQLite path component rejected: {absolute}") from exc
                raise
            os.close(directory_fd)
            directory_fd = next_fd
        leaf = absolute.name
        try:
            leaf_fd = os.open(leaf, file_flags, dir_fd=directory_fd)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise RuntimeError(f"symlinked SQLite leaf rejected: {absolute}") from exc
            raise
        leaf_stat = os.fstat(leaf_fd)
        if not stat.S_ISREG(leaf_stat.st_mode):
            os.close(leaf_fd)
            raise RuntimeError(f"SQLite path is not a regular file: {absolute}")
        current = os.stat(leaf, dir_fd=directory_fd, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (leaf_stat.st_dev, leaf_stat.st_ino):
            os.close(leaf_fd)
            raise RuntimeError(f"SQLite leaf changed during binding: {absolute}")
        return absolute, directory_fd, leaf_fd
    except Exception:
        os.close(directory_fd)
        raise


def _leaf_identity(directory_fd: int, leaf: str) -> tuple[int, int, int, int, int] | None:
    try:
        value = os.stat(leaf, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(value.st_mode):
        raise RuntimeError(f"symlinked SQLite snapshot leaf rejected: {leaf}")
    if not stat.S_ISREG(value.st_mode):
        raise RuntimeError(f"SQLite snapshot leaf is not regular: {leaf}")
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _stream_bound_once(fd: int, label: str, destination: Path | None = None) -> tuple[tuple[int, int, int, int, int], int, str]:
    def identity() -> tuple[int, int, int, int, int]:
        value = os.fstat(fd)
        if not stat.S_ISREG(value.st_mode):
            raise RuntimeError(f"SQLite snapshot source is not regular: {label}")
        return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns

    before = identity()
    digest = hashlib.sha256()
    offset = 0
    output_fd: int | None = None
    if destination is not None:
        output_fd = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
    remaining = before[2]
    try:
        while remaining:
            chunk = os.pread(fd, min(1024 * 1024, remaining), offset)
            if not chunk:
                raise RuntimeError(f"SQLite snapshot source ended early: {label}")
            digest.update(chunk)
            if output_fd is not None:
                view = memoryview(chunk)
                while view:
                    written = os.write(output_fd, view)
                    view = view[written:]
            offset += len(chunk)
            remaining -= len(chunk)
    finally:
        if output_fd is not None:
            os.close(output_fd)
    after = identity()
    if before != after or offset != before[2]:
        raise RuntimeError(f"SQLite snapshot source changed while reading: {label}")
    return before, offset, digest.hexdigest()


def _copy_bound_stable(fd: int, label: str, destination: Path | None = None) -> None:
    first = _stream_bound_once(fd, label, destination)
    second = _stream_bound_once(fd, label)
    if first != second:
        raise RuntimeError(f"SQLite snapshot bytes changed while copying: {label}")


def _open_optional_leaf(directory_fd: int, leaf: str) -> int | None:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(leaf, flags, dir_fd=directory_fd)
    except FileNotFoundError:
        return None
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise RuntimeError(f"symlinked SQLite snapshot leaf rejected: {leaf}") from exc
        raise
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise RuntimeError(f"SQLite snapshot leaf is not regular: {leaf}")
    return fd


def _isolated_snapshot(path: Path, attempts: int = 3) -> tuple[tempfile.TemporaryDirectory[str], Path]:
    absolute, directory_fd, main_fd = _open_nofollow_binding(path)
    try:
        main_leaf = absolute.name
        wal_leaf = main_leaf + "-wal"
        shm_leaf = main_leaf + "-shm"
        for _ in range(attempts):
            side_fds: dict[str, int | None] = {}
            snapshot_tmp: tempfile.TemporaryDirectory[str] | None = None
            try:
                initial_main = _leaf_identity(directory_fd, main_leaf)
                main_stat = os.fstat(main_fd)
                if initial_main is None or initial_main[:2] != (main_stat.st_dev, main_stat.st_ino):
                    continue
                initial_sides = {
                    wal_leaf: _leaf_identity(directory_fd, wal_leaf),
                    shm_leaf: _leaf_identity(directory_fd, shm_leaf),
                }
                for leaf, initial in initial_sides.items():
                    fd = _open_optional_leaf(directory_fd, leaf)
                    side_fds[leaf] = fd
                    if (fd is None) != (initial is None):
                        raise RuntimeError(f"SQLite sidecar presence raced: {leaf}")
                    if fd is not None:
                        value = os.fstat(fd)
                        if initial is None or initial[:2] != (value.st_dev, value.st_ino):
                            raise RuntimeError(f"SQLite sidecar replacement raced: {leaf}")
                snapshot_tmp = tempfile.TemporaryDirectory(prefix="kanban-origin-review-ro-")
                os.chmod(snapshot_tmp.name, 0o700)
                snapshot = Path(snapshot_tmp.name) / "snapshot.db"
                _copy_bound_stable(main_fd, str(absolute), snapshot)
                wal_fd = side_fds[wal_leaf]
                if wal_fd is not None:
                    _copy_bound_stable(wal_fd, wal_leaf, Path(str(snapshot) + "-wal"))
                shm_fd = side_fds[shm_leaf]
                if shm_fd is not None:
                    _copy_bound_stable(shm_fd, shm_leaf)
                final_main = _leaf_identity(directory_fd, main_leaf)
                final_sides = {
                    wal_leaf: _leaf_identity(directory_fd, wal_leaf),
                    shm_leaf: _leaf_identity(directory_fd, shm_leaf),
                }
                if final_main != initial_main or final_sides != initial_sides:
                    snapshot_tmp.cleanup()
                    snapshot_tmp = None
                    continue
            except RuntimeError:
                if snapshot_tmp is not None:
                    snapshot_tmp.cleanup()
                continue
            finally:
                for fd in side_fds.values():
                    if fd is not None:
                        os.close(fd)
            assert snapshot_tmp is not None
            return snapshot_tmp, snapshot
        raise RuntimeError(f"SQLite database changed during read-only snapshot: {absolute}")
    finally:
        os.close(main_fd)
        os.close(directory_fd)


def _connect_ro(path: Path) -> sqlite3.Connection:
    snapshot_tmp, snapshot = _isolated_snapshot(path)
    uri = "file:" + quote(str(snapshot), safe="/") + "?mode=ro"
    conn: _BoundReadOnlyConnection | None = None
    try:
        conn = sqlite3.connect(
            uri,
            uri=True,
            timeout=10,
            factory=_BoundReadOnlyConnection,
        )
        conn._snapshot_tmp = snapshot_tmp
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        if int(conn.execute("PRAGMA query_only").fetchone()[0]) != 1:
            raise RuntimeError("SQLite query_only readback failed")
        conn.set_authorizer(_readonly_authorizer)
        return conn
    except Exception:
        if conn is not None:
            conn.close()
        else:
            snapshot_tmp.cleanup()
        raise


def read_board_graph(path: Path) -> tuple[list[dict[str, Any]], list[tuple[str, str]]]:
    conn = _connect_ro(path)
    try:
        task_columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        required = {"id", "title", "status", "assignee"}
        missing = required - task_columns
        if missing:
            raise RuntimeError(f"tasks schema missing {sorted(missing)}")

        def expr(name: str, fallback: str = "NULL") -> str:
            return name if name in task_columns else f"{fallback} AS {name}"

        rows = [
            dict(row)
            for row in conn.execute(
                "SELECT id,title,body,status,assignee,"
                + ",".join(
                    [
                        expr("session_id"),
                        expr("created_at"),
                        expr("block_kind"),
                        expr("current_run_id"),
                        expr("last_heartbeat_at"),
                        expr("result"),
                    ]
                )
                + " FROM tasks ORDER BY id"
            ).fetchall()
        ]

        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        links: list[tuple[str, str]] = []
        if "task_links" in tables:
            links = [
                (str(row["parent_id"]), str(row["child_id"]))
                for row in conn.execute("SELECT parent_id,child_id FROM task_links ORDER BY parent_id,child_id")
            ]

        latest: dict[tuple[str, str], dict[str, Any]] = {}
        if "task_events" in tables:
            for event in conn.execute(
                "SELECT task_id,kind,payload FROM task_events "
                "WHERE kind IN ('completed','blocked') ORDER BY id"
            ):
                payload: dict[str, Any] = {}
                if event["payload"]:
                    try:
                        parsed = json.loads(event["payload"])
                        if isinstance(parsed, dict):
                            payload = parsed
                    except (TypeError, ValueError):
                        pass
                latest[(str(event["task_id"]), str(event["kind"]))] = payload
        for row in rows:
            tid = str(row["id"])
            blocked = latest.get((tid, "blocked"), {})
            completed = latest.get((tid, "completed"), {})
            row["block_reason"] = blocked.get("reason") if row.get("status") == "blocked" else None
            row["summary"] = completed.get("summary") or row.get("result")
        return rows, links
    finally:
        conn.close()


# Compatibility wrapper for read-only consumers of the previous API.
def read_board_tasks(path: Path) -> list[dict[str, Any]]:
    return read_board_graph(path)[0]


def _session_entry_to_target(entry: Any) -> WakeTarget | None:
    source = getattr(entry, "origin", None)
    if source is None:
        return None
    raw_platform = getattr(source, "platform", None)
    platform = str(getattr(raw_platform, "value", raw_platform) or "").strip().lower()
    chat_id = getattr(source, "chat_id", None)
    chat_type = getattr(source, "chat_type", None)
    session_key = getattr(entry, "session_key", None)
    session_id = getattr(entry, "session_id", None)
    if not all(isinstance(value, str) and value for value in (chat_id, chat_type, session_key, session_id)):
        return None
    if platform != "discord" or chat_type not in {"dm", "group", "channel", "thread"}:
        return None
    raw_thread_id = getattr(source, "thread_id", None)
    if raw_thread_id is None or raw_thread_id == "":
        thread_id = None
    elif isinstance(raw_thread_id, str):
        thread_id = raw_thread_id
    else:
        return None
    if not session_id or not session_key:
        return None
    # Preserve the stored profile representation; canonical matching is separate.
    raw_profile = getattr(source, "profile", None)
    if raw_profile is not None and not isinstance(raw_profile, str):
        return None
    profile = raw_profile
    # Preserve a string parent identifier byte-for-byte; reject other types.
    raw_parent_chat_id = getattr(source, "parent_chat_id", None)
    if raw_parent_chat_id is not None and not isinstance(raw_parent_chat_id, str):
        return None
    stored_parent_chat_id = raw_parent_chat_id
    return WakeTarget(
        kind="creator_session",
        key=f"discord:{profile or 'default'}:{session_key}",
        session_id=session_id,
        session_key=session_key,
        platform=platform,
        chat_id=chat_id,
        chat_name=getattr(source, "chat_name", None),
        chat_type=chat_type,
        thread_id=thread_id,
        parent_chat_id=stored_parent_chat_id,
        user_id=str(getattr(source, "user_id", None) or "").strip() or None,
        user_name=str(getattr(source, "user_name", None) or "").strip() or None,
        user_id_alt=str(getattr(source, "user_id_alt", None) or "").strip() or None,
        chat_id_alt=str(getattr(source, "chat_id_alt", None) or "").strip() or None,
        chat_topic=str(getattr(source, "chat_topic", None) or "").strip() or None,
        scope_id=str(getattr(source, "scope_id", None) or "").strip() or None,
        guild_id=str(getattr(source, "guild_id", None) or "").strip() or None,
        profile=profile,
    )


def resolve_creator_target(runner: Any, claim: CreatorClaim) -> OriginDecision:
    """Resolve the claim against the current SessionStore only.

    The block is the sole origin authority: no birth/OriginBinding/board-route
    corroboration, no legacy fallback. Scope gate first, then conjunctive
    selector matching over the store snapshot, then destination validation
    including the installed boolean-property adapter readiness contract.
    """
    # Scope gate first: both authority fields are byte-for-byte literals; no
    # case or whitespace normalization is applied.
    if claim.profile != SUPPORTED_PROFILE or claim.source != SUPPORTED_SOURCE:
        return OriginDecision("SKIP", "SKIP_UNSUPPORTED_SOURCE")
    store = getattr(runner, "session_store", None)
    lock = getattr(store, "_lock", None)
    ensure_loaded = getattr(store, "_ensure_loaded_locked", None)
    selectors: dict[str, str] = {}
    for key in ("session_id", "session_key", "chat_id", "thread_id"):
        value = getattr(claim, key, "")
        if isinstance(value, str) and value:
            selectors[key] = value
    # conversation_id has no general current-host mapping in this build.
    if str(getattr(claim, "conversation_id") or "").strip():
        return OriginDecision("HOLD", "HOLD_SESSION_UNRESOLVABLE")
    if not selectors:
        return OriginDecision("HOLD", "HOLD_SESSION_UNRESOLVABLE")
    if store is None or lock is None or not callable(ensure_loaded):
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    matches: list[tuple[str, WakeTarget]] = []
    with lock:
        try:
            ensure_loaded()
        except Exception:
            return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
        entries = getattr(store, "_entries", None)
        if not isinstance(entries, dict):
            return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
        for entry in entries.values():
            target = _session_entry_to_target(entry)
            if target is None:
                continue
            stored_profile = _canonical_stored_profile(getattr(entry.origin, "profile", None))
            if stored_profile != claim.profile:
                continue
            if selectors.get("session_id") and selectors["session_id"] != target.session_id:
                continue
            if selectors.get("session_key") and selectors["session_key"] != target.session_key:
                continue
            if selectors.get("chat_id") and selectors["chat_id"] != target.chat_id:
                continue
            if selectors.get("thread_id"):
                supplied = selectors["thread_id"]
                stored_thread = str(target.thread_id or "").strip()
                if stored_thread:
                    if supplied != stored_thread:
                        continue
                elif target.chat_type == "thread" and supplied == target.chat_id:
                    # Discord thread-as-chat shape: origin.chat_id is the thread id.
                    pass
                else:
                    continue
            matches.append((str(getattr(entry, "session_key", "") or ""), target))
    if not matches:
        return OriginDecision("HOLD", "HOLD_SESSION_UNRESOLVABLE")
    if len(matches) > 1:
        return OriginDecision("HOLD", "HOLD_SESSION_AMBIGUOUS")
    target = matches[0][1]
    # Keep the stored profile representation after canonical matching.
    # Require the canonical runner session key and an available adapter.
    # Both capabilities are part of the destination contract.  Do not let a
    # partially initialized runner leak READY merely because the store row matched.
    key_fn = getattr(runner, "_session_key_for_source", None)
    adapter_fn = getattr(runner, "_authorization_adapter", None)
    if not callable(key_fn) or not callable(adapter_fn):
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    try:
        from gateway.config import Platform
        from gateway.session import SessionSource
    except Exception:
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    try:
        source = SessionSource(
            platform=Platform(target.platform),
            chat_id=target.chat_id,
            chat_name=target.chat_name,
            chat_type=target.chat_type,
            user_id=target.user_id,
            user_name=target.user_name,
            thread_id=target.thread_id,
            chat_topic=target.chat_topic,
            user_id_alt=target.user_id_alt,
            chat_id_alt=target.chat_id_alt,
            scope_id=target.scope_id,
            guild_id=target.guild_id,
            parent_chat_id=target.parent_chat_id,
            profile=target.profile,
        )
        canonical = key_fn(source)
    except Exception:
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    if canonical != target.session_key:
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    # Resolve once and retain this exact adapter object for the dispatch seam.
    # The injector will fetch it again immediately before handle_message and
    # require object identity plus a fresh readiness check.
    try:
        # Canonical profile derivation for adapter lookup only; the stored
        # representation stays raw in the target/event.  The installed host
        # canonicalizes None/"" to the default identity, mirrored here
        # explicitly without ambient inference.
        adapter = adapter_fn(
            Platform(target.platform), target.profile or SUPPORTED_PROFILE
        )
    except Exception:
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    if adapter is None or not callable(getattr(adapter, "handle_message", None)):
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    if not _adapter_is_connected(adapter):
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    return OriginDecision("READY", None, target, adapter)


def _adapter_is_connected(adapter: Any) -> bool:
    """Read the installed boolean property ABI and fail closed."""
    try:
        value = getattr(adapter, "is_connected")
    except Exception:
        return False
    return value is True


def build_fallback_target(
    thread_id: str,
    *,
    session_id: str = "",
    parent_channel_id: str = DEFAULT_PARENT_CHANNEL_ID,
    board: str = "",
) -> WakeTarget:
    return WakeTarget.fallback_thread(
        thread_id,
        session_id=session_id,
        parent_chat_id=parent_channel_id or None,
        board=board,
    )


def _clip(value: Any, limit: int) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: max(0, limit - 1)] + "…"


MAX_INJECTION_CHARS = 3900
INVARIANT_ACTION_SUFFIX = "\n".join(
    [
        "이 이벤트는 heartbeat가 멈춘 authoritative root 계열을 기존 default 대화로 되돌리는 내부 복구 turn입니다.",
        "이 세션의 기존 대화 기록과 해당 root 계열의 현재 Kanban handoff·등록 산출물을 먼저 읽고 실제 정지 원인을 확인하십시오.",
        "이 turn은 이 기존 세션의 정상 Kanban·파일·터미널 도구를 유지합니다. 확인된 문제를 기존 권한 범위에서 진단·수정하고 결과를 검증하십시오.",
        "Root가 done으로 정상 종료된 경우에도 Kanban 상태만으로 성공을 간주하지 마십시오.",
        "정확한 root handoff, 독립 QA·검토 근거, 등록 산출물·첨부파일 및 실제 산출물을 직접 열어 존재·식별자/해시(제공된 경우)·가독성·필수 도메인 QA·원 요청 충족 여부를 평가하십시오.",
        "기준을 충족하면 검증한 정확한 최종 결과·산출물을 provenance로 확인된 origin 세션에 즉시 전달하십시오. 중복 전달이나 승인되지 않은 제3자·외부 전송은 하지 마십시오.",
        "기준 미달 또는 불완전하면 성공으로 보고하거나 미완성 산출물을 전달하지 말고, 기존 제품 정체성·범위·권한·후보 계보·승인 경계 안에서 최소한의 개선·재검증 작업을 자율적으로 시작한 뒤 전달 전 다시 평가하십시오.",
        "다른 root 계열은 독립 감시 대상이므로 이 보고에 섞지 마십시오.",
        "credential, 결제, 외부 게시, 파괴적 조치, production 변경, Gateway 재시작은 기존 승인 경계를 그대로 따르십시오.",
        "마지막 답변에는 확인한 상태, 수행한 수정, 검증 결과, 남은 실제 차단 요인만 간결한 한국어로 전달하십시오. 내부 이벤트 문구를 그대로 반복하지 마십시오.",
    ]
)


def build_injection_text(trigger: Trigger) -> str:
    snapshot = trigger.snapshot
    blocked_lines: list[str] = []
    completed_lines: list[str] = []
    for task in snapshot.tasks:
        if task["status"] == "blocked" and len(blocked_lines) < 10:
            blocked_lines.append(
                f"- {task['id']} @{_clip(task.get('assignee'), 30)} {_clip(task.get('title'), 100)} | "
                f"{_clip(task.get('block_kind') or 'blocked', 30)}: "
                f"{_clip(task.get('block_reason') or '사유 미기록', 180)}"
            )
        elif task["status"] == "done" and len(completed_lines) < 8:
            completed_lines.append(
                f"- {task['id']} {_clip(task.get('title'), 100)} | "
                f"{_clip(task.get('summary') or '완료 handoff 미요약', 180)}"
            )
    counts = ", ".join(f"{key}={value}" for key, value in snapshot.counts.items()) or "none"
    sections = [
        f"[kanban-origin-review:{trigger.review_key}]",
        "[내부 Kanban root 계열 상태 주입]",
        f"보드: {trigger.board}",
        f"Root: {trigger.root_task_id} {_clip(trigger.root_title, 120)}",
        f"감시 범위: root 및 descendant {len(snapshot.tasks)}개 카드",
        f"판정: {trigger.outcome} (heartbeat 기준 활성 카드 없음, 연속 2회)",
        f"Heartbeat 기준: running + 최근 {HEARTBEAT_LIVE_SECONDS // 60}분 이내 heartbeat",
        f"상태 수: {counts}",
        f"상태 식별자: {trigger.fingerprint[:16]}",
        f"전달 대상: {trigger.target.kind}",
    ]
    if blocked_lines:
        sections.extend(["", "Blocked 카드:", *blocked_lines])
    if completed_lines:
        sections.extend(["", "완료 카드 요약:", *completed_lines])
    prefix = "\n".join(sections)
    available = max(0, MAX_INJECTION_CHARS - len(INVARIANT_ACTION_SUFFIX) - 2)
    if len(prefix) > available:
        prefix = prefix[: max(0, available - 1)] + "…"
    return prefix + "\n\n" + INVARIANT_ACTION_SUFFIX


def load_origin_targets_from_store(
    session_store: Any,
    session_ids: list[str] | tuple[str, ...] | set[str],
) -> dict[str, WakeTarget]:
    wanted = {str(value or "").strip() for value in session_ids if str(value or "").strip()}
    if not wanted or session_store is None:
        return {}
    lock = getattr(session_store, "_lock", None)
    ensure_loaded = getattr(session_store, "_ensure_loaded_locked", None)
    if lock is None or not callable(ensure_loaded):
        return {}
    result: dict[str, WakeTarget] = {}
    with lock:
        ensure_loaded()
        entries = getattr(session_store, "_entries", None)
        if not isinstance(entries, dict):
            return {}
        for entry in entries.values():
            session_id = str(getattr(entry, "session_id", None) or "").strip()
            if session_id not in wanted:
                continue
            target = _session_entry_to_target(entry)
            if target is not None:
                result[session_id] = target
    return result


def _binding_matches(runner: Any, target: WakeTarget) -> bool:
    current = load_origin_targets_from_store(
        getattr(runner, "session_store", None), [target.session_id]
    ).get(target.session_id)
    if current is None:
        return False
    return (
        current.session_id == target.session_id
        and current.session_key == target.session_key
        and current.platform == target.platform
        and current.chat_id == target.chat_id
        and current.chat_name == target.chat_name
        and current.chat_type == target.chat_type
        and current.thread_id == target.thread_id
        and current.parent_chat_id == target.parent_chat_id
        and current.user_id == target.user_id
        and current.user_name == target.user_name
        and current.user_id_alt == target.user_id_alt
        and current.chat_id_alt == target.chat_id_alt
        and current.chat_topic == target.chat_topic
        and current.scope_id == target.scope_id
        and current.guild_id == target.guild_id
        and current.profile == target.profile
    )


# A missed old marker stays uncertain; never scan an unbounded transcript or reinject.
CONFIRMATION_RECENT_ROWS = 256


def _has_durable_acceptance(runner: Any, trigger: Trigger) -> bool:
    store = getattr(runner, "session_store", None)
    resolver = getattr(store, "_db_for_key", None)
    if not callable(resolver):
        return False
    # The runner's ambient DB need not own this route in a multiplexed Gateway.
    # Resolve and use the thread-safe SessionDB in this offloaded operation.
    db = resolver(trigger.target.session_key)
    checker = getattr(db, "has_platform_message_id", None)
    loader = getattr(db, "get_messages", None)
    if callable(checker) and checker(trigger.target.session_id, trigger.review_key):
        return True
    if not callable(loader):
        return False
    # load_transcript is MODEL replay: it follows compression tips, filters archived
    # rows and repairs alternation. It is not evidence for an exact pinned session.
    history = loader(
        trigger.target.session_id, include_inactive=True,
        limit=CONFIRMATION_RECENT_ROWS, latest=True,
    )
    marker = f"[kanban-origin-review:{trigger.review_key}]"
    return any(
        isinstance(row, Mapping)
        and row.get("session_id") == trigger.target.session_id
        and row.get("role") == "user"
        and row.get("display_kind") == "internal_notification"
        and row.get("platform_message_id") in (None, "", trigger.review_key)
        and isinstance(row.get("content"), str)
        and (row["content"] == marker or row["content"].startswith(marker + "\n"))
        for row in (history or ())
    )


async def trigger_is_confirmed(runner: Any, trigger: Trigger) -> bool:
    if not trigger.review_key or not trigger.target.session_id or not trigger.target.session_key:
        return False
    if not _binding_matches(runner, trigger.target):
        return False
    try:
        durable = await asyncio.to_thread(_has_durable_acceptance, runner, trigger)
    except Exception:
        logging.getLogger(__name__).debug("durable acceptance lookup failed", exc_info=True)
        return False
    return bool(durable) and _binding_matches(runner, trigger.target)


async def inject_trigger(
    runner: Any,
    trigger: Trigger,
    *,
    parent_channel_id: str = DEFAULT_PARENT_CHANNEL_ID,
    confirmation_timeout: float = 30.0,
    confirmation_poll: float = 0.25,
    resolved_adapter: Any | None = None,
) -> bool:
    from gateway.config import Platform
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.session import SessionSource

    target = trigger.target
    if not target.session_id or not target.session_key:
        raise RuntimeError("persisted target session identity is unavailable")
    if not _binding_matches(runner, target):
        raise RuntimeError("target session binding is stale")
    try:
        platform = Platform(target.platform)
    except ValueError as exc:
        raise RuntimeError(f"unsupported wake platform: {target.platform}") from exc
    source = SessionSource(
        platform=platform,
        chat_id=target.chat_id,
        chat_name=target.chat_name,
        chat_type=target.chat_type,
        user_id=target.user_id,
        user_name=target.user_name,
        thread_id=target.thread_id,
        chat_topic=target.chat_topic,
        user_id_alt=target.user_id_alt,
        chat_id_alt=target.chat_id_alt,
        scope_id=target.scope_id,
        guild_id=target.guild_id,
        # Preserve the selected source's parent identifier exactly.
        parent_chat_id=target.parent_chat_id,
        profile=target.profile,
    )
    resolved_session_key = runner._session_key_for_source(source)
    if resolved_session_key != target.session_key:
        raise RuntimeError("target session key mismatch")
    event = MessageEvent(
        text=build_injection_text(trigger),
        message_type=MessageType.TEXT,
        source=source,
        message_id=trigger.review_key,
        internal=True,
        allow_gateway_control=False,
        metadata={
            "hermes_plugin_id": "kanban-origin-review",
            "hermes_plugin_injection": True,
            "kind": "kanban_origin_review",
            "board": trigger.board,
            "root_task_id": trigger.root_task_id,
            "target_kind": target.kind,
            "outcome": trigger.outcome,
            "review_key": trigger.review_key,
            "state_fingerprint": trigger.fingerprint,
            "gateway_session_key": target.session_key,
            "gateway_session_id": target.session_id,
            "gateway_session_strict": True,
        },
    )
    # This is the only dispatch seam: refetch the live adapter after all
    # target/event work, then require retained identity and fresh readiness.
    adapter_fn = getattr(runner, "_authorization_adapter", None)
    if not callable(adapter_fn):
        raise RuntimeError("authorization adapter lookup is unavailable")
    try:
        live_adapter = adapter_fn(platform, target.profile or SUPPORTED_PROFILE)
    except Exception as exc:
        raise RuntimeError("authorization adapter lookup failed") from exc
    if resolved_adapter is not None and live_adapter is not resolved_adapter:
        raise RuntimeError("target adapter binding changed")
    if live_adapter is None:
        raise RuntimeError(f"{target.profile or 'default'} {target.platform} adapter is unavailable")
    handler = getattr(live_adapter, "handle_message", None)
    if not callable(handler):
        raise RuntimeError("target adapter handle_message is unavailable")
    if not _adapter_is_connected(live_adapter):
        raise RuntimeError("target adapter is not connected")
    await handler(event)
    deadline = asyncio.get_running_loop().time() + max(0.0, float(confirmation_timeout))
    while True:
        if await trigger_is_confirmed(runner, trigger):
            return True
        if asyncio.get_running_loop().time() >= deadline:
            return False
        await asyncio.sleep(max(0.001, float(confirmation_poll)))


class OriginReviewRuntime:
    def __init__(
        self,
        *,
        runner: Any,
        state_store: StateStore,
        route_state_path: Path = DEFAULT_ROUTE_STATE,
        board_paths: Callable[[], Mapping[str, Path]] = discover_board_paths,
        injector: Callable[..., Awaitable[bool]] = inject_trigger,
        parent_channel_id: str = DEFAULT_PARENT_CHANNEL_ID,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
    ):
        self.runner = runner
        self.state_store = state_store
        self.route_state_path = Path(route_state_path)
        self.board_paths = board_paths
        self.injector = injector
        self.parent_channel_id = parent_channel_id
        self.poll_seconds = max(1.0, float(poll_seconds))

    async def tick(self) -> dict[str, Any]:
        # The root body's provenance block is the sole targeting authority.
        paths = dict(await asyncio.to_thread(self.board_paths))
        report: dict[str, Any] = {"status": "ok", "status_code": 0, "boards": 0, "families": 0, "triggers": 0, "fallbacks": 0, "errors": []}
        # Recover receipts before observing boards.  A pending receipt was
        # durably recorded but never marked scheduled, so it can be dispatched
        # once.  Any later unfinished state is confirmation-only: blindly
        # injecting it again could create a duplicate recovery turn.
        for receipt_status, trigger in self.state_store.unfinished():
            try:
                # Only previously dispatched receipts may use confirmation-only recovery.
                if receipt_status != "pending" and await trigger_is_confirmed(self.runner, trigger):
                    self.state_store.set_status(trigger, "confirmed")
                    report["triggers"] += 1
                    continue
                if receipt_status != "pending":
                    if receipt_status != "uncertain":
                        self.state_store.set_status(
                            trigger,
                            "uncertain",
                            error="durable transcript confirmation not yet present",
                        )
                    continue
                # Re-prove pending receipts against the current root before dispatch.
                recovery, resolved_adapter = await self._recover_pending_receipt(trigger)
                if recovery == "dispatched":
                    confirmed = await self.injector(
                        self.runner,
                        trigger,
                        parent_channel_id=self.parent_channel_id,
                        resolved_adapter=resolved_adapter,
                    )
                    if confirmed:
                        self.state_store.set_status(trigger, "confirmed")
                        report["triggers"] += 1
                    else:
                        self.state_store.set_status(
                            trigger,
                            "uncertain",
                            error="injection returned without durable transcript confirmation",
                        )
                elif recovery == "confirmed":
                    self.state_store.set_status(trigger, "confirmed")
                    report["triggers"] += 1
                else:
                    self.state_store.set_status(
                        trigger,
                        "uncertain",
                        error="pending recovery provenance/target/adapter gate refused dispatch",
                    )
                    report["errors"].append(
                        f"{trigger.board}/{trigger.root_task_id}: HOLD_PENDING_ORIGIN_UNPROVEN"
                    )
            except Exception as exc:
                self.state_store.set_status(trigger, "uncertain", error=_clip(exc, 300))
                report["errors"].append(
                    f"{trigger.board}/{trigger.root_task_id}: recovery {type(exc).__name__}: {_clip(exc, 200)}"
                )
        for board, path in sorted(paths.items()):
            try:
                tasks, links = await asyncio.to_thread(read_board_graph, Path(path))
                families = build_root_families(tasks, links)
                report["boards"] += 1
                for family in families:
                    # The exact root's own body is the only origin authority.
                    # No board-route or ambient session-id fallback: unusable or
                    # ambiguous provenance produces a typed HOLD with zero injection.
                    root_body: str | None = None
                    for row in tasks:
                        if str(row.get("id") or "") == family.root_id:
                            body = row.get("body")
                            root_body = body if isinstance(body, str) else None
                            break
                    parsed = parse_creator_provenance(root_body)
                    if isinstance(parsed, OriginDecision):
                        report["errors"].append(
                            f"{board}/{family.root_id}: {parsed.reason_code}"
                        )
                        continue
                    claim = parsed
                    decision = await asyncio.to_thread(
                        resolve_creator_target, self.runner, claim
                    )
                    if not decision.ready or decision.target is None:
                        report["errors"].append(
                            f"{board}/{family.root_id}: {decision.reason_code}"
                        )
                        continue
                    target = decision.target
                    snapshot = classify_family(family.tasks)
                    trigger = self.state_store.observe(
                        board,
                        family.root_id,
                        family.root_title,
                        target,
                        snapshot,
                    )
                    report["families"] += 1
                    if trigger is None:
                        continue
                    try:
                        self.state_store.set_status(trigger, "scheduled")
                        confirmed = await self.injector(
                            self.runner,
                            trigger,
                            parent_channel_id=self.parent_channel_id,
                            resolved_adapter=decision.adapter,
                        )
                    except Exception as exc:
                        self.state_store.set_status(trigger, "uncertain", error=_clip(exc, 300))
                        report["errors"].append(
                            f"{board}/{family.root_id}: {type(exc).__name__}: {_clip(exc, 200)}"
                        )
                        logger.warning("kanban origin review delivery failed for %s/%s: %s", board, family.root_id, exc)
                    else:
                        if confirmed:
                            self.state_store.set_status(trigger, "confirmed")
                            report["triggers"] += 1
                        else:
                            self.state_store.set_status(
                                trigger,
                                "uncertain",
                                error="injection returned without durable transcript confirmation",
                            )
            except Exception as exc:
                report["errors"].append(f"{board}: {type(exc).__name__}: {_clip(exc, 200)}")
                logger.warning("kanban origin review tick failed for %s: %s", board, exc)
        if report["errors"]:
            report["status"] = "partial"
            report["status_code"] = 1
        return report

    async def _recover_pending_receipt(
        self, trigger: Trigger
    ) -> tuple[str, Any]:
        """Re-prove a persisted pending receipt against the current root.

        Returns one typed outcome plus the adapter bound at proof time (None
        unless "dispatched"):
          "confirmed"  -- durable acceptance exists and was consulted only
                          after the full current-root/target/adapter proof;
                          no dispatch.
          "dispatched" -- current-root provenance, target agreement and the
                          retained adapter all re-proved; the caller must
                          pass this exact adapter object as resolved_adapter
                          so inject_trigger rejects any swap before handle.
          "refused"    -- any gate failed; caller must hold the receipt
                          uncertain with zero handle_message.
        """
        # Prove the current origin and adapter before checking a durable marker.
        # Re-read the current canonical root body from the live board.
        try:
            paths = dict(self.board_paths())
            path = paths.get(trigger.board)
            if path is None:
                return "refused", None
            tasks, _links = await asyncio.to_thread(read_board_graph, Path(path))
        except Exception:
            return "refused", None
        root_body = None
        for row in tasks:
            if str(row.get("id") or "") == trigger.root_task_id:
                body = row.get("body")
                root_body = body if isinstance(body, str) else None
                break
        if root_body is None:
            return "refused", None
        # Same strict request-local provenance parser and resolver as a
        # fresh observation; any missing/malformed/multiple/unresolvable
        # provenance fails closed here.
        parsed = parse_creator_provenance(root_body)
        if isinstance(parsed, OriginDecision):
            return "refused", None
        try:
            decision = await asyncio.to_thread(
                resolve_creator_target, self.runner, parsed
            )
        except Exception:
            return "refused", None
        if not decision.ready or decision.target is None or decision.adapter is None:
            return "refused", None
        # Exact agreement between the newly resolved target and the
        # persisted receipt target across the full stored identity.
        live = decision.target
        persisted = trigger.target
        if (
            live.kind != persisted.kind
            or live.session_id != persisted.session_id
            or live.session_key != persisted.session_key
            or live.platform != persisted.platform
            or live.chat_id != persisted.chat_id
            or live.chat_name != persisted.chat_name
            or live.chat_type != persisted.chat_type
            or live.thread_id != persisted.thread_id
            or live.parent_chat_id != persisted.parent_chat_id
            or live.user_id != persisted.user_id
            or live.user_name != persisted.user_name
            or live.user_id_alt != persisted.user_id_alt
            or live.chat_id_alt != persisted.chat_id_alt
            or live.chat_topic != persisted.chat_topic
            or live.scope_id != persisted.scope_id
            or live.guild_id != persisted.guild_id
            or live.profile != persisted.profile
        ):
            return "refused", None
        # Same retained-current adapter identity, callable handle_message
        # and fresh is_connected literal True, mirroring the fresh-path
        # dispatch contract.
        if decision.adapter is not self._retained_pending_adapter(trigger):
            return "refused", None
        if not callable(getattr(decision.adapter, "handle_message", None)):
            return "refused", None
        if not _adapter_is_connected(decision.adapter):
            return "refused", None
        # Proof complete: only now may a durable marker confirm without
        # resend.  trigger_is_confirmed re-reads the store around the marker
        # lookup, so a concurrent target change still fails closed here.
        try:
            if await trigger_is_confirmed(self.runner, trigger):
                return "confirmed", None
        except Exception:
            return "refused", None
        return "dispatched", decision.adapter

    def _retained_pending_adapter(self, trigger: Trigger) -> Any:
        """Resolve the adapter exactly as inject_trigger will fetch it, so
        identity can be checked before dispatch without a second lookup
        racing in between."""
        try:
            from gateway.config import Platform

            adapter_fn = getattr(self.runner, "_authorization_adapter", None)
            if not callable(adapter_fn):
                return None
            return adapter_fn(
                Platform(trigger.target.platform),
                trigger.target.profile or SUPPORTED_PROFILE,
            )
        except Exception:
            return None

    async def run(self) -> None:
        logger.info("kanban origin review watcher scheduled (poll=%.1fs)", self.poll_seconds)
        shutdown_event = getattr(self.runner, "_shutdown_event", None)
        while not getattr(self.runner, "_running", False):
            if shutdown_event is not None and shutdown_event.is_set():
                return
            await asyncio.sleep(0.25)
        logger.info("kanban origin review watcher started")
        while getattr(self.runner, "_running", False):
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # defensive watcher containment
                logger.warning("kanban origin review watcher tick crashed: %s", exc)
            if shutdown_event is not None:
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=self.poll_seconds)
                    return
                except asyncio.TimeoutError:
                    pass
            else:
                await asyncio.sleep(self.poll_seconds)
