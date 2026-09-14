from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections import Counter, deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

try:
    from hermes_cli.kanban_db import _STALE_HEARTBEAT_GAP_SECONDS as HEARTBEAT_LIVE_SECONDS
except ImportError:  # pragma: no cover - isolated source checks
    HEARTBEAT_LIVE_SECONDS = 3600

HEARTBEAT_LIVE_STATE = "heartbeat_live"
HEARTBEAT_QUIESCENT_STATE = "heartbeat_quiescent"
TERMINAL_STATES = {HEARTBEAT_QUIESCENT_STATE}
UNFINISHED_RECEIPT_STATUSES = {"pending", "scheduled", "awaiting_confirmation", "uncertain"}
FIRST_OBSERVATION_QUIESCENT_GRACE_SECONDS = 15 * 60


@dataclass(frozen=True)
class WakeTarget:
    kind: str
    key: str
    session_id: str = ""
    session_key: str = ""
    platform: str = "discord"
    chat_id: str = ""
    chat_name: str | None = None
    chat_type: str = "thread"
    thread_id: str | None = None
    parent_chat_id: str | None = None
    user_id: str | None = None
    user_name: str | None = None
    user_id_alt: str | None = None
    chat_id_alt: str | None = None
    chat_topic: str | None = None
    scope_id: str | None = None
    guild_id: str | None = None
    profile: str | None = None

    @classmethod
    def fallback_thread(
        cls,
        thread_id: str,
        *,
        session_id: str = "",
        parent_chat_id: str | None = None,
        board: str = "",
    ) -> "WakeTarget":
        tid = str(thread_id or "").strip()
        return cls(
            kind="board_thread_fallback",
            key=f"discord:default:thread:{tid}",
            session_id=str(session_id or ""),
            chat_id=tid,
            chat_name=f"Kanban / {board}" if board else "Kanban",
            chat_type="thread",
            thread_id=tid or None,
            parent_chat_id=parent_chat_id or None,
        )


@dataclass(frozen=True)
class FamilySnapshot:
    state: str
    fingerprint: str
    counts: dict[str, int]
    blocked_ids: tuple[str, ...]
    live_task_ids: tuple[str, ...]
    tasks: tuple[dict[str, Any], ...]


# Backward-compatible name for callers importing the previous type.
BoardSnapshot = FamilySnapshot


@dataclass(frozen=True)
class RootFamily:
    root_id: str
    root_title: str
    session_id: str
    task_ids: tuple[str, ...]
    tasks: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class Trigger:
    review_key: str
    board: str
    root_task_id: str
    root_title: str
    target: WakeTarget
    outcome: str
    fingerprint: str
    episode: int
    attempt: int
    snapshot: FamilySnapshot

    @property
    def thread_id(self) -> str:
        return str(self.target.thread_id or self.target.chat_id or "")


def _task_material(task: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": str(task.get("id") or ""),
        "title": str(task.get("title") or ""),
        "status": str(task.get("status") or "unknown"),
        "assignee": str(task.get("assignee") or ""),
        "session_id": str(task.get("session_id") or ""),
        "created_at": task.get("created_at"),
        "block_kind": task.get("block_kind"),
        "current_run_id": task.get("current_run_id"),
        "last_heartbeat_at": task.get("last_heartbeat_at"),
        "block_reason": task.get("block_reason"),
        "summary": task.get("summary"),
    }


def _has_live_heartbeat(task: Mapping[str, Any], *, now: int, window_seconds: int) -> bool:
    if str(task.get("status") or "") != "running":
        return False
    heartbeat = task.get("last_heartbeat_at")
    if heartbeat is None:
        return False
    try:
        return (now - int(heartbeat)) < window_seconds
    except (TypeError, ValueError):
        return False


def _is_recent_singleton_quiescence(snapshot: FamilySnapshot, *, now: int) -> bool:
    """Arm a just-finished singleton without backfilling old quiet cards."""
    if snapshot.state != HEARTBEAT_QUIESCENT_STATE or len(snapshot.tasks) != 1:
        return False
    heartbeat = snapshot.tasks[0].get("last_heartbeat_at")
    if heartbeat is None:
        return False
    try:
        age = now - int(heartbeat)
    except (TypeError, ValueError):
        return False
    return 0 <= age <= HEARTBEAT_LIVE_SECONDS + FIRST_OBSERVATION_QUIESCENT_GRACE_SECONDS


def classify_family(
    tasks: Iterable[Mapping[str, Any]],
    *,
    now: int | None = None,
    heartbeat_live_seconds: int = HEARTBEAT_LIVE_SECONDS,
) -> FamilySnapshot:
    observed_at = int(time.time()) if now is None else int(now)
    normalized = tuple(sorted((_task_material(t) for t in tasks), key=lambda row: row["id"]))
    counts = dict(sorted(Counter(row["status"] for row in normalized).items()))
    live_task_ids = tuple(
        row["id"]
        for row in normalized
        if _has_live_heartbeat(row, now=observed_at, window_seconds=int(heartbeat_live_seconds))
    )
    state = HEARTBEAT_LIVE_STATE if live_task_ids else HEARTBEAT_QUIESCENT_STATE
    payload = json.dumps(
        {"state": state, "live_task_ids": live_task_ids},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return FamilySnapshot(
        state=state,
        fingerprint=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        counts=counts,
        blocked_ids=tuple(row["id"] for row in normalized if row["status"] == "blocked"),
        live_task_ids=live_task_ids,
        tasks=normalized,
    )


# Preserve the former public entry point while changing its monitoring unit.
classify_board = classify_family


def build_root_families(
    tasks: Iterable[Mapping[str, Any]],
    links: Iterable[tuple[str, str]],
) -> tuple[RootFamily, ...]:
    normalized = {_task_material(task)["id"]: _task_material(task) for task in tasks}
    if "" in normalized:
        raise ValueError("task id must be non-empty")
    if not normalized:
        return ()

    children: dict[str, set[str]] = {tid: set() for tid in normalized}
    parents: dict[str, set[str]] = {tid: set() for tid in normalized}
    indegree: dict[str, int] = {tid: 0 for tid in normalized}
    for link in links:
        if not isinstance(link, (tuple, list)) or len(link) != 2:
            raise ValueError(f"malformed link shape: {link!r}")
        raw_parent, raw_child = link
        if not isinstance(raw_parent, str) or not raw_parent or not isinstance(raw_child, str) or not raw_child:
            raise ValueError(f"malformed link endpoints: {link!r}")
        parent, child = raw_parent, raw_child
        if parent not in normalized or child not in normalized:
            raise ValueError(f"task graph contains unknown endpoint: {parent}->{child}")
        if parent == child:
            raise ValueError(f"task graph contains self link: {parent}")
        if child not in children[parent]:
            children[parent].add(child)
            parents[child].add(parent)
            indegree[child] += 1

    queue = deque(sorted(tid for tid, degree in indegree.items() if degree == 0))
    visited = 0
    degree_work = dict(indegree)
    while queue:
        current = queue.popleft()
        visited += 1
        for child in sorted(children[current]):
            degree_work[child] -= 1
            if degree_work[child] == 0:
                queue.append(child)
    if visited != len(normalized):
        raise ValueError("task graph contains a cycle")

    families: list[RootFamily] = []
    for root_id in sorted(tid for tid, descendants in children.items() if not descendants):
        closure: set[str] = set()
        pending = [root_id]
        while pending:
            current = pending.pop()
            if current in closure:
                continue
            closure.add(current)
            pending.extend(sorted(parents[current], reverse=True))
        root = normalized[root_id]
        task_ids = tuple(sorted(closure))
        families.append(
            RootFamily(
                root_id=root_id,
                root_title=root["title"],
                session_id=root["session_id"],
                task_ids=task_ids,
                tasks=tuple(normalized[tid] for tid in task_ids),
            )
        )
    return tuple(families)


class StateStore:
    """Exactly-once-ish debounce state scoped to each board/root family."""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path), timeout=30, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS family_state (
                board TEXT NOT NULL,
                root_task_id TEXT NOT NULL,
                root_title TEXT NOT NULL,
                target_key TEXT NOT NULL,
                last_state TEXT NOT NULL,
                last_fingerprint TEXT NOT NULL,
                armed INTEGER NOT NULL DEFAULT 0,
                candidate_state TEXT,
                candidate_fingerprint TEXT,
                candidate_count INTEGER NOT NULL DEFAULT 0,
                episode INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(board, root_task_id)
            );
            CREATE TABLE IF NOT EXISTS family_receipts (
                review_key TEXT PRIMARY KEY,
                board TEXT NOT NULL,
                root_task_id TEXT NOT NULL,
                target_key TEXT NOT NULL,
                outcome TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                episode INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                status TEXT NOT NULL,
                error TEXT,
                payload_json TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            """
        )
        columns = {row[1] for row in self.conn.execute("PRAGMA table_info(family_receipts)")}
        if "payload_json" not in columns:
            self.conn.execute("ALTER TABLE family_receipts ADD COLUMN payload_json TEXT")

    def close(self) -> None:
        self.conn.close()

    def observe(
        self,
        board: str,
        root_task_id: str,
        root_title: str,
        target: WakeTarget,
        snapshot: FamilySnapshot,
    ) -> Trigger | None:
        now = int(time.time())
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT * FROM family_state WHERE board=? AND root_task_id=?",
                (board, root_task_id),
            ).fetchone()
            if row is None:
                late_singleton = _is_recent_singleton_quiescence(snapshot, now=now)
                initially_armed = snapshot.state == HEARTBEAT_LIVE_STATE or late_singleton
                self.conn.execute(
                    """INSERT INTO family_state
                       (board,root_task_id,root_title,target_key,last_state,last_fingerprint,armed,
                        candidate_state,candidate_fingerprint,candidate_count,episode,updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        board,
                        root_task_id,
                        root_title,
                        target.key,
                        snapshot.state,
                        snapshot.fingerprint,
                        1 if initially_armed else 0,
                        snapshot.state if late_singleton else None,
                        snapshot.fingerprint if late_singleton else None,
                        1 if late_singleton else 0,
                        1 if initially_armed else 0,
                        now,
                    ),
                )
                self.conn.commit()
                return None

            episode = int(row["episode"])
            if snapshot.state == HEARTBEAT_LIVE_STATE:
                if row["last_state"] != HEARTBEAT_LIVE_STATE:
                    episode += 1
                self.conn.execute(
                    """UPDATE family_state SET root_title=?,target_key=?,last_state=?,last_fingerprint=?,
                       armed=1,candidate_state=NULL,candidate_fingerprint=NULL,candidate_count=0,
                       episode=?,updated_at=? WHERE board=? AND root_task_id=?""",
                    (
                        root_title,
                        target.key,
                        snapshot.state,
                        snapshot.fingerprint,
                        episode,
                        now,
                        board,
                        root_task_id,
                    ),
                )
                self.conn.commit()
                return None

            if not int(row["armed"]):
                self.conn.execute(
                    """UPDATE family_state SET root_title=?,target_key=?,last_state=?,last_fingerprint=?,
                       updated_at=? WHERE board=? AND root_task_id=?""",
                    (root_title, target.key, snapshot.state, snapshot.fingerprint, now, board, root_task_id),
                )
                self.conn.commit()
                return None

            same_candidate = (
                row["candidate_state"] == snapshot.state
                and row["candidate_fingerprint"] == snapshot.fingerprint
            )
            candidate_count = int(row["candidate_count"]) + 1 if same_candidate else 1
            if candidate_count < 2:
                self.conn.execute(
                    """UPDATE family_state SET root_title=?,target_key=?,last_state=?,last_fingerprint=?,
                       candidate_state=?,candidate_fingerprint=?,candidate_count=?,updated_at=?
                       WHERE board=? AND root_task_id=?""",
                    (
                        root_title,
                        target.key,
                        snapshot.state,
                        snapshot.fingerprint,
                        snapshot.state,
                        snapshot.fingerprint,
                        candidate_count,
                        now,
                        board,
                        root_task_id,
                    ),
                )
                self.conn.commit()
                return None

            material = "|".join(
                [board, root_task_id, target.session_id, str(episode), snapshot.state, snapshot.fingerprint]
            )
            review_key = "korf-" + hashlib.sha256(material.encode("utf-8")).hexdigest()
            if self.conn.execute(
                "SELECT 1 FROM family_receipts WHERE review_key=?", (review_key,)
            ).fetchone() is not None:
                self.conn.execute(
                    "UPDATE family_state SET armed=0,updated_at=? WHERE board=? AND root_task_id=?",
                    (now, board, root_task_id),
                )
                self.conn.commit()
                return None
            trigger = Trigger(
                review_key=review_key,
                board=board,
                root_task_id=root_task_id,
                root_title=root_title,
                target=target,
                outcome=snapshot.state,
                fingerprint=snapshot.fingerprint,
                episode=episode,
                attempt=1,
                snapshot=snapshot,
            )
            payload_json = json.dumps(
                asdict(trigger), ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
            self.conn.execute(
                """INSERT INTO family_receipts
                   (review_key,board,root_task_id,target_key,outcome,fingerprint,episode,attempt,status,error,payload_json,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    review_key,
                    board,
                    root_task_id,
                    target.key,
                    snapshot.state,
                    snapshot.fingerprint,
                    episode,
                    1,
                    "pending",
                    None,
                    payload_json,
                    now,
                    now,
                ),
            )
            self.conn.execute(
                """UPDATE family_state SET root_title=?,target_key=?,last_state=?,last_fingerprint=?,
                   armed=0,candidate_state=NULL,candidate_fingerprint=NULL,candidate_count=0,updated_at=?
                   WHERE board=? AND root_task_id=?""",
                (root_title, target.key, snapshot.state, snapshot.fingerprint, now, board, root_task_id),
            )
            self.conn.commit()
            return trigger
        except Exception:
            self.conn.rollback()
            raise

    def unfinished(self) -> tuple[tuple[str, Trigger], ...]:
        placeholders = ",".join("?" for _ in UNFINISHED_RECEIPT_STATUSES)
        rows = self.conn.execute(
            f"SELECT status,payload_json FROM family_receipts WHERE status IN ({placeholders}) ORDER BY created_at,review_key",
            tuple(sorted(UNFINISHED_RECEIPT_STATUSES)),
        ).fetchall()
        result: list[tuple[str, Trigger]] = []
        for row in rows:
            if not row["payload_json"]:
                continue
            raw = json.loads(row["payload_json"])
            snap_raw = raw["snapshot"]
            snapshot = FamilySnapshot(
                state=snap_raw["state"],
                fingerprint=snap_raw["fingerprint"],
                counts=dict(snap_raw["counts"]),
                blocked_ids=tuple(snap_raw["blocked_ids"]),
                live_task_ids=tuple(snap_raw["live_task_ids"]),
                tasks=tuple(dict(task) for task in snap_raw["tasks"]),
            )
            result.append(
                (
                    row["status"],
                    Trigger(
                        review_key=raw["review_key"],
                        board=raw["board"],
                        root_task_id=raw["root_task_id"],
                        root_title=raw["root_title"],
                        target=WakeTarget(**raw["target"]),
                        outcome=raw["outcome"],
                        fingerprint=raw["fingerprint"],
                        episode=int(raw["episode"]),
                        attempt=int(raw["attempt"]),
                        snapshot=snapshot,
                    ),
                )
            )
        return tuple(result)

    def set_status(self, trigger: Trigger, status: str, *, error: str | None = None) -> None:
        allowed = UNFINISHED_RECEIPT_STATUSES | {"confirmed", "retryable_failed", "rejected"}
        if status not in allowed:
            raise ValueError(f"invalid receipt status: {status}")
        now = int(time.time())
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute(
                "UPDATE family_receipts SET status=?,error=?,updated_at=? WHERE review_key=?",
                (status, error, now, trigger.review_key),
            )
            if status == "retryable_failed":
                self.conn.execute(
                    """UPDATE family_state SET armed=1,candidate_state=NULL,candidate_fingerprint=NULL,
                       candidate_count=0,updated_at=? WHERE board=? AND root_task_id=?""",
                    (now, trigger.board, trigger.root_task_id),
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def finish(self, trigger: Trigger, *, success: bool, error: str | None = None) -> None:
        self.set_status(trigger, "confirmed" if success else "retryable_failed", error=error)
