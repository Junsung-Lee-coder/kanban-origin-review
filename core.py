from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections import Counter, deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

UNFINISHED_RECEIPT_STATUSES = {"pending", "scheduled", "awaiting_confirmation", "uncertain"}
# Attempt-ledger vocabulary. `attempted_at` is a nullable epoch-seconds
# stamp recording whether the durable post-fence injection ATTEMPT actually
# began for this receipt:
#   NULL  -- the fence was committed but no injector call has started; the
#            episode's single authorized physical send is still owed.
#   set   -- the attempt began (or the state predates the ledger); a second
#            physical send is FORBIDDEN under the one-deduplicated-internal-
#            message per semantic episode authority, whatever the outcome.
UNRESOLVED_ESCALATION_STATUSES = {"scheduled", "awaiting_confirmation", "uncertain"}
UNRESOLVED_REVIEW_STATUSES = UNRESOLVED_ESCALATION_STATUSES | {"pending"}

# Bounded structured-evidence labels (semantic re-entry c12). Exactly one
# label is recorded per observe() outcome and per runtime dispatch decision;
# values carry identifiers/counts only, never private message content.
EVIDENCE_DEBOUNCE = "debounce"
EVIDENCE_DEDUP_SAME_EPISODE = "dedup_same_episode"
EVIDENCE_NEW_EPISODE = "new_episode"
EVIDENCE_TARGET_HOLD = "target_hold"
EVIDENCE_DISPATCH = "dispatch"
EVIDENCE_CONFIRMATION = "confirmation"
EVIDENCE_RECOVERY_HOLD = "recovery_hold"
EVIDENCE_LABELS = frozenset(
    {
        EVIDENCE_DEBOUNCE,
        EVIDENCE_DEDUP_SAME_EPISODE,
        EVIDENCE_NEW_EPISODE,
        EVIDENCE_TARGET_HOLD,
        EVIDENCE_DISPATCH,
        EVIDENCE_CONFIRMATION,
        EVIDENCE_RECOVERY_HOLD,
    }
)
EVIDENCE_MAX_LOG_BYTES = 8192

# ---- canonical canonical posture vocabulary --------------------------------
# FamilySnapshot.state uses ONLY these values; Trigger.outcome only the three
# eligible ones. The old heartbeat_live/heartbeat_quiescent eligibility is
# Heartbeat and worker identity are diagnostic, never a posture.
FAMILY_STATES = {
    "active",
    "action_required",
    "root_completed",
    "external_blocked",
    "recovery_exhausted",
    "archived_silent",
}
ELIGIBLE_STATES = {
    "action_required",
    "heartbeat_lost",
    "root_completed",
    "external_blocked",
    "recovery_exhausted",
}
STATE_ACTIVE = "active"
STATE_ACTION_REQUIRED = "action_required"
STATE_HEARTBEAT_LOST = "heartbeat_lost"
STATE_ROOT_COMPLETED = "root_completed"
STATE_EXTERNAL_BLOCKED = "external_blocked"
STATE_RECOVERY_EXHAUSTED = "recovery_exhausted"
STATE_ARCHIVED_SILENT = "archived_silent"

# Terminal row statuses (current kanban schema).
TERMINAL_TASK_STATUSES = {"done", "archived"}
# Internal runnable/in-flight statuses: any of these suppress posture injection.
# ``triage`` is deliberately excluded: this installation has no automatic
# classifier/assignee, so every triage row requires owner action.
RUNNABLE_TASK_STATUSES = {"todo", "ready", "running", "review"}
# External posture block kinds (human postures only).
GATING_BLOCK_KINDS = {"needs_input", "capability"}

# Status-affecting lifecycle transition kinds.
#
# The vocabulary of task_events kinds that can CHANGE a row's posture.
# read_board_graph retains the newest event per (task, kind) for these
# kinds, and _authority_is_current proves every authority event against
# ALL of them — not merely the terminal kinds. Any event kind OUTSIDE this
# vocabulary that is recorded on a row is UNKNOWN currentness evidence and
# fails closed (a brand-new transition kind must never be silently
# ignored). Pure audit noise (comments/attachments/heartbeats/spawns) is
# excluded: it never contradicts a terminal posture.
STATUS_AFFECTING_EVENT_KINDS = frozenset(
    {
        "completed",
        "archived",
        "blocked",
        "gave_up",
        "claimed",
        "review_requested",
        "changes_requested",
        "unblocked",
        "timed_out",
        "crashed",
        "promoted",
    }
)

# Pure audit-noise event kinds: these record activity but
# never change a row's posture, so read_board_graph keeps them OUT of
# latest_events. A recorded kind that is neither status-affecting nor in this
# noise set is UNKNOWN currentness evidence and is surfaced for fail-closed
# currentness review.
NOISY_EVENT_KINDS = frozenset(
    {
        "created",
        "spawned",
        "heartbeat",
        "linked",
        "unlinked",
        "commented",
        "attached",
        "dependency_wait",
        "claim_extended",
        "archive_worker_termination",
        "protocol_violation",
        "model_override_set",
        "reasoning_effort_set",
    }
)

# Watcher staleness horizon for lost liveness. This REUSES the installed
# watcher's own constant (hermes_cli.kanban_db_dispatch._STALE_HEARTBEAT_GAP_
# SECONDS, mirrored by the kanban watcher); it is deliberately NOT a new
# constant. Import is best-effort so the module stays importable outside a
# full hermes installation; the fallback equals the installed value.
try:  # pragma: no cover - environment dependent
    from hermes_cli.kanban_db_dispatch import (  # type: ignore
        _STALE_HEARTBEAT_GAP_SECONDS as WATCHER_STALE_HEARTBEAT_SECONDS,
    )
except Exception:  # pragma: no cover
    WATCHER_STALE_HEARTBEAT_SECONDS = 3600


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
class GateAuthority:
    """Bounded deterministic proof for an eligible classification.

    ``kind``   -- which eligible state this authority proves.
    ``event``  -- the authoritative lifecycle event row (id/run_id/kind/payload)
                  or, for root_completed, the terminal-status receipt evidence.
    """

    kind: str
    task_id: str
    event_id: int | None
    event_run_id: int | None
    event_kind: str | None
    payload: Mapping[str, Any] | None


@dataclass(frozen=True)
class FamilySnapshot:
    state: str
    fingerprint: str
    counts: dict[str, int]
    blocked_ids: tuple[str, ...]
    live_task_ids: tuple[str, ...]
    tasks: tuple[dict[str, Any], ...]
    gate_authority: GateAuthority | None = None


# Backward-compatible name for callers importing the previous type.
BoardSnapshot = FamilySnapshot


@dataclass(frozen=True)
class RootFamily:
    root_id: str
    root_title: str
    session_id: str
    task_ids: tuple[str, ...]
    tasks: tuple[dict[str, Any], ...]
    links: tuple[tuple[str, str], ...] = ()


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
    material = {
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
    events = task.get("latest_events")
    if isinstance(events, Mapping):
        material["latest_events"] = {
            str(kind): dict(value) if isinstance(value, Mapping) else None
            for kind, value in sorted(events.items())
        }
    return material


def _event_is_wellformed(authority: Any, expected_kind: str) -> bool:
    """Strict typed-event authority: id, kind, and dict payload or explicit None."""
    if not isinstance(authority, Mapping):
        return False
    if str(authority.get("kind") or "") != expected_kind:
        return False
    event_id = authority.get("id")
    if not isinstance(event_id, int) or isinstance(event_id, bool):
        return False
    payload = authority.get("payload")
    if payload is not None and not isinstance(payload, Mapping):
        return False
    run_id = authority.get("run_id")
    if run_id is not None and not isinstance(run_id, int):
        return False
    return True


def _authority_is_current(task: Mapping[str, Any], authority: Mapping[str, Any]) -> bool:
    """The authority event must be the task's newest status-affecting event.

    Currentness is proved against the newest event of EVERY
    status-affecting lifecycle kind recorded for the row — including
    ``claimed`` and ``review_requested`` — not merely against the terminal
    kinds. Any recorded status-affecting event with a newer id proves the
    authority no longer describes the row's CURRENT posture: fail closed.
    A recorded kind OUTSIDE the known transition vocabulary is unknown
    currentness evidence and also fails closed; it is never silently
    omitted.
    """
    events = task.get("latest_events")
    if not isinstance(events, Mapping):
        return True
    event_id = authority.get("id")
    if not isinstance(event_id, int) or isinstance(event_id, bool):
        return False
    for kind, value in events.items():
        if not isinstance(value, Mapping):
            continue
        if str(kind) not in STATUS_AFFECTING_EVENT_KINDS:
            # Pure audit noise (heartbeat/comment/attachment/model or effort
            # override records) never contradicts a terminal posture: skip it.
            # Any kind that is neither status-affecting nor known noise is
            # UNKNOWN currentness evidence: fail closed rather than silently
            # ignoring a brand-new transition kind.
            if str(kind) not in NOISY_EVENT_KINDS:
                return False
            continue
        other = value.get("id")
        if isinstance(other, int) and not isinstance(other, bool) and other > event_id:
            return False
    return True


def _terminal_authority(
    task: Mapping[str, Any],
    root_task_id: str,
    member_ids: set[str],
) -> GateAuthority | None:
    """Completion proof for ONE terminal member (native-evidence D1).

    Native ``kanban_complete``/archive emit ordinary lifecycle events
    (``kind`` matched to the row's terminal status; payload such as
    ``result_len``/``summary``); no producer ever emits a synthetic
    ``payload.root_completed`` binding, so authority is derived ONLY from
    immutable native evidence:

    - the row's CURRENT status is terminal (done/archived);
    - its latest lifecycle event of the MATCHING terminal kind
      ('completed' for done, 'archived' for archived) is well-formed;
    - that event is CURRENT (its id is not older than any other recorded
      lifecycle event for the task), so the newest terminal event matches
      the row's current terminal status;
    - the task id is non-empty and a member of THIS exact family.

    Missing, malformed, stale (newer lifecycle event exists), contradicted
    (kind disagrees with current status), status-only, or out-of-family
    evidence returns None (fail closed to active). No assignee, timestamp,
    run-id, or other heuristic substitutes for the event.
    """
    status = str(task.get("status") or "")
    if status not in TERMINAL_TASK_STATUSES:
        return None
    expected_kind = "archived" if status == "archived" else "completed"
    task_id = str(task.get("id") or "")
    if not task_id or task_id not in member_ids:
        return None
    events = task.get("latest_events")
    if not isinstance(events, Mapping):
        return None
    # The terminal proof must arrive under the kind MATCHING the row's
    # current terminal status ('completed' for done, 'archived' for
    # archived). The other terminal kind beside it proves a superseded
    # posture, not the current one. Well-formedness and currency (newest
    # lifecycle event for the row) are still required: outdated proof
    # fails closed.
    authority = events.get(expected_kind)
    if not _event_is_wellformed(authority, expected_kind):
        return None
    if not _authority_is_current(task, authority):
        return None
    payload = authority.get("payload")
    if not isinstance(payload, Mapping):
        return None
    return GateAuthority(
        kind=STATE_ROOT_COMPLETED,
        task_id=task_id,
        event_id=int(authority["id"]),
        event_run_id=authority.get("run_id"),
        event_kind=str(authority.get("kind")),
        payload=dict(payload),
    )


def _external_block_authority(task: Mapping[str, Any]) -> GateAuthority | None:
    """external_blocked proof: status blocked + block_kind in the human-gate set
    + latest authoritative lifecycle event is the MATCHING blocked event."""
    status = str(task.get("status") or "")
    block_kind = task.get("block_kind")
    if status != "blocked" or block_kind not in GATING_BLOCK_KINDS:
        return None
    events = task.get("latest_events")
    if not isinstance(events, Mapping):
        return None
    authority = events.get("blocked")
    if not _event_is_wellformed(authority, "blocked"):
        return None
    if not _authority_is_current(task, authority):
        return None
    payload = authority.get("payload")
    if not isinstance(payload, Mapping) or payload.get("kind") != block_kind:
        return None
    return GateAuthority(
        kind=STATE_EXTERNAL_BLOCKED,
        task_id=str(task.get("id") or ""),
        event_id=int(authority["id"]),
        event_run_id=authority.get("run_id"),
        event_kind="blocked",
        payload=dict(payload),
    )


def _triage_authority(task: Mapping[str, Any]) -> GateAuthority | None:
    """Return stable owner-action authority for a native triage row."""
    if str(task.get("status") or "") != "triage":
        return None
    return GateAuthority(
        kind=STATE_ACTION_REQUIRED,
        task_id=str(task.get("id") or ""),
        event_id=None,
        event_run_id=None,
        event_kind="triage",
        payload={"status": "triage"},
    )


def _exhaustion_authority(task: Mapping[str, Any]) -> GateAuthority | None:
    """recovery_exhausted proof: currently blocked + latest authoritative event
    is gave_up with integer failures >= effective_limit >= 1."""
    status = str(task.get("status") or "")
    if status != "blocked":
        return None
    events = task.get("latest_events")
    if not isinstance(events, Mapping):
        return None
    authority = events.get("gave_up")
    if not _event_is_wellformed(authority, "gave_up"):
        return None
    if not _authority_is_current(task, authority):
        return None
    payload = authority.get("payload")
    if not isinstance(payload, Mapping):
        return None
    failures = payload.get("failures")
    effective_limit = payload.get("effective_limit")
    if (
        not isinstance(failures, int)
        or isinstance(failures, bool)
        or not isinstance(effective_limit, int)
        or isinstance(effective_limit, bool)
    ):
        return None
    if effective_limit < 1 or failures < effective_limit:
        return None
    return GateAuthority(
        kind=STATE_RECOVERY_EXHAUSTED,
        task_id=str(task.get("id") or ""),
        event_id=int(authority["id"]),
        event_run_id=authority.get("run_id"),
        event_kind="gave_up",
        payload=dict(payload),
    )


def _has_prior_activity_evidence(task: Mapping[str, Any]) -> bool:
    """Evidence of PRIOR worker activity for one row: a recorded heartbeat,
    a claimed/running/ended run, or a heartbeat lifecycle event."""
    hb = task.get("last_heartbeat_at")
    if isinstance(hb, (int, float)) and not isinstance(hb, bool):
        return True
    if task.get("current_run_id") is not None:
        return True
    events = task.get("latest_events")
    if isinstance(events, Mapping):
        heartbeat_event = events.get("heartbeat")
        if _event_is_wellformed(heartbeat_event, "heartbeat"):
            return True
    return False


def _liveness_lost(
    task: Mapping[str, Any], now: int | None
) -> bool:
    """Per-card lost-liveness rule (owner simplification, 2026-09-17).

    A row lost liveness when it HAS evidence of prior worker activity AND
    that activity is no longer current (last heartbeat older than the
    watcher staleness horizon) AND the row is NOT terminal. A row with NO
    prior activity never notifies (roots that never started stay silent).
    """
    if now is None:
        return False  # liveness needs a clock; callers must pass one
    if str(task.get("status") or "") in TERMINAL_TASK_STATUSES:
        return False
    if not _has_prior_activity_evidence(task):
        return False
    hb = task.get("last_heartbeat_at")
    if isinstance(hb, (int, float)) and not isinstance(hb, bool):
        return (int(now) - int(hb)) > WATCHER_STALE_HEARTBEAT_SECONDS
    # Prior activity is proven only by a claim/heartbeat event (no recorded
    # heartbeat timestamp): the row's run never reported a beat, so its
    # liveness is already beyond the horizon by construction.
    return True


def _heartbeat_lost_authority(
    tasks: tuple[dict[str, Any], ...], now: int | None
) -> GateAuthority | None:
    """heartbeat_lost proof: coalesce every lost-liveness member of the
    family into ONE notification authority listing the member ids."""
    lost_ids = sorted(
        str(row["id"]) for row in tasks if _liveness_lost(row, now)
    )
    if not lost_ids:
        return None
    return GateAuthority(
        kind=STATE_HEARTBEAT_LOST,
        task_id=lost_ids[0],
        event_id=None,
        event_run_id=None,
        event_kind="heartbeat_lost",
        payload={"member_ids": lost_ids},
    )


def classify_family(
    tasks: Iterable[Mapping[str, Any]],
    *,
    root_task_id: str | None = None,
    links: Iterable[tuple[str, str]] | None = None,
    now: int | None = None,
) -> FamilySnapshot:
    """One canonical semantic decision interface (c10).

    Precedence is fixed:
      archived root with no actionable descendant -> archived_silent
      any triage row -> action_required (classification automation is disabled)
      root + every member terminal done/archived -> root_completed
      runnable/in-flight work (todo/ready/running/review) NOT behind a
        verified external gate -> active
      blocked task with a verified matching human-gate (needs_input/capability)
        blocked event -> external_blocked
      currently blocked task whose latest authoritative event is gave_up with
        integer failures >= effective_limit >= 1 (and no runnable members
        anywhere) -> recovery_exhausted
      non-terminal member WITH prior worker activity whose liveness is no
        longer current (heartbeat older than the watcher staleness horizon)
        and NO current verified human gate on the family ->
        heartbeat_lost (lost members coalesce into one authority)
      everything else (dependency/transient blocks, retryable failures,
        malformed/missing authority, mixed ambiguity,
        unresolvable root identity) -> active

    ``root_task_id`` must name exactly one row in ``tasks``; a missing,
    unknown, or duplicate root identity fails closed to ``active``.
    """
    normalized = tuple(sorted((_task_material(t) for t in tasks), key=lambda row: row["id"]))
    counts = dict(sorted(Counter(row["status"] for row in normalized).items()))
    blocked_ids = tuple(row["id"] for row in normalized if row["status"] == "blocked")
    # Diagnostic only (kept for receipt observability); never eligibility.
    live_task_ids: tuple[str, ...] = ()

    root_rows = [row for row in normalized if row["id"] == (root_task_id or "")]
    if not root_task_id or len(root_rows) != 1:
        # Missing/unknown/duplicate root identity: fail closed.
        return _snapshot(STATE_ACTIVE, root_task_id, normalized, counts, blocked_ids, live_task_ids, None)
    root = root_rows[0]

    if root["status"] == "archived" and not any(
        _triage_authority(row) is not None
        or _external_block_authority(row) is not None
        or _exhaustion_authority(row) is not None
        for row in normalized
    ):
        return _snapshot(
            STATE_ARCHIVED_SILENT, root_task_id, normalized, counts, blocked_ids, live_task_ids, None
        )

    if root["status"] in TERMINAL_TASK_STATUSES and all(
        row["status"] in TERMINAL_TASK_STATUSES for row in normalized
    ):
        # A terminal status alone proves nothing. root_completed
        # requires AUTHENTICATED terminal authority for EVERY terminal family
        # member: a latest lifecycle event that is well-formed, current (newest
        # for its row), and kind-matched to the row's current terminal status.
        # The family passed to this classifier is the root's EXACT closure: a
        # member whose id does not belong to this closure is unknown/cross-
        # family evidence and fails closed to active (zero injection) rather
        # than being silently excluded. Missing, malformed, outdated, or
        # contradictory terminal evidence fails closed the same way.
        member_ids = {row["id"] for row in normalized}
        if len(member_ids) != len(normalized):
            # Duplicate rows for one task id are member/graph drift: the
            # exact closure carries each member exactly once. Fail closed.
            return _snapshot(
                STATE_ACTIVE,
                root_task_id,
                normalized,
                counts,
                blocked_ids,
                live_task_ids,
                None,
            )
        authorities: list[GateAuthority] = []
        for row in normalized:
            authority = _terminal_authority(row, root_task_id, member_ids)
            if authority is None:
                return _snapshot(
                    STATE_ACTIVE,
                    root_task_id,
                    normalized,
                    counts,
                    blocked_ids,
                    live_task_ids,
                    None,
                )
            authorities.append(authority)
        completion_authority = min(
            authorities, key=lambda a: (a.task_id, a.event_id if a.event_id is not None else -1)
        )
        return _snapshot(
            STATE_ROOT_COMPLETED,
            root_task_id,
            normalized,
            counts,
            blocked_ids,
            live_task_ids,
            completion_authority,
        )

    # Verified external human postures (blocked + needs_input/capability + matching
    # authoritative blocked event) require owner attention independently of
    # unrelated runnable work elsewhere in the same root family.
    gate_ids: set[str] = set()
    external_authorities: list[GateAuthority] = []
    triage_authorities: list[GateAuthority] = []
    for row in normalized:
        triage = _triage_authority(row)
        if triage is not None:
            triage_authorities.append(triage)
        authority = _external_block_authority(row)
        if authority is not None:
            gate_ids.add(row["id"])
            external_authorities.append(authority)

    # descendant-contract: rows sitting BEHIND an exhausted row (its descendant closure) are
    # parked by that row's own posture; they never count as free runnable work.
    # The exhausted row itself still yields its authority below.
    exhaustion_gate_ids: set[str] = set()
    for row in normalized:
        if _exhaustion_authority(row) is not None:
            exhaustion_gate_ids.add(row["id"])

    # links follow the board's (parent, child) orientation where the family
    # root is the sink; a member is BEHIND a posture when a parked row sits on
    # its path toward the root sink (i.e., inside its child closure).
    children: dict[str, set[str]] = {row["id"]: set() for row in normalized}
    if links is not None:
        for link in links or ():
            try:
                parent, child = link
            except (TypeError, ValueError):
                continue
            if parent in children and child in children:
                children[parent].add(child)

    def _reaches(start: str, targets: set[str]) -> bool:
        seen: set[str] = set()
        pending = [start]
        while pending:
            current = pending.pop()
            if current in targets:
                return True
            for child in children.get(current, ()):
                if child not in seen:
                    seen.add(child)
                    pending.append(child)
        return False

    gate_downstream: dict[str, bool] = {
        row["id"]: _reaches(row["id"], gate_ids) for row in normalized
    }

    def _runnable_row_blocked_behind_gate(row: dict[str, Any]) -> bool:
        return bool(gate_downstream.get(row["id"], False))

    # The parked root's own runnable status (ready/todo) is the normal waiting
    # posture and never suppresses a posture; only non-root runnable members do.
    runnable_rows = [
        row
        for row in normalized
        if row["status"] in RUNNABLE_TASK_STATUSES and row["id"] != root["id"]
    ]
    external = min(external_authorities, key=lambda a: (a.task_id, a.event_id or -1)) if external_authorities else None
    action_required = min(triage_authorities, key=lambda a: a.task_id) if triage_authorities else None

    # link-contract fail-closed: without the board link vector, member reachability behind
    # a verified external posture is unknown. A family that still carries a
    # verified human posture must therefore stay parked (external_blocked) rather
    # than let unproven runnable members defeat the posture.
    if external is not None and links is None:
        return _snapshot(
            STATE_EXTERNAL_BLOCKED,
            root_task_id,
            normalized,
            counts,
            blocked_ids,
            live_task_ids,
            external,
        )

    # A verified human/credential/capability gate needs owner attention even
    # while unrelated family members remain runnable.
    if external is not None:
        return _snapshot(
            STATE_EXTERNAL_BLOCKED,
            root_task_id,
            normalized,
            counts,
            blocked_ids,
            live_task_ids,
            external,
        )

    # Triage needs owner manipulation even while unrelated work remains runnable.
    # One deterministic family authority coalesces multiple triage rows.
    if action_required is not None:
        return _snapshot(
            STATE_ACTION_REQUIRED,
            root_task_id,
            normalized,
            counts,
            blocked_ids,
            live_task_ids,
            action_required,
        )

    # Exhaustion rows posture their own descendant closure: a runnable member whose
    # path to the root sink crosses an exhausted row is parked and does not
    # defeat the exhaustion posture. Exhausted ids are not
    # external postures, so suppression uses a dedicated map.
    def _parked_by_exhaustion(row: dict[str, Any]) -> bool:
        if not exhaustion_gate_ids:
            return False
        seen: set[str] = set()
        pending_stack = [row["id"]]
        while pending_stack:
            current = pending_stack.pop()
            if current in exhaustion_gate_ids:
                return True
            for child in children.get(current, ()):
                if child not in seen:
                    seen.add(child)
                    pending_stack.append(child)
        return False

    exhausted = next(
        (authority for row in normalized if (authority := _exhaustion_authority(row)) is not None),
        None,
    )
    # A row that lost liveness is by definition NOT current work even when
    # its status field still says todo/ready/running/review: the stale status
    # is the crash residue the rule exists to surface. Excluding lost rows
    # from free runnable keeps the rule unmaskable (a family whose only
    # runnable rows are stale still notifies) without touching live work.
    lost_row_ids = {
        str(row["id"]) for row in normalized if _liveness_lost(row, now)
    }
    free_runnable = [
        row
        for row in runnable_rows
        if not _runnable_row_blocked_behind_gate(row)
        and not _parked_by_exhaustion(row)
        and row["id"] not in lost_row_ids
    ]
    if free_runnable:
        return _snapshot(STATE_ACTIVE, root_task_id, normalized, counts, blocked_ids, live_task_ids, None)
    if exhausted is not None:
        return _snapshot(
            STATE_RECOVERY_EXHAUSTED,
            root_task_id,
            normalized,
            counts,
            blocked_ids,
            live_task_ids,
            exhausted,
        )

    # Per-card lost-liveness posture (owner simplification, 2026-09-17).
    # Evaluated AFTER external/triage/exhaustion postures and free runnable
    # work so a current gate, owner-action row, or genuinely runnable member
    # keeps precedence (gate wins; triage wins; live work is not alarm noise).
    # The rule itself is not maskable: any member that hit it coalesces into
    # one family authority listing the member ids.
    lost_authority = _heartbeat_lost_authority(normalized, now)
    if lost_authority is not None:
        return _snapshot(
            STATE_HEARTBEAT_LOST,
            root_task_id,
            normalized,
            counts,
            blocked_ids,
            live_task_ids,
            lost_authority,
        )

    if runnable_rows:
        # runnable members remain (behind failed postures or elsewhere) and no
        # verified posture: fail closed.
        return _snapshot(STATE_ACTIVE, root_task_id, normalized, counts, blocked_ids, live_task_ids, None)

    return _snapshot(STATE_ACTIVE, root_task_id, normalized, counts, blocked_ids, live_task_ids, None)


def verify_current_semantic_gate(
    tasks: Iterable[Mapping[str, Any]],
    *,
    root_task_id: str,
    outcome: str,
    fingerprint: str,
    links: Iterable[tuple[str, str]] | None,
    now: int | None = None,
) -> tuple[bool, FamilySnapshot | None]:
    """Shared fail-closed current-posture verifier.

    Re-reads and re-classifies the COMPLETE exact root family right now and
    requires that the current semantic state is still the SAME eligible
    outcome AND that the fresh classification reproduces the exact persisted
    fingerprint (which binds root/family identity + gate authority). Any
    drift -- gate appears/disappears/changes kind, authority event changes,
    membership changes, root identity drifts -- fails closed with False.

    Returns (True, current_snapshot) only when the current gate exactly
    matches the persisted trigger's semantic posture; (False, snapshot|None)
    otherwise. Callers must treat False as zero-action refusal.
    """
    if outcome not in ELIGIBLE_STATES:
        return False, None
    current = classify_family(tasks, root_task_id=root_task_id, links=links, now=now)
    if current.state != outcome:
        return False, current
    if current.fingerprint != fingerprint:
        return False, current
    return True, current


def _snapshot(
    state: str,
    root_task_id: str | None,
    tasks: tuple[dict[str, Any], ...],
    counts: dict[str, int],
    blocked_ids: tuple[str, ...],
    live_task_ids: tuple[str, ...],
    authority: GateAuthority | None,
) -> FamilySnapshot:
    """Bind the fingerprint to root/family identity plus the exact gate
    authority only: no heartbeat timestamps, run ids, live-task gaps,
    incidental counts, or task ordering."""
    payload = json.dumps(
        {
            "root": root_task_id or "",
            "state": state,
            "members": [row["id"] for row in tasks],
            "authority": None
            if authority is None
            else {
                "kind": authority.kind,
                "task_id": authority.task_id,
                "event_id": authority.event_id,
                "event_run_id": authority.event_run_id,
                "event_kind": authority.event_kind,
                "payload": authority.payload,
            },
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return FamilySnapshot(
        state=state,
        fingerprint=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        counts=counts,
        blocked_ids=blocked_ids,
        live_task_ids=live_task_ids,
        tasks=tasks,
        gate_authority=authority,
    )


# Preserve the former public entry point while changing its monitoring unit.
classify_board = classify_family


def build_root_families(
    tasks: Iterable[Mapping[str, Any]],
    links: Iterable[tuple[str, str]],
) -> tuple[RootFamily, ...]:
    # Duplicate-row guard BEFORE any lossy normalization: a raw
    # input sequence that carries one task id more than once is member/graph
    # drift. materializing per-id dicts/sets would erase that ambiguity, so
    # the ambiguity is rejected here, at the raw-row layer, fail closed.
    seen_ids: set[str] = set()
    materialized = list(tasks)
    for task in materialized:
        task_id = _task_material(task)["id"]
        if not task_id:
            raise ValueError("task id must be non-empty")
        if task_id in seen_ids:
            raise ValueError(f"duplicate task id in raw input rows: {task_id}")
        seen_ids.add(task_id)
    normalized = {_task_material(task)["id"]: _task_material(task) for task in materialized}
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
        family_links = tuple(
            sorted(
                (str(parent), str(child))
                for parent in closure
                for child in children[parent]
                if child in closure
            )
        )
        families.append(
            RootFamily(
                root_id=root_id,
                root_title=root["title"],
                session_id=root["session_id"],
                task_ids=task_ids,
                tasks=tuple(normalized[tid] for tid in task_ids),
                links=family_links,
            )
        )
    return tuple(families)


class SemanticEvidence:
    """Bounded ring buffer of structured episode evidence (c12).

    Every entry is a closed vocabulary label from ``EVIDENCE_LABELS`` plus
    identifier/counter fields only. Entries never carry block reasons, task
    bodies, summaries, or any other private content, and the buffer is capped
    so per-poll logging can never flood: one row per decision, 512 rows max.
    """

    MAX_ENTRIES = 512

    def __init__(self) -> None:
        self.entries: deque[dict[str, Any]] = deque(maxlen=self.MAX_ENTRIES)

    def record(self, label: str, **fields: Any) -> dict[str, Any]:
        if label not in EVIDENCE_LABELS:
            raise ValueError(f"invalid evidence label: {label}")
        entry: dict[str, Any] = {"label": label, "ts": int(time.time())}
        for key, value in sorted(fields.items()):
            if value is None:
                continue
            if isinstance(value, bool):
                entry[key] = value
            elif isinstance(value, int):
                entry[key] = value
            elif isinstance(value, str):
                entry[key] = value[:64]
        self.entries.append(entry)
        return entry

    def to_list(self) -> list[dict[str, Any]]:
        return [dict(entry) for entry in self.entries]


class StateStore:
    """Exactly-once-ish debounce state scoped to each board/root family.

    c10: arming is NEVER derived from heartbeat transitions. A pending trigger
    is created only after the same eligible semantic state plus stable gate
    fingerprint is observed twice; nongate observations clear an uncommitted
    candidate and create no receipt.
    """

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
        # Attempt ledger migration: adds attempted_at and stamps
        # pre-ledger rows fail-closed (see _backfill_attempted_at). Ledger-era
        # NULL stamps on unfinished rows survive reopen untouched.
        self._backfill_attempted_at()
        columns = {row[1] for row in self.conn.execute("PRAGMA table_info(family_state)")}
        if "last_noneligible_seen" not in columns:
            # c12 semantic re-entry: persisted 0/1 flag remembering whether the
            # family's most recent non-eligible observation closed the prior
            # eligible episode. Backfilled to 0 so existing rows keep their
            # current episode until the next real transition.
            self.conn.execute(
                "ALTER TABLE family_state ADD COLUMN last_noneligible_seen INTEGER NOT NULL DEFAULT 0"
            )
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS family_evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                label TEXT NOT NULL,
                board TEXT NOT NULL DEFAULT '',
                root_task_id TEXT NOT NULL DEFAULT '',
                review_key TEXT NOT NULL DEFAULT '',
                episode INTEGER,
                fingerprint TEXT NOT NULL DEFAULT '',
                outcome TEXT NOT NULL DEFAULT '',
                dedup_count INTEGER NOT NULL DEFAULT 0,
                hold_count INTEGER NOT NULL DEFAULT 0,
                count INTEGER,
                unfinished INTEGER,
                ts INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_family_evidence_scope
                ON family_evidence(board, root_task_id, id);
            """
        )
        self._evidence_prune()
        self.evidence = SemanticEvidence()

    def _backfill_attempted_at(self) -> None:
        """Attempt-ledger state migration for pre-ledger receipts.

        A PRE-LEDGER row (the attempted_at column itself absent before this
        migration added it) has no attempt record, so whether its physical
        send began is UNKNOWABLE from the store alone. Fail closed: stamp it
        attempted_at=updated_at so recovery treats it as already-attempted
        (escalation-only, never re-sent), regardless of status. Rows created
        BY this ledger version are untouched: observe() inserts with
        attempted_at=NULL and set_status transitions preserve NULL, so a NULL
        stamp on a ledger-era row is positive evidence of an unstarted send
        and must survive reopen untouched.
        """
        columns = {row[1] for row in self.conn.execute("PRAGMA table_info(family_receipts)")}
        if "attempted_at" not in columns:
            # This store predates the ledger entirely: every row is legacy.
            self.conn.execute(
                "ALTER TABLE family_receipts ADD COLUMN attempted_at INTEGER"
            )
            self.conn.execute(
                """UPDATE family_receipts SET attempted_at=updated_at
                   WHERE attempted_at IS NULL"""
            )

    # ---- attempt ledger -------------------------------------------------------

    def receipt_attempted(self, review_key: str) -> bool:
        """Whether the durable post-fence attempt BEGAN for this receipt.

        False only when the row exists, is still in an unfinished status, and
        carries attempted_at IS NULL — i.e. the fence committed but the
        injector call provably never started. Everything else (attempted, or
        unknown/legacy) reads as attempted so recovery fails closed.
        """
        row = self.conn.execute(
            """SELECT status, attempted_at FROM family_receipts WHERE review_key=?""",
            (review_key,),
        ).fetchone()
        if row is None:
            return True
        if str(row["status"]) not in UNFINISHED_RECEIPT_STATUSES:
            return True
        return row["attempted_at"] is not None

    def mark_attempted(self, review_key: str) -> None:
        """Durably record that the post-fence injector attempt BEGAN.

        Called immediately before the injector await in every dispatch path.
        One-way: the stamp is never cleared or reset while the receipt stays
        unfinished, so a crash or transport loss after this point leaves a
        receipt that recovery may confirm or escalate but never re-send.
        """
        self.conn.execute(
            "UPDATE family_receipts SET attempted_at=?,updated_at=? WHERE review_key=?",
            (int(time.time()), int(time.time()), review_key),
        )

    def unresolved_escalations(self) -> tuple[dict[str, Any], ...]:
        """Bounded operator-visible report of receipts never confirmed.

        Returns the receipt rows (compact identifiers only) for every receipt
        in a non-pending unfinished status, regardless of attempt state, plus
        the count of unfinished pending rows. The tick report embeds this so
        an eligible wake that was attempted but never accepted stays visible
        every poll instead of surfacing once and disappearing.
        """
        rows = self.conn.execute(
            """SELECT review_key,board,root_task_id,status,attempt,episode,attempted_at,updated_at
               FROM family_receipts
               ORDER BY CASE WHEN status IN ('scheduled','awaiting_confirmation','uncertain')
                             THEN 0 ELSE 1 END, updated_at, review_key"""
        ).fetchall()
        escalations: list[dict[str, Any]] = []
        pending_unfinished = 0
        for row in rows:
            entry = {
                "review_key": str(row["review_key"]),
                "board": str(row["board"]),
                "root_task_id": str(row["root_task_id"]),
                "status": str(row["status"]),
                "attempt": int(row["attempt"]),
                "episode": int(row["episode"]),
                "attempted": row["attempted_at"] is not None,
                "age_seconds": max(0, int(time.time()) - int(row["updated_at"])),
            }
            if row["status"] in UNRESOLVED_ESCALATION_STATUSES:
                escalations.append(entry)
            else:
                pending_unfinished += 1
        if pending_unfinished:
            escalations.append(
                {
                    "review_key": "",
                    "board": "",
                    "root_task_id": "",
                    "status": "pending_unfinished",
                    "attempt": 0,
                    "episode": 0,
                    "attempted": False,
                    "age_seconds": 0,
                    "count": pending_unfinished,
                }
            )
        return tuple(escalations)

    # ---- bounded persisted evidence -------------------------------------------

    EVIDENCE_MAX_ROWS = 512
    _EVIDENCE_TEXT_KEYS = frozenset(
        {"board", "root_task_id", "review_key", "fingerprint", "outcome"}
    )

    def _evidence_prune(self) -> None:
        """Keep at most EVIDENCE_MAX_ROWS rows (oldest evicted first)."""
        self.conn.execute(
            """DELETE FROM family_evidence WHERE id <= (
                   SELECT MAX(id) FROM family_evidence
               ) - ?""",
            (int(self.EVIDENCE_MAX_ROWS),),
        )

    def record_evidence(self, label: str, **fields: Any) -> dict[str, Any]:
        """Record one closed-vocabulary evidence row, dedup-aware.

        ``dedup_same_episode`` and ``target_hold`` are steady-posture labels:
        a repeat for the same (label, board, root, review_key) updates the
        existing row's counter in place instead of appending a new row, so
        steady polls never flood the table. All other labels append one row
        per decision. Values carry identifiers/counts only; free-form strings
        are rejected and identifiers are clipped.
        """
        if label not in EVIDENCE_LABELS:
            raise ValueError(f"invalid evidence label: {label}")
        now = int(time.time())
        row: dict[str, Any] = {
            "label": label,
            "board": "",
            "root_task_id": "",
            "review_key": "",
            "episode": None,
            "fingerprint": "",
            "outcome": "",
            "dedup_count": 0,
            "hold_count": 0,
            "count": None,
            "unfinished": None,
            "ts": now,
        }
        for key, value in sorted(fields.items()):
            if key not in row or value is None or key in ("label", "ts"):
                continue
            if key in self._EVIDENCE_TEXT_KEYS:
                if not isinstance(value, str):
                    raise ValueError(f"evidence field {key} must be str")
                row[key] = value[:72]
            elif key in ("episode", "count", "unfinished"):
                if isinstance(value, bool) or not isinstance(value, int):
                    raise ValueError(f"evidence field {key} must be int")
                row[key] = value
            else:
                raise ValueError(f"unknown evidence field: {key}")
        steady = label in (EVIDENCE_DEDUP_SAME_EPISODE, EVIDENCE_TARGET_HOLD)
        # When called inside a caller's transaction (observe(), set_status())
        # participate in it; otherwise open our own atomic unit.
        owns_tx = not self.conn.in_transaction
        if owns_tx:
            self.conn.execute("BEGIN IMMEDIATE")
        try:
            if steady:
                existing = self.conn.execute(
                    """SELECT id, dedup_count, hold_count FROM family_evidence
                       WHERE label=? AND board=? AND root_task_id=? AND review_key=?
                       ORDER BY id DESC LIMIT 1""",
                    (label, row["board"], row["root_task_id"], row["review_key"]),
                ).fetchone()
                if existing is not None:
                    column = "dedup_count" if label == EVIDENCE_DEDUP_SAME_EPISODE else "hold_count"
                    self.conn.execute(
                        f"""UPDATE family_evidence SET {column}={column}+1, episode=?, ts=?
                            WHERE id=?""",
                        (row["episode"], now, existing["id"]),
                    )
                    if owns_tx:
                        self.conn.commit()
                    row["id"] = int(existing["id"])
                    row[column] = int(existing[column]) + 1
                    return row
            self.conn.execute(
                """INSERT INTO family_evidence
                   (label,board,root_task_id,review_key,episode,fingerprint,outcome,
                    dedup_count,hold_count,count,unfinished,ts)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row["label"], row["board"], row["root_task_id"], row["review_key"],
                    row["episode"], row["fingerprint"], row["outcome"],
                    row["dedup_count"], row["hold_count"], row["count"],
                    row["unfinished"], row["ts"],
                ),
            )
            self._evidence_prune()
            if owns_tx:
                self.conn.commit()
        except Exception:
            if owns_tx:
                self.conn.rollback()
            raise
        return row

    def evidence_rows(
        self,
        *,
        board: str | None = None,
        root_task_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read the persisted evidence ring (oldest first) for audit."""
        query = (
            "SELECT id,label,board,root_task_id,review_key,episode,fingerprint,outcome,"
            "dedup_count,hold_count,count,unfinished,ts FROM family_evidence"
        )
        params: list[Any] = []
        clauses: list[str] = []
        if board is not None:
            clauses.append("board=?")
            params.append(board)
        if root_task_id is not None:
            clauses.append("root_task_id=?")
            params.append(root_task_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY id"
        rows = self.conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]

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
                first_count = 1 if snapshot.state in ELIGIBLE_STATES else 0
                self.conn.execute(
                    """INSERT INTO family_state
                       (board,root_task_id,root_title,target_key,last_state,last_fingerprint,armed,
                        candidate_state,candidate_fingerprint,candidate_count,episode,updated_at,
                        last_noneligible_seen)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        board,
                        root_task_id,
                        root_title,
                        target.key,
                        snapshot.state,
                        snapshot.fingerprint,
                        0,
                        snapshot.state if first_count else None,
                        snapshot.fingerprint if first_count else None,
                        first_count,
                        0,
                        now,
                        0 if snapshot.state in ELIGIBLE_STATES else 1,
                    ),
                )
                if first_count:
                    self.record_evidence(
                        EVIDENCE_DEBOUNCE, board=board, root_task_id=root_task_id, count=1
                    )
                self.conn.commit()
                return None

            episode = int(row["episode"])
            last_noneligible_seen = bool(row["last_noneligible_seen"])
            if snapshot.state not in ELIGIBLE_STATES:
                # Non-posture observation: clear any uncommitted pending; never
                # arm. The persisted flag (NOT an in-memory counter) records
                # that the prior eligible episode is closed, so the NEXT
                # eligible observation re-arms as a NEW semantic episode. This
                # survives restarts and does not depend on heartbeat/run/count
                # churn (c12 re-entry contract).
                self.conn.execute(
                    """UPDATE family_state SET root_title=?,target_key=?,last_state=?,last_fingerprint=?,
                       armed=0,candidate_state=NULL,candidate_fingerprint=NULL,candidate_count=0,
                       episode=?,last_noneligible_seen=1,updated_at=? WHERE board=? AND root_task_id=?""",
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

            same_candidate = (
                row["candidate_state"] == snapshot.state
                and row["candidate_fingerprint"] == snapshot.fingerprint
            )
            candidate_count = int(row["candidate_count"]) + 1 if same_candidate else 1
            if candidate_count < 2:
                self.conn.execute(
                    """UPDATE family_state SET root_title=?,target_key=?,last_state=?,last_fingerprint=?,
                       candidate_state=?,candidate_fingerprint=?,candidate_count=?,last_noneligible_seen=?,
                       updated_at=?
                       WHERE board=? AND root_task_id=?""",
                    (
                        root_title,
                        target.key,
                        snapshot.state,
                        snapshot.fingerprint,
                        snapshot.state,
                        snapshot.fingerprint,
                        candidate_count,
                        1 if last_noneligible_seen else 0,
                        now,
                        board,
                        root_task_id,
                    ),
                )
                self.record_evidence(
                    EVIDENCE_DEBOUNCE, board=board, root_task_id=root_task_id, count=candidate_count
                )
                self.conn.commit()
                return None

            # Two stable observations of this exact eligible posture: derive
            # the episode key, then decide dedup vs new episode (c12).
            def _key_for(ep: int) -> str:
                material = "|".join(
                    [board, root_task_id, target.session_id, str(ep), snapshot.state, snapshot.fingerprint]
                )
                return "korf-" + hashlib.sha256(material.encode("utf-8")).hexdigest()

            episode = int(row["episode"])
            review_key = _key_for(episode)
            existing = self.conn.execute(
                "SELECT status FROM family_receipts WHERE review_key=?", (review_key,)
            ).fetchone()
            new_episode = False
            if existing is None:
                if last_noneligible_seen:
                    # c12 NEW semantic episode: a non-eligible observation
                    # closed the prior eligible episode and this posture has
                    # no receipt at the current episode, so rotate BEFORE
                    # deriving the dispatch key. Rotation derives only from
                    # semantic posture transitions; historical receipts are
                    # never replayed and heartbeat/run/count churn is
                    # invisible here.
                    episode += 1
                    review_key = _key_for(episode)
                    new_episode = True
            elif str(existing["status"]) == "confirmed" and last_noneligible_seen:
                # The defect fix: the prior episode's receipt was confirmed,
                # and a non-eligible observation has since closed that
                # episode. Re-entry to the same eligible state must bind to a
                # strictly NEW review key instead of being suppressed
                # forever. family_state.episode is always >= every inserted
                # receipt episode for this row, so the rotated key is fresh.
                episode += 1
                review_key = _key_for(episode)
                new_episode = True
                existing = self.conn.execute(
                    "SELECT status FROM family_receipts WHERE review_key=?", (review_key,)
                ).fetchone()
            if existing is not None:
                # Same-episode dedup/hold: this exact posture already has a
                # receipt. An unfinished receipt is owned by recovery (never
                # a second trigger); a confirmed one means the open episode
                # already notified (continuous unchanged eligible posture,
                # steady poll). Suppress; the episode does not advance.
                self.conn.execute(
                    """UPDATE family_state SET armed=0,last_noneligible_seen=0,updated_at=?
                       WHERE board=? AND root_task_id=?""",
                    (now, board, root_task_id),
                )
                self.record_evidence(
                    EVIDENCE_DEDUP_SAME_EPISODE,
                    board=board,
                    root_task_id=root_task_id,
                    review_key=review_key,
                    episode=episode,
                    fingerprint=snapshot.fingerprint,
                    unfinished=1 if str(existing["status"]) in UNFINISHED_RECEIPT_STATUSES else 0,
                )
                self.conn.commit()
                return None
            if new_episode:
                self.record_evidence(
                    EVIDENCE_NEW_EPISODE,
                    board=board,
                    root_task_id=root_task_id,
                    review_key=review_key,
                    episode=episode,
                    fingerprint=snapshot.fingerprint,
                )
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
                   (review_key,board,root_task_id,target_key,outcome,fingerprint,episode,attempt,status,error,payload_json,created_at,updated_at,attempted_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)""",
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
            )  # attempted_at=NULL: the physical send has not begun; recovery may complete it exactly once
            self.conn.execute(
                """UPDATE family_state SET root_title=?,target_key=?,last_state=?,last_fingerprint=?,
                   armed=0,candidate_state=NULL,candidate_fingerprint=NULL,candidate_count=0,
                   episode=?,last_noneligible_seen=0,updated_at=?
                   WHERE board=? AND root_task_id=?""",
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
            authority_raw = snap_raw.get("gate_authority")
            snapshot = FamilySnapshot(
                state=snap_raw["state"],
                fingerprint=snap_raw["fingerprint"],
                counts=dict(snap_raw["counts"]),
                blocked_ids=tuple(snap_raw["blocked_ids"]),
                live_task_ids=tuple(snap_raw["live_task_ids"]),
                tasks=tuple(dict(task) for task in snap_raw["tasks"]),
                gate_authority=GateAuthority(**authority_raw) if authority_raw else None,
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
                # Rotate the episode so the next arm of the same posture
                # hashes a NEW review_key: the failed receipt can never
                # shadow the re-trigger (dedup/episode-rotation contract:
                # "episode rotates ONLY on retryable failure"). armed=1
                # marks the rotation for observers. last_noneligible_seen is
                # cleared so the rotation itself is not mistaken for a
                # semantic re-entry boundary.
                self.conn.execute(
                    """UPDATE family_state SET armed=1,episode=episode+1,candidate_state=NULL,candidate_fingerprint=NULL,
                       candidate_count=0,last_noneligible_seen=0,updated_at=? WHERE board=? AND root_task_id=?""",
                    (now, trigger.board, trigger.root_task_id),
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def finish(self, trigger: Trigger, *, success: bool, error: str | None = None) -> None:
        self.set_status(trigger, "confirmed" if success else "retryable_failed", error=error)

    def decide_trigger_action(
        self,
        board: str,
        root_task_id: str,
        candidate_state: str,
        candidate_fingerprint: str,
    ) -> str:
        """Dedup boundary for the CURRENT observation.

        ``review_key`` binds (board, root, session, episode, state,
        fingerprint); ``episode`` rotates on retryable failure and on a
        semantic eligible <- non-eligible re-entry (c12). The consequence for
        a needs_input family: once a receipt with the same (state,
        fingerprint) is confirmed, later observations of the UNCHANGED open
        episode hash to the SAME review_key and are swallowed (the human
        already answered this exact gate); after the family has passed
        through a non-eligible posture (worked, re-entered triage), the next
        arm is a NEW semantic episode and wakes again.

        Contract:
          pending/scheduled/awaiting_confirmation/uncertain -> "hold"
              (an unfinished receipt for this exact posture exists; recovery
              owns it, never a second trigger. Recovery itself may
              still complete the single owed send of an unattempted fence
              (attempted_at IS NULL); an attempted receipt is confirmation-
              only. This dedup boundary itself never dispatches.)
          confirmed with the SAME (state, fingerprint) -> "skip"
              (dedup boundary; the human already answered this exact gate)
          confirmed with a DIFFERENT (state, fingerprint) -> "trigger"
              (the posture MOVED -- new gate kind, authority event, or
              membership; a new wake is legitimate)
          no receipt at all -> "trigger"
        """
        row = self.conn.execute(
            "SELECT last_state,last_fingerprint,episode FROM family_state WHERE board=? AND root_task_id=?",
            (board, root_task_id),
        ).fetchone()
        if row is None:
            return "trigger"
        episode = int(row["episode"])
        material = "|".join(
            [board, root_task_id, "", str(episode), candidate_state, candidate_fingerprint]
        )
        review_key = "korf-" + hashlib.sha256(material.encode("utf-8")).hexdigest()
        row = self.conn.execute(
            "SELECT status FROM family_receipts WHERE review_key=?",
            (review_key,),
        ).fetchone()
        if row is None:
            return "trigger"
        status = str(row["status"])
        if status in UNFINISHED_RECEIPT_STATUSES:
            return "hold"
        if status == "confirmed":
            # Same posture already answered -> skip; moved posture -> retrigger.
            if (
                row_last := self.conn.execute(
                    "SELECT last_state,last_fingerprint FROM family_state WHERE board=? AND root_task_id=?",
                    (board, root_task_id),
                ).fetchone()
            ) is not None and (
                str(row_last["last_state"]) == candidate_state
                and str(row_last["last_fingerprint"]) == candidate_fingerprint
            ):
                return "skip"
            return "trigger"
        # retryable_failed/rejected: the episode already rotated; observe()
        # owns the re-trigger path.
        return "trigger"
