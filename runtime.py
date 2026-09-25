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
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping
from urllib.parse import quote

from .core import (
    EVIDENCE_CONFIRMATION,
    EVIDENCE_DISPATCH,
    EVIDENCE_RECOVERY_HOLD,
    EVIDENCE_TARGET_HOLD,
    NOISY_EVENT_KINDS,
    STATUS_AFFECTING_EVENT_KINDS,
    StateStore,
    Trigger,
    WakeTarget,
    build_root_families,
    classify_family,
    verify_current_semantic_gate,
)

# ---------------------------------------------------------------------------
# The root provenance envelope supplies targeting authority.
# ---------------------------------------------------------------------------

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


@dataclass(frozen=True)
class _SourceAdapterBinding:
    """Exact source key, expected platform value, and runner adapter resolver."""

    expected_platform: str
    resolver: Callable[[Any, Any, str], Any]


@dataclass(frozen=True)
class _AdapterResolution:
    status: str  # READY | HOLD | SKIP
    reason_code: str | None = None
    binding: _SourceAdapterBinding | None = None
    platform: Any | None = None
    adapter: Any | None = None
    delivery: str = "adapter"  # adapter | host


# Sentinel standing in for the retained transport identity of a host-delivered
# target. The adapter path pinned the exact live adapter object across recovery; a host
# delivery has no transport object, so the retained channel is proved by this
# sentinel plus the full current re-proof (same call graph, same fences).
HOST_DELIVERY_SENTINEL = object()

# platforms eligible for canonical host-level delivery when no connected
# transport adapter exists for them. Membership mirrors the installed
# gateway Platform enum; internal events reach the host without a transport adapter
# (run_inbound admission returns before adapter consultation), so the host
# path is the canonical delivery channel for these origins.
# the canonical discord platform joins the host-eligible set. A stored
# discord-origin session whose transport adapter object is absent (unprovisioned
# channel) delivers through the same host-level injection entrypoint; a present
# connected discord adapter keeps the exact live adapter path, and a present but
# unhealthy one still fails closed with the typed HOLD.
_HOST_DELIVERY_PLATFORMS = frozenset({"local", "api_server", "relay", "discord"})


def _platform_is_installable(platform: str) -> bool:
    """True iff the stored platform value is a member of the installed enum.

    Origin-agnostic session identity fix. The stored platform string
    must be exactly a registered gateway Platform value (no case fold, no
    trim on the value that travels — normalization applies to a copy used
    only for the membership test). Any unregistered value fails closed.
    """
    if not isinstance(platform, str) or not platform:
        return False
    try:
        from gateway.config import Platform

        platform_values = {member.value for member in Platform}
    except Exception:
        return False
    return platform in platform_values


def _runner_authorization_adapter(runner: Any, platform: Any, profile: str) -> Any:
    resolver = getattr(runner, "_authorization_adapter", None)
    if not callable(resolver):
        return None
    return resolver(platform, profile)


# Source lookup is intentionally exact.  Adding a source requires an explicit
# registry entry with its platform binding and resolver; unknown keys never
# inherit a default adapter or an ambient session.
SOURCE_ADAPTER_REGISTRY: Mapping[str, _SourceAdapterBinding] = {
    "discord": _SourceAdapterBinding(
        expected_platform="discord",
        resolver=_runner_authorization_adapter,
    ),
    # api_server: the Recorder/API transport. READY requires a connected
    # adapter object because delivery goes through the adapter's own
    # session-addressed wake loopback (host/port/key live on the adapter);
    # there is deliberately NO adapterless host fallback for declared-key
    # sessions. Session identity itself is proven by _resolve_api_server_target
    # through the SessionDB, not by this registry entry.
    "api_server": _SourceAdapterBinding(
        expected_platform="api_server",
        resolver=_runner_authorization_adapter,
    ),
}


@dataclass(frozen=True)
class CreatorClaim:
    """One strictly parsed creator provenance claim (D3)."""

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
    """Typed origin resolution; READY retains the selected adapter identity.

    READY decisions produced by ``_current_root_decision`` additionally bind
    the exact current family graph rows/links (``family_tasks``/
    ``family_links``) so the caller can re-prove the semantic gate on the
    same read; default None elsewhere (never a delivery authority).
    """

    status: str  # READY | HOLD | SKIP
    reason_code: str | None = None
    target: WakeTarget | None = None
    adapter: Any | None = None
    family_tasks: tuple[dict, ...] | None = None
    family_links: tuple[tuple[str, str], ...] | None = None
    # transport channel for a READY decision. "adapter" = deliver
    # through the resolved live adapter; "host" = deliver through
    # the gateway's host-level internal-event entrypoint with no transport;
    # "api" = deliver through the adapter's session-addressed wake loopback
    # (api_server only; requires the exact SessionDB session id on target).
    delivery: str = "adapter"

    @property
    def ready(self) -> bool:
        return self.status == "READY"

    def current_gate_holds(self, trigger: Trigger) -> bool:
        """Shared fail-closed current-posture verification.

        Proves, on THIS decision's own bound graph, that the family's current
        semantic state is still the persisted trigger's eligible outcome AND
        that the fresh classification reproduces the persisted fingerprint
        (root/family + gate authority). Unbound decisions (no graph) and any
        drift fail closed.
        """
        if self.family_tasks is None or self.family_links is None:
            return False
        holds, _snapshot = verify_current_semantic_gate(
            self.family_tasks,
            root_task_id=trigger.root_task_id,
            outcome=trigger.outcome,
            fingerprint=trigger.fingerprint,
            links=self.family_links,
        )
        return holds


class DeliveryRefusedError(RuntimeError):
    """Typed delivery refusal at the inject_trigger delivery boundary.

    Raised when the pre-delivery binding re-proof fails (ambiguous key,
    stale/closed/rotated target, unusable destination): zero delivery, the
    exact typed reason rides ``reason`` for durable evidence, and the
    generic RuntimeError superclass keeps existing ``except`` handling
    (retry/uncertain classification) intact.
    """

    def __init__(self, message: str, *, reason: str = "HOLD_DESTINATION_UNUSABLE"):
        super().__init__(message)
        self.reason = reason


def _canonical_claim_bytes(claim: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(claim),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def parse_creator_provenance(body: str | None) -> CreatorClaim | OriginDecision:
    """Strict full-envelope parser over the exact root body (D3).

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
    # Validate authority fields as exact literals before matching.
    # rejects case or whitespace variants without normalization.
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


# ---------------------------------------------------------------------------
# api_server exact-session support (Recorder/API provenance contract).
#
# The canonical Recorder session identity is the CLIENT-DECLARED
# ``X-Hermes-Session-Key`` byte-echo stored on the SessionDB row; it is NOT a
# derivable canonical key (``build_session_key`` would mint
# ``agent:main:api_server:dm``), so every canonical-key re-derivation proof is
# unreachable for these claims. Identity is proven exclusively through the
# supported SessionDB APIs (peer finder + resumable-tip resolution) and
# delivered through the adapter's official session-addressed wake channel
# (``gateway.wake.deliver_wake`` → loopback self-post with the exact session
# id). Nothing here consults recent/default/home/board/title/process context.
# ---------------------------------------------------------------------------

_API_SERVER_SOURCE = "api_server"

# New typed refusal: supplied selector evidence contradicts the current
# SessionDB resolution (stale raw id, cross-session id, ancestor-after-rotation).
HOLD_STALE_TARGET = "HOLD_STALE_TARGET"

# Upper bound for one exact-key enumeration page through the supported
# public list API. An exact (source, session_key) equality filter with a
# healthy store has a handful of rows; a result at or above the limit means
# the candidate universe cannot be bounded, so uniqueness is unprovable and
# every caller fails closed (never paged through). The bound is ENFORCED in
# ``_api_server_resumable_row_ids``: a saturating page returns
# None there, so no caller can mistake an emptied window for an empty key.
_API_SERVER_ENUM_LIMIT = 32


def _api_server_session_db(runner: Any):
    """Read-only SessionDB handle for exact-key resolution.

    Uses a runner-provided seam when present (``_api_session_db`` attribute or
    callable, ``_session_db`` handle) so isolated rehearsals can inject a
    temporary database; otherwise opens the runtime home read-only. Never
    writes; any failure returns None (caller fails closed).
    """
    seam = getattr(runner, "_api_session_db", None)
    if callable(seam):
        try:
            return seam()
        except Exception:
            return None
    if seam is not None:
        return seam
    handle = getattr(runner, "_session_db", None)
    if handle is not None:
        return handle
    try:
        from hermes_constants import get_hermes_home
        from hermes_state import SessionDB

        return SessionDB(db_path=Path(get_hermes_home()) / "state.db", read_only=True)
    except Exception:
        return None


def _api_server_claim_selectors(claim: CreatorClaim) -> tuple[str, str] | None:
    """Strict api_server selector extraction -> (session_key, session_id).

    ``session_key`` (the byte-exact ``X-Hermes-Session-Key`` echo) is the
    durable authority and REQUIRED. ``session_id`` is optional and conjunctive.
    Any other selector (chat_id/thread_id/conversation_id) has no stored
    api_server identity and fails closed.
    """
    session_key = getattr(claim, "session_key", "") or ""
    session_key = session_key if isinstance(session_key, str) else ""
    if not session_key.strip() or session_key.strip() != session_key:
        return None
    session_id = getattr(claim, "session_id", "") or ""
    session_id = session_id if isinstance(session_id, str) else ""
    if session_id and (session_id.strip() != session_id or not session_id.strip()):
        return None
    for other in ("chat_id", "thread_id", "conversation_id"):
        value = getattr(claim, other, "") or ""
        if isinstance(value, str) and value.strip():
            return None
    return session_key, session_id


def _api_server_resumable_row_ids(db: Any, session_key: str) -> tuple[str, ...] | None:
    """Enumerate the resumable row ids sharing one exact (source, session_key).

    ``find_latest_gateway_session_for_peer`` ranks candidates and ends in
    ``LIMIT 1``: multiple live rows for one declared key are silently reduced
    instead of reported as ambiguous. The code establishes uniqueness through
    the supported public surface only:

    - ``db.list_sessions_rich(source=..., session_key=..., limit=N)`` is the
      installed public list API whose shared filter composer applies the exact
      SQL equality ``s.session_key = ? AND s.source IN (...)`` — no
      normalization, no prefixing, no recency reduction. Relevance filters
      (``hidden``, ``archived``, child exclusion) are re-checked below from
      the returned row dicts so the enumeration cannot silently differ from
      the finder's candidate set. The page is BOUNDED: an at-limit result
      means rows exist beyond the window and uniqueness is unprovable
      — the caller fails closed rather than reading the residue.
    - Public ``SessionDB.RECOVERABLE_END_REASONS`` supplies the installed
      recoverability taxonomy by name (the finder SQL admits rows whose
      ``ended_at IS NULL`` or whose ``end_reason`` is in that public tuple).
    - The reset-fence re-check uses the boundary taxonomy documented in the
      installed ``find_latest_gateway_session_for_peer`` contract
      (``session_reset``, ``session_switch``, ``idle``, ``daily``,
      ``suspended``, ``resume_pending_expired``); it is a re-application of
      the finder's documented fence, not new host surface.

    A candidate row is resumable when it is not hidden, not archived, not a
    hidden continuation child, its end state is open or in the public
    recoverable set, and no reset-boundary row for the same exact key ended
    after its last activity. Any API failure or unexpected row shape returns
    None (caller fails closed). No private member, no raw SQL against host
    tables.
    """
    lister = getattr(db, "list_sessions_rich", None)
    if not callable(lister):
        return None
    # Public installed taxonomy. Read by name so an installed-surface change
    # fails the getattr gate instead of silently drifting from host truth.
    recoverable = tuple(getattr(type(db), "RECOVERABLE_END_REASONS", ()) or ())
    if not recoverable:
        return None
    try:
        # The installed method is latency-bounded by design; one exact-key
        # page is the complete candidate universe for a unique-key proof.
        rows = lister(
            source=_API_SERVER_SOURCE,
            session_key=session_key,
            limit=_API_SERVER_ENUM_LIMIT,
            offset=0,
            order_by_last_active=False,
            project_compression_tips=False,
            include_archived=False,
            include_children=False,
            include_hidden=False,
        )
    except Exception:
        return None
    if not isinstance(rows, list):
        return None
    if len(rows) >= _API_SERVER_ENUM_LIMIT:
        # The page is a BOUNDED window, not a proof. A saturating
        # page means rows sharing this exact key exist beyond the window —
        # the enumeration cannot distinguish "no resumable row" from "the
        # resumable rows were crowded out by newer ended rows", so the
        # uniqueness proof is unprovable and both callers must fail closed
        # instead of trusting the (possibly emptied) residue. The documented
        # cost of latency-bounded enumeration is exactly this bound.
        return None
    ids: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            return None
        text = str(row.get("id") or "").strip()
        if not text:
            continue
        # Public-surface re-checks (row dicts from the supported API):
        # archived/hidden are re-verified defensively against the returned
        # row, and the end state must be open or in the public recoverable
        # taxonomy — mirroring the finder's candidate predicate exactly.
        if row.get("archived") or row.get("hidden"):
            continue
        ended_at = row.get("ended_at")
        end_reason = row.get("end_reason")
        open_row = ended_at is None
        recoverable_row = (not open_row) and end_reason in recoverable
        if not (open_row or recoverable_row):
            continue
        last_active = row.get("last_activity_at")
        if last_active is None:
            last_active = row.get("started_at")
        if not isinstance(last_active, (int, float)):
            # Cannot rank the fence against this row: uniqueness would be
            # assumed rather than proven. Fail closed.
            return None
        fence_pending = _api_server_reset_fence_pending(
            db, session_key, float(last_active))
        if fence_pending is None or fence_pending:
            # Unprovable or fenced: this key is not uniquely resumable
            # through the supported surface — fail closed.
            return None
        ids.append(text)
    return tuple(ids)


# Boundary end reasons that fence recovery in the installed
# ``find_latest_gateway_session_for_peer`` contract: an intentional boundary
# row for the exact key that ended AFTER a candidate's last activity rejects
# that candidate. Mirrors the documented host fence; the recoverable side is
# supplied live from public SessionDB.RECOVERABLE_END_REASONS.
_API_SERVER_RESET_FENCE_REASONS = (
    "session_reset",
    "session_switch",
    "idle",
    "daily",
    "suspended",
    "resume_pending_expired",
)


def _api_server_reset_fence_pending(
    db: Any, session_key: str, last_active: float
) -> bool | None:
    """True when a reset-boundary row fences this key; None when unprovable.

    Re-applies the installed finder's documented reset fence through the
    supported public list API: any same-source, same-key row that ended with
    a boundary reason after ``last_active`` fences the candidate. Read
    failure or an unreadable row returns None so callers fail closed.
    """
    lister = getattr(db, "list_sessions_rich", None)
    if not callable(lister):
        return None
    try:
        rows = lister(
            source=_API_SERVER_SOURCE,
            session_key=session_key,
            limit=_API_SERVER_ENUM_LIMIT,
            offset=0,
            order_by_last_active=False,
            project_compression_tips=False,
            include_archived=True,
            include_children=True,
            include_hidden=True,
        )
    except Exception:
        return None
    if not isinstance(rows, list):
        return None
    for row in rows:
        if not isinstance(row, dict):
            return None
        ended_at = row.get("ended_at")
        end_reason = row.get("end_reason")
        if ended_at is None or end_reason not in _API_SERVER_RESET_FENCE_REASONS:
            continue
        if not isinstance(ended_at, (int, float)):
            return None
        if float(ended_at) > last_active:
            return True
    return False


def _resolve_api_server_target(runner: Any, claim: CreatorClaim) -> OriginDecision:
    """Resolve an ``api_server`` claim to the unique current resumable tip.

    Selector gate → read-only SessionDB peer lookup (reset-fenced, recoverable
    rows only) → uniqueness proof over every resumable row sharing the exact
    key (ambiguous match fails closed, never reduced by recency/id/order) →
    conjunctive ``session_id`` agreement against the resolved tip → connected
    transport re-proof. Every mismatch, absence, fence, or unusable transport
    fails closed with a typed reason.
    """
    selectors = _api_server_claim_selectors(claim)
    if selectors is None:
        return OriginDecision("HOLD", "HOLD_SESSION_UNRESOLVABLE")
    session_key, session_id = selectors
    db = _api_server_session_db(runner)
    finder = getattr(db, "find_latest_gateway_session_for_peer", None)
    if db is None or not callable(finder):
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    try:
        row = finder(source=_API_SERVER_SOURCE, session_key=session_key)
    except Exception:
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    if not isinstance(row, dict) or not str(row.get("id") or "").strip():
        # Absent or reset-fenced: no recoverable session for this exact key.
        return OriginDecision("HOLD", "HOLD_SESSION_UNRESOLVABLE")
    row_ids = _api_server_resumable_row_ids(db, session_key)
    if row_ids is None:
        # Uniqueness is unprovable: fail closed rather than trust LIMIT 1.
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    if len(row_ids) > 1:
        # Two or more live/resumable rows share this exact profile,
        # source, and producer session_key. The claim is ambiguous — typed
        # HOLD with zero delivery; never reduce by recency, id, or ordering,
        # and never route to either row.
        return OriginDecision("HOLD", "HOLD_SESSION_AMBIGUOUS")
    row_id = str(row["id"])
    resolver = getattr(db, "resolve_resume_session_id", None)
    try:
        tip = str(resolver(row_id) or "") if callable(resolver) else ""
    except Exception:
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    tip = tip or row_id
    if session_id and session_id != tip:
        # Stale raw id (closed parent after rotation) or cross-session id:
        # conjunctive exactness is never rescued.
        return OriginDecision("HOLD", HOLD_STALE_TARGET)
    stored_profile = _canonical_stored_profile(row.get("profile_name"))
    if stored_profile != SUPPORTED_PROFILE:
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    chat_type = row.get("chat_type") if isinstance(row.get("chat_type"), str) else ""
    if chat_type not in {"dm", "group", "channel", "thread"}:
        return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
    target = WakeTarget(
        kind="creator_session",
        key=f"{_API_SERVER_SOURCE}:{stored_profile}:{session_key}",
        session_id=tip,
        session_key=session_key,
        platform=_API_SERVER_SOURCE,
        chat_id=str(row.get("chat_id") or ""),
        chat_name=row.get("chat_name") if isinstance(row.get("chat_name"), str) else None,
        chat_type=chat_type,
        thread_id=row.get("thread_id") if isinstance(row.get("thread_id"), str) else None,
        parent_chat_id=None,
        user_id=None,
        user_name=None,
        user_id_alt=None,
        chat_id_alt=None,
        chat_topic=None,
        scope_id=None,
        guild_id=None,
        profile=stored_profile,
    )
    # Transport re-proof through the exact registry. The api_server wake
    # channel is the adapter's own loopback session-addressed post (host,
    # port, and key live on the adapter object), so a missing or unhealthy
    # transport is an unusable destination — there is NO adapterless fallback
    # for declared-key sessions (the host internal-event path would drop the
    # event on the canonical-key re-derivation it runs first).
    resolution = _resolve_registered_adapter(
        runner, _API_SERVER_SOURCE, _API_SERVER_SOURCE, target.profile)
    if resolution.status == "READY" and resolution.adapter is not None:
        return OriginDecision("READY", None, target, resolution.adapter, delivery="api")
    if resolution.status == "HOLD":
        reason = resolution.reason_code or "HOLD_DESTINATION_UNUSABLE"
        if reason in {"HOLD_ADAPTER_OBJECT_ABSENT", "HOLD_DESTINATION_UNUSABLE"}:
            return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
        return OriginDecision("HOLD", reason)
    return OriginDecision(resolution.status, resolution.reason_code)


def _resolve_registered_adapter(
    runner: Any,
    source: Any,
    target_platform: Any,
    stored_profile: Any,
) -> _AdapterResolution:
    """Resolve one exact source key through the registered runner capability.

    Platform exactness seam: the stored platform representation must already be
    exactly the canonical registry binding value. No case folding, no whitespace
    stripping, no candidate-side normalization of any kind.
    """
    binding = (
        SOURCE_ADAPTER_REGISTRY.get(source)
        if isinstance(source, str)
        else None
    )
    if binding is None:
        return _AdapterResolution("SKIP", "SKIP_UNSUPPORTED_SOURCE")
    if target_platform != binding.expected_platform:  # exact value comparison
        return _AdapterResolution("HOLD", "HOLD_SOURCE_PLATFORM_MISMATCH", binding=binding)
    profile = _canonical_stored_profile(stored_profile)
    if profile != SUPPORTED_PROFILE:
        return _AdapterResolution("HOLD", "HOLD_DESTINATION_UNUSABLE", binding=binding)
    try:
        from gateway.config import Platform

        platform = Platform(binding.expected_platform)
        adapter = binding.resolver(runner, platform, profile)
    except Exception:
        return _AdapterResolution(
            "HOLD",
            "HOLD_DESTINATION_UNUSABLE",
            binding=binding,
        )
    if adapter is None or not callable(getattr(adapter, "handle_message", None)):
        # Transport separation: a transport with NO adapter object is an
        # unprovisioned channel, not a broken one. The canonical host-level
        # injection path can carry the internal event without any transport,
        # so resolve_creator_target may fall through to host delivery for
        # host-eligible stored platforms. A present-but-unusable adapter
        # stays the generic transport failure below.
        if adapter is None:
            return _AdapterResolution(
                "HOLD",
                "HOLD_ADAPTER_OBJECT_ABSENT",
                binding=binding,
                platform=platform,
            )
        return _AdapterResolution(
            "HOLD",
            "HOLD_DESTINATION_UNUSABLE",
            binding=binding,
            platform=platform,
        )
    if not _adapter_is_connected(adapter):
        return _AdapterResolution(
            "HOLD",
            "HOLD_DESTINATION_UNUSABLE",
            binding=binding,
            platform=platform,
        )
    return _AdapterResolution(
        "READY",
        binding=binding,
        platform=platform,
        adapter=adapter,
    )


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
DEFAULT_PARENT_CHANNEL_ID = "1517304790600912986"
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

        # Currentness is proved against the newest
        # status-affecting lifecycle transition per (task, kind), so the
        # reader must retain ``claimed``/``review_requested`` and every
        # other transition kind in core.STATUS_AFFECTING_EVENT_KINDS —
        # not only the terminal kinds. A recorded kind outside that
        # vocabulary is UNKNOWN currentness evidence: it is surfaced (with
        # payload=None) so _authority_is_current fails closed instead of
        # silently ignoring a brand-new transition kind. Pure audit noise
        # (comments/attachments/heartbeats/spawns/links) never contradicts a
        # terminal posture and stays out of latest_events.
        latest: dict[tuple[str, str], dict[str, Any]] = {}
        if "task_events" in tables:
            known_kinds = set(STATUS_AFFECTING_EVENT_KINDS)
            for event in conn.execute(
                "SELECT id,task_id,run_id,kind,payload FROM task_events "
                "WHERE kind IN ({}) OR kind NOT IN ({}) ORDER BY id".format(
                    ",".join("?" for _ in sorted(known_kinds)),
                    ",".join("?" for _ in NOISY_EVENT_KINDS),
                ),
                tuple(sorted(known_kinds)) + tuple(sorted(NOISY_EVENT_KINDS)),
            ):
                payload: dict[str, Any] | None
                if event["payload"]:
                    try:
                        parsed = json.loads(event["payload"])
                    except (TypeError, ValueError):
                        parsed = None
                    if isinstance(parsed, dict):
                        payload = parsed
                    else:
                        # Parse validity is preserved from
                        # the reader boundary. Non-object / truncated /
                        # malformed JSON must never normalize into an
                        # acceptable empty dict; payload stays explicitly
                        # None so well-formedness checks reject the event
                        # and terminal authority is deterministically
                        # refused.
                        payload = None
                else:
                    payload = None
                latest[(str(event["task_id"]), str(event["kind"]))] = {
                    "id": int(event["id"]),
                    "run_id": event["run_id"],
                    "kind": str(event["kind"]),
                    "payload": payload,
                }
        for row in rows:
            tid = str(row["id"])
            # Promote EVERY retained latest event of this row: the
            # fixed terminal tuple is not an allowlist. Unknown/new
            # transition kinds must reach latest_events so currentness
            # fails closed on them.
            events_for_task: dict[str, dict[str, Any]] = {
                kind: authority
                for (event_tid, kind), authority in latest.items()
                if event_tid == tid
            }
            blocked = events_for_task.get("blocked", {})
            completed = events_for_task.get("completed", {})
            row["latest_events"] = events_for_task
            row["block_reason"] = (
                (blocked.get("payload") or {}).get("reason")
                if row.get("status") == "blocked"
                else None
            )
            row["summary"] = (completed.get("payload") or {}).get("summary") or row.get("result")
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
    # Preserve the stored platform representation byte-for-byte: only the
    # enum wrapper is unwrapped. Case folding and whitespace trimming are
    # applied to a separate copy used solely by the membership check below;
    # the untouched representation travels on the target so the registry
    # can compare it exactly at its own boundary.
    platform = str(getattr(raw_platform, "value", raw_platform) or "")
    platform_canonical = platform.strip().lower()
    chat_id = getattr(source, "chat_id", None)
    chat_type = getattr(source, "chat_type", None)
    session_key = getattr(entry, "session_key", None)
    session_id = getattr(entry, "session_id", None)
    if not all(isinstance(value, str) and value for value in (chat_id, chat_type, session_key, session_id)):
        return None
    # origin-agnostic canonical-session identity. Session insert
    # eligibility MUST NOT depend on creator-session origin shape. The stored
    # platform must still name a registered gateway Platform value (any
    # unregistered/garbage value fails closed); membership is checked on the
    # normalized copy so recognizable variants ("Discord", " discord ") keep
    # traveling to the registry boundary, which alone types the exact-value
    # HOLD_SOURCE_PLATFORM_MISMATCH. Garbage never becomes a resolvable row.
    if not _platform_is_installable(platform_canonical) or chat_type not in {"dm", "group", "channel", "thread"}:
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
    # Preserve the stored profile representation for downstream delivery.
    # carry the exact stored profile representation (None stays None, "" stays
    # "", explicit names stay byte-exact) into WakeTarget and the emitted
    # MessageEvent.source.  Canonical default-namespace equality is proven
    # separately: _canonical_stored_profile during selection and the canonical
    # session_key check in resolve_creator_target.  The stored representation
    # itself is never rewritten.
    raw_profile = getattr(source, "profile", None)
    if raw_profile is not None and not isinstance(raw_profile, str):
        return None
    profile = raw_profile
    # Preserve the stored parent identifier exactly through delivery.
    # stored parent_chat_id is preserved exactly when None or str (including
    # empty and surrounding whitespace, byte-for-byte) through WakeTarget,
    # receipt round trip, and the emitted MessageEvent.source.  Non-string
    # values fail closed; a caller/default parent never fills absence.
    raw_parent_chat_id = getattr(source, "parent_chat_id", None)
    if raw_parent_chat_id is not None and not isinstance(raw_parent_chat_id, str):
        return None
    stored_parent_chat_id = raw_parent_chat_id
    return WakeTarget(
        kind="creator_session",
        key=f"{platform}:{profile or 'default'}:{session_key}",
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
    """Resolve the claim against the current SessionStore only (D3).

    The block is the sole origin authority: no birth/OriginBinding/board-route
    corroboration, no legacy fallback. Scope gate first, then conjunctive
    selector matching over the store snapshot, then destination validation
    including the installed boolean-property adapter readiness contract.
    """
    if claim.profile != SUPPORTED_PROFILE:
        return OriginDecision("SKIP", "SKIP_UNSUPPORTED_SOURCE")
    # api_server exact-session path: the Recorder's declared-key identity is
    # SessionDB-only (see _resolve_api_server_target). Registry typing for
    # unknown sources stays observable BEFORE any store consult: a claim from
    # an unregistered transport skips regardless of store presence.
    if claim.source == _API_SERVER_SOURCE:
        return _resolve_api_server_target(runner, claim)
    if claim.source not in SOURCE_ADAPTER_REGISTRY:
        return OriginDecision("SKIP", "SKIP_UNSUPPORTED_SOURCE")
    # the source-adapter registry is transport evidence, not a
    # delivery-eligibility filter. Resolution proceeds against the canonical
    # session store for every claim; the transport decision (connected
    # adapter vs host-level injection) happens after the session is proven.
    store = getattr(runner, "session_store", None)
    lock = getattr(store, "_lock", None)
    ensure_loaded = getattr(store, "_ensure_loaded_locked", None)
    selectors: dict[str, str] = {}
    for key in ("session_id", "session_key", "chat_id", "thread_id", "conversation_id"):
        value = getattr(claim, key, "")
        if isinstance(value, str) and value:
            selectors[key] = value
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
        if isinstance(entries, dict):
            entry_iter = list(entries.values())
        elif isinstance(entries, (list, tuple)):
            # Duplicate-key-capable stores: ambiguity must stay observable.
            entry_iter = list(entries)
        else:
            return OriginDecision("HOLD", "HOLD_DESTINATION_UNUSABLE")
        for entry in entry_iter:
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
            if selectors.get("conversation_id"):
                supplied_conv = selectors["conversation_id"]
                # conversation_id is a canonical stored-session selector: it must
                # agree with the stored conversation slots (chat_id, plus the
                # thread slot when present). Discord thread-as-chat shape keeps
                # the conversation in chat_id.
                if supplied_conv != target.chat_id:
                    continue
                stored_thread_conv = str(target.thread_id or "").strip()
                if stored_thread_conv and supplied_conv != stored_thread_conv:
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
        # Ambiguity is decided before any destination capability readback:
        # two stored rows satisfy the claim, so no single destination exists
        # to validate (ambiguity must not depend on runner plumbing).
        return OriginDecision("HOLD", "HOLD_SESSION_AMBIGUOUS")
    target = matches[0][1]
    # Preserve the stored profile representation for downstream delivery.
    # exact stored SessionSource profile representation.  The stored
    # canonical profile already proved equality with the claim literal via
    # _canonical_stored_profile, and the canonical session_key equality
    # check below proves the default namespace; rewriting a None/omitted
    # stored profile onto the target manufactured profile data and broke
    # the binding readback for supported omitted-profile sessions.
    # Require an exact destination capability before returning readiness.
    # Both capabilities are part of the destination contract.  Do not let a
    # partially initialized runner leak READY merely because the store row matched.
    key_fn = getattr(runner, "_session_key_for_source", None)
    if not callable(key_fn):
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
    # transport separation. The session identity is proven; now decide
    # HOW to reach it. A connected transport adapter for the stored platform
    # is used when present (Discord keeps the exact adapter path). Otherwise the
    # canonical host-level injection channel delivers the internal event with
    # no transport at all — internal admission never consults an adapter.
    resolution = _resolve_registered_adapter(
        runner,
        claim.source,
        target.platform,
        target.profile,
    )
    if resolution.status == "READY":
        return OriginDecision(
            "READY", None, target, resolution.adapter, delivery="adapter"
        )
    if resolution.status == "HOLD" and resolution.reason_code != "SKIP_UNSUPPORTED_SOURCE":
        # A transport that exists but is unhealthy/unused is a transport
        # failure, not an origin refusal (unchanged semantics) — EXCEPT an
        # absent adapter object on a claim whose registered source binding
        # matches the stored platform exactly: that transport is simply not
        # provisioned, and the canonical host-level injection channel needs no
        # transport at all (internal admission never consults an adapter). The
        # such a HOLD falls through to the host-delivery tail below, which
        # still gates on the host-eligible platform set. Unknown sources,
        # platform mismatches, and unhealthy transports keep the exact
        # typed refusal (fail closed).
        if not (
            resolution.reason_code == "HOLD_ADAPTER_OBJECT_ABSENT"
            and resolution.binding is not None
            and str(target.platform) == resolution.binding.expected_platform
        ):
            return OriginDecision(resolution.status, resolution.reason_code)
    # the host tail admits only claims whose registered source binding
    # names exactly the stored platform. An unregistered source keeps the
    # typed SKIP (fail closed) even when the stored row is host-eligible; a
    # registered discord claim reaches here only via the absent-adapter
    # fall-through above or the READY path, never past a mismatch HOLD.
    if (
        resolution.status == "SKIP"
        and resolution.reason_code == "SKIP_UNSUPPORTED_SOURCE"
    ):
        # typed skip preserved on every platform
        return OriginDecision("SKIP", "SKIP_UNSUPPORTED_SOURCE")
    if str(target.platform) not in _HOST_DELIVERY_PLATFORMS:
        return OriginDecision("SKIP", "SKIP_UNSUPPORTED_SOURCE")
    return OriginDecision("READY", None, target, None, delivery="host")


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


def _now_int() -> int:
    """Current wall clock as int seconds; the classifier's liveness input."""
    return int(time.time())


def _provenance_family_bodies(
    tasks: list[dict[str, Any]],
    links: list[tuple[str, str]],
    root_task_id: str,
) -> tuple[tuple[str | None, ...], tuple[str | None, ...]]:
    """Anchor + descendant bodies for provenance, from ONE family graph read.

    The family is the root's exact ancestor closure as built by
    ``build_root_families`` (the family "root" is the childless sink). The
    board-wide task rows are filtered down to that exact closure first: a
    body from a DIFFERENT root's tree is never this family's provenance.

    Candidate layout anchors are the closure's PARENTLESS members: members
    with no parent edge to any closure member. A legacy parentless
    single-root family uses its root as anchor (the root is then the only
    parentless member by construction). Every other member body — each
    descendant of an anchor, including the sink itself when it has parents —
    is returned as a descendant body, which is never an authority source.

    Membership is computed from the exact passed link vector restricted to
    closure members.
    """
    members = {str(row.get("id") or "") for row in tasks}
    parented: set[str] = set()
    for link in links or ():
        try:
            parent, child = link
        except (TypeError, ValueError):
            continue
        parent_id, child_id = str(parent), str(child)
        if parent_id in members and child_id in members:
            parented.add(child_id)
    # Restrict to the root's exact ancestor closure. build_root_families
    # raises on links with unknown endpoints, so the passed vector contains
    # only known ids; the closure walk follows parent edges from the root.
    children_of: dict[str, set[str]] = {}
    for parent_id, child_id in parented_pairs(links, members):
        children_of.setdefault(child_id, set()).add(parent_id)
    closure: set[str] = set()
    pending = [str(root_task_id)]
    while pending:
        current = pending.pop()
        if current in closure:
            continue
        closure.add(current)
        pending.extend(children_of.get(current, ()))
    anchor_bodies: list[str | None] = []
    descendant_bodies: list[str | None] = []
    for row in tasks:
        member_id = str(row.get("id") or "")
        if member_id not in closure:
            continue
        body = row.get("body")
        body_str = body if isinstance(body, str) else None
        if member_id in parented:
            descendant_bodies.append(body_str)
        else:
            anchor_bodies.append(body_str)
    return tuple(anchor_bodies), tuple(descendant_bodies)


def parented_pairs(
    links: list[tuple[str, str]], members: set[str]
) -> list[tuple[str, str]]:
    """In-vector (parent, child) pairs restricted to known member ids."""
    pairs: list[tuple[str, str]] = []
    for link in links or ():
        try:
            parent, child = link
        except (TypeError, ValueError):
            continue
        parent_id, child_id = str(parent), str(child)
        if parent_id in members and child_id in members:
            pairs.append((parent_id, child_id))
    return pairs


MAX_INJECTION_CHARS = 3900
INVARIANT_ACTION_SUFFIX = "\n".join(
    [
        "이 이벤트는 semantic gate(root 완료 / 분류 대기 / 외부 게이트 / 복구 소진)가 확정된 authoritative root 계열을 기존 default 대화로 되돌리는 내부 신호입니다.",
        "이 세션의 기존 대화 기록과 해당 root 계열의 현재 Kanban handoff·등록 산출물을 먼저 읽고 실제 정지 원인을 확인하십시오.",
        "이 turn은 이 기존 세션의 정상 Kanban·파일·터미널 도구를 유지합니다. 확인된 문제를 기존 권한 범위에서 진단·수정하고 결과를 검증하십시오.",
        "Root가 done으로 정상 종료된 경우에도 Kanban 상태만으로 성공을 간주하지 마십시오.",
        "정확한 root handoff, 독립 QA·검토 근거, 등록 산출물·첨부파일 및 실제 산출물을 직접 열어 존재·식별자/해시(제공된 경우)·가독성·필수 도메인 QA·원 요청 충족 여부를 평가하십시오.",
        "기준을 충족하면 검증한 정확한 최종 결과·산출물을 provenance로 확인된 이 origin 세션의 LJS에게 즉시 전달하십시오. 중복 전달이나 승인되지 않은 제3자·외부 전송은 하지 마십시오.",
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
        f"판정: {trigger.outcome} (semantic gate 확정, 연속 2회 동일 관측)",
        f"게이트 근거: {trigger.snapshot.gate_authority.event_kind if trigger.snapshot.gate_authority else 'terminal'} 권한 이벤트 #{trigger.snapshot.gate_authority.event_id if trigger.snapshot.gate_authority else '-'}",
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




def _channel_adapter_usable(adapter: Any) -> bool:
    """Adapter-channel usability: a dispatch entrypoint and a live transport.

    The api channel dispatches through deliver_wake (not handle_message), so
    usability is presence + connected only; the handle_message requirement
    stays adapter-channel exact.
    """
    return callable(getattr(adapter, "handle_message", None)) and _adapter_is_connected(
        adapter
    )


def _channel_carries_adapter(delivery: str) -> bool:
    """True when the channel dispatches through a live transport object.

    "adapter" and "api" (api_server wake loopback) both bind the
    exact resolved adapter; "host" carries none (host sentinel instead).
    """
    return delivery in ("adapter", "api")


def _api_server_binding_proof(runner: Any, target: WakeTarget) -> tuple[bool, str | None]:
    """Re-prove the COMPLETE api_server binding immediately before delivery.

    The earlier re-proof trusted the public LIMIT-1 finder plus tip equality
    (uniqueness finding): a second active/resumable row sharing the
    exact ``(profile, source="api_server", session_key)`` inserted between
    resolution and ``deliver_wake`` still passed, and the wake was delivered
    despite ambiguous destination evidence. The current code fails closed instead: the
    uniqueness/fence proof (``_api_server_resumable_row_ids`` through the
    supported public surface) is re-run at the delivery boundary, and any
    ambiguity, staleness, closure, disconnection, unusable transport, tip
    change, or unprovable state returns ``(False, typed_reason)`` with zero
    delivery. Never reduced by recency, id, or ordering.
    """
    db = _api_server_session_db(runner)
    finder = getattr(db, "find_latest_gateway_session_for_peer", None)
    if db is None or not callable(finder):
        return False, "HOLD_DESTINATION_UNUSABLE"
    try:
        row = finder(
            source=_API_SERVER_SOURCE, session_key=target.session_key)
    except Exception:
        return False, "HOLD_DESTINATION_UNUSABLE"
    if not isinstance(row, dict) or not str(row.get("id") or "").strip():
        return False, "HOLD_SESSION_UNRESOLVABLE"
    # Uniqueness proof over EVERY resumable row sharing the
    # exact key, through the same supported public seam used at resolution
    # time. Two live rows must never pass because the LIMIT-1 finder happens
    # to return the recorded tip.
    row_ids = _api_server_resumable_row_ids(db, target.session_key)
    if row_ids is None:
        return False, "HOLD_DESTINATION_UNUSABLE"
    if len(row_ids) > 1:
        return False, "HOLD_SESSION_AMBIGUOUS"
    row_id = str(row["id"])
    resolver = getattr(db, "resolve_resume_session_id", None)
    try:
        tip = str(resolver(row_id) or "") if callable(resolver) else ""
    except Exception:
        return False, "HOLD_DESTINATION_UNUSABLE"
    if (tip or row_id) != target.session_id:
        # Stale (closed/rotated past the recorded id), tip change, or
        # cross-session id: conjunctive exactness is never rescued.
        return False, HOLD_STALE_TARGET
    stored_profile = _canonical_stored_profile(row.get("profile_name"))
    if stored_profile != target.profile:
        return False, "HOLD_DESTINATION_UNUSABLE"
    return True, None


def _binding_matches(runner: Any, target: WakeTarget) -> bool:
    if getattr(target, "platform", "") == _API_SERVER_SOURCE:
        # api_server targets are SessionDB-native: the JSON session store has
        # no row for them (the gateway never writes one), so re-proof goes
        # through the same read-only SessionDB path used at resolution time —
        # exact session_key still owned by the recorded id (or its current
        # compression tip), profile and transport re-checked at dispatch.
        # this is the COMPLETE binding re-proof — unique
        # current resumable key, not merely the LIMIT-1 finder's tip echo.
        ok, _reason = _api_server_binding_proof(runner, target)
        return ok
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


def _target_matches(left: WakeTarget, right: WakeTarget) -> bool:
    return all(
        getattr(left, field) == getattr(right, field)
        for field in (
            "kind",
            "session_id",
            "session_key",
            "platform",
            "chat_id",
            "chat_name",
            "chat_type",
            "thread_id",
            "parent_chat_id",
            "user_id",
            "user_name",
            "user_id_alt",
            "chat_id_alt",
            "chat_topic",
            "scope_id",
            "guild_id",
            "profile",
        )
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
    delivery: str = "adapter",
) -> bool:
    if delivery not in ("adapter", "host", "api"):
        raise RuntimeError(f"unsupported delivery channel: {delivery}")
    from gateway.config import Platform
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.session import SessionSource

    target = trigger.target
    if not target.session_id or not target.session_key:
        raise RuntimeError("persisted target session identity is unavailable")
    if getattr(target, "platform", "") == _API_SERVER_SOURCE and delivery == "api":
        # Delivery-boundary guard: re-prove the COMPLETE binding
        # through the supported public host seam immediately before
        # deliver_wake. Any ambiguity, staleness, closure, unusable
        # transport, or tip change fails closed with a typed reason and
        # zero delivery — the wake is raised as a typed refusal, never
        # dispatched to a possibly-different session.
        ok, reason = _api_server_binding_proof(runner, target)
        if not ok:
            raise DeliveryRefusedError(
                f"pre-delivery binding re-proof refused: {reason or 'HOLD_DESTINATION_UNUSABLE'}",
                reason=reason or "HOLD_DESTINATION_UNUSABLE",
            )
    if not _binding_matches(runner, target):
        raise RuntimeError("target session binding is stale")
    if delivery == "api":
        # api_server session-addressed wake: the adapter's official loopback
        # channel (gateway.wake.deliver_wake → _self_post_chat_completion)
        # posts the turn with the exact RAW session id (X-Hermes-Session-Id).
        # The api_server platform is NOT derivable into a canonical key
        # (build_session_key mints agent:main:api_server:dm), so the
        # canonical-key reproof below is unreachable for these claims — the
        # exactness contract here is target.session_id, proven at resolution
        # time against the SessionDB tip and re-asserted by the strict-pin
        # admission path on the receiving side.
        if target.platform != _API_SERVER_SOURCE:
            raise RuntimeError(
                f"api delivery is api_server-only, got {target.platform!r}")
        resolution = _resolve_registered_adapter(
            runner,
            target.platform,
            target.platform,
            target.profile,
        )
        if resolution.status != "READY" or resolution.adapter is None:
            raise RuntimeError(resolution.reason_code or "target adapter is unavailable")
        live_adapter = resolution.adapter
        if resolved_adapter is not None and live_adapter is not resolved_adapter:
            raise RuntimeError("target adapter binding changed")
        if not _adapter_is_connected(live_adapter):
            raise RuntimeError("target adapter is not connected")
        from gateway.wake import deliver_wake

        await deliver_wake(
            live_adapter,
            text=build_injection_text(trigger),
            session_id=target.session_id,
        )
        return True
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
        # Preserve the stored parent identifier exactly through delivery.
        # emitted MessageEvent.source preserves the selected stored
        # SessionSource.parent_chat_id exactly.  A caller/default parent
        # must never fill absence: None stays None, "" stays "", and a
        # nonempty value stays byte-exact.
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
    # transport separation at dispatch. Host-delivered targets have no
    # transport adapter: the gateway's own internal-notification entrypoint
    # (runner._handle_message) is the canonical injection channel. Internal
    # events are admitted by the host without transport consultation and are
    # pinned to the canonical session by the strict gateway_session_* metadata
    # above. Adapter-identity pinning applies only to the adapter channel.
    if delivery == "host":
        if resolved_adapter is not None:
            raise RuntimeError("host delivery requires no resolved adapter")
        host_entry = getattr(runner, "_handle_message", None)
        if not callable(host_entry):
            raise RuntimeError("host-level injection entrypoint is unavailable")
        await host_entry(event)
    else:
        # Re-resolve through the exact source registry after event construction.
        resolution = _resolve_registered_adapter(
            runner,
            target.platform,
            target.platform,
            target.profile,
        )
        if resolution.status != "READY" or resolution.adapter is None:
            raise RuntimeError(resolution.reason_code or "target adapter is unavailable")
        live_adapter = resolution.adapter
        if resolved_adapter is not None and live_adapter is not resolved_adapter:
            raise RuntimeError("target adapter binding changed")
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

    def _root_claim(
        self,
        board: str,
        root_body: str | None,
        anchor_bodies: tuple[str | None, ...] = (),
        descendant_bodies: tuple[str | None, ...] = (),
    ) -> CreatorClaim | OriginDecision:
        """Resolve root provenance, with one bounded persisted-board fallback.

        Provenance is searched over the family's actual ancestor subgraph
        (contract: every parentless ancestor is a potential producer-authored
        layout anchor; the family sink itself is usually a childless sink, not
        the creator layout). Each candidate anchor body is strictly parsed by
        the unchanged ``parse_creator_provenance``:

        - exactly one explicit valid envelope among parentless anchors wins
          (the remaining anchors may have no envelope);
        - zero explicit envelopes across the anchors permits the existing
          missing-only persisted board-thread fallback;
        - any malformed/multiple-envelope anchor decision, duplicated
          envelope in one anchor, or two-or-more explicit parentless
          envelopes (even matching) fails closed — never resolved by
          timestamp, title, thread equality, or board fallback;
        - a descendant-authored envelope is not an authority source: its
          presence in any non-anchor family body is detected and rejected
          instead of silently taken or falling back.

        The persisted board thread fallback converts to the same strict
        canonical Discord session selector used by explicit provenance.
        """
        explicit_claims: list[CreatorClaim] = []
        for anchor_body in anchor_bodies:
            parsed = parse_creator_provenance(anchor_body)
            if isinstance(parsed, CreatorClaim):
                explicit_claims.append(parsed)
                continue
            if parsed.reason_code == "HOLD_PROVENANCE_MISSING":
                continue
            # malformed/multiple/unsupported envelope in a parentless anchor:
            # fail closed on the exact typed reason.
            return parsed
        for descendant_body in descendant_bodies:
            parsed = parse_creator_provenance(descendant_body)
            if not isinstance(parsed, OriginDecision):
                # A descendant (including the childless sink) is never an
                # authority source: detect and reject its envelope instead
                # of silently taking it or falling back.
                return OriginDecision("HOLD", "HOLD_PROVENANCE_DESCENDANT")
            if parsed.reason_code == "HOLD_PROVENANCE_MISSING":
                continue
            # malformed/multiple envelope below the anchors: same rejection.
            return OriginDecision("HOLD", "HOLD_PROVENANCE_DESCENDANT")
        if len(explicit_claims) > 1:
            return OriginDecision("HOLD", "HOLD_PROVENANCE_MULTIPLE")
        if explicit_claims:
            return explicit_claims[0]
        parsed = parse_creator_provenance(root_body)
        if not isinstance(parsed, OriginDecision):
            return parsed
        if parsed.reason_code != "HOLD_PROVENANCE_MISSING":
            return parsed
        try:
            thread_id = load_board_threads(
                self.route_state_path or DEFAULT_ROUTE_STATE
            ).get(board, "")
        except Exception:
            return OriginDecision("HOLD", "HOLD_BOARD_THREAD_UNPROVEN")
        if not thread_id:
            return parsed
        payload = {
            "profile": SUPPORTED_PROFILE,
            "source": "discord",
            "thread_id": thread_id,
        }
        return CreatorClaim(
            profile=SUPPORTED_PROFILE,
            source="discord",
            thread_id=thread_id,
            provenance_sha256=hashlib.sha256(_canonical_claim_bytes(payload)).hexdigest(),
        )

    async def _current_root_decision(self, trigger: Trigger) -> OriginDecision:
        """Read and resolve the root immediately before delivery.

        Returns the ready decision carrying the exact resolved adapter; on
        success the decision also binds the CURRENT family graph
        (``family_tasks``/``family_links``) read in the same pass so the
        caller can re-prove the whole posture on the very same read:
        provenance alone is not a delivery authority.
        """
        try:
            paths = dict(await asyncio.to_thread(self.board_paths))
            path = paths.get(trigger.board)
            if path is None:
                return OriginDecision("HOLD", "HOLD_CURRENT_ROOT_UNPROVEN")
            tasks, links = await asyncio.to_thread(read_board_graph, Path(path))
        except Exception:
            return OriginDecision("HOLD", "HOLD_CURRENT_ROOT_UNPROVEN")
        # Provenance comes from the same family subgraph read, exactly as at
        # observe time: parentless anchors carry authority, the root body is
        # already among them (or a descendant body), so no separate root scan.
        anchor_bodies, descendant_bodies = _provenance_family_bodies(
            tasks, links, trigger.root_task_id
        )
        parsed = self._root_claim(
            trigger.board,
            None,
            anchor_bodies=anchor_bodies,
            descendant_bodies=descendant_bodies,
        )
        if isinstance(parsed, OriginDecision):
            return parsed
        try:
            decision = await asyncio.to_thread(resolve_creator_target, self.runner, parsed)
        except Exception:
            return OriginDecision("HOLD", "HOLD_CURRENT_ROOT_UNPROVEN")
        if decision.ready:
            # Bind the exact current root family, not the whole board. Multi-root
            # boards otherwise change the fingerprint between observation and
            # dispatch even when the selected family is byte-for-byte stable.
            try:
                matching_families = [
                    family
                    for family in build_root_families(tasks, links)
                    if family.root_id == trigger.root_task_id
                ]
            except Exception:
                return OriginDecision("HOLD", "HOLD_CURRENT_ROOT_UNPROVEN")
            if len(matching_families) != 1:
                return OriginDecision("HOLD", "HOLD_CURRENT_ROOT_UNPROVEN")
            family = matching_families[0]
            decision = replace(
                decision,
                family_tasks=tuple(family.tasks),
                family_links=tuple(family.links),
            )
        # carry the delivery channel from the fresh decision so the
        # recovery fences can branch adapter-identity vs host re-proof.
        decision = replace(decision, delivery=getattr(decision, "delivery", "adapter"))
        return decision

    async def tick(self) -> dict[str, Any]:
        # Validate exact provenance first; persisted board-thread targeting is
        # consulted only for roots whose provenance envelope is absent.
        paths = dict(await asyncio.to_thread(self.board_paths))
        report: dict[str, Any] = {"status": "ok", "status_code": 0, "boards": 0, "families": 0, "triggers": 0, "fallbacks": 0, "errors": []}
        # Recover receipts before observing boards. The attempt
        # ledger splits recovery into two provable classes: an unattempted
        # fence still owes its one authorized physical send (completed only
        # after the full current-root gate re-proves everything), while an
        # attempted receipt is confirmation-only — a second physical send
        # under the one-deduplicated-message authority is forbidden, so an
        # attempted-but-unconfirmed receipt escalates instead.
        for receipt_status, trigger in self.state_store.unfinished():
            try:
                # An explicit PENDING row is recovered through the
                # marker-first pending path BEFORE the attempt-ledger
                # fence below. For pending the episode was never dispatched,
                # so no send is "owed" yet: a durable acceptance marker (if
                # present) confirms with zero injections, and only an
                # unconfirmed pending row may fence-and-dispatch exactly
                # once. The earlier unconditional branch exit after the
                # unattempted-fence completion made this whole path dead
                # code, so a marker-confirmed pending row was physically
                # injected again without consulting its marker.
                if receipt_status == "pending":
                    recovery, pending_gate_payload = await self._recover_pending_receipt(trigger)
                    refused_reason = (
                        pending_gate_payload
                        if recovery == "refused" and isinstance(pending_gate_payload, str)
                        else "HOLD_CURRENT_ROOT_UNPROVEN"
                    )
                    if recovery == "confirmed":
                        # Marker consulted only after the full current-root,
                        # target, posture, and transport proof: zero physical
                        # injections, terminal for the receipt.
                        self.state_store.set_status(trigger, "confirmed")
                        self.state_store.record_evidence(
                            EVIDENCE_CONFIRMATION,
                            board=trigger.board,
                            root_task_id=trigger.root_task_id,
                            review_key=trigger.review_key,
                            episode=trigger.episode,
                        )
                        report["triggers"] += 1
                    elif recovery == "refused":
                        # Any gate failure: typed uncertain hold, zero
                        # actions, exact reason preserved; the unresolved
                        # escalation below keeps it visible every poll.
                        self.state_store.set_status(
                            trigger,
                            "uncertain",
                            error=f"pending recovery refused: {refused_reason}",
                        )
                        self.state_store.record_evidence(
                            EVIDENCE_RECOVERY_HOLD,
                            board=trigger.board,
                            root_task_id=trigger.root_task_id,
                            review_key=trigger.review_key,
                            episode=trigger.episode,
                        )
                        report["errors"].append(
                            f"{trigger.board}/{trigger.root_task_id}: pending recovery refused: {refused_reason}"
                        )
                    else:
                        # recovery == "dispatched": the marker is absent and
                        # the full current-root/target/posture/transport
                        # proof passed, so this is the episode's ONE
                        # authorized recovery dispatch. Fence
                        # a pending receipt durably before entering the
                        # handler — status scheduled plus the attempt stamp
                        # move it past pending, so a later poll can never
                        # re-enter this path and a crash leaves an attempted
                        # (confirmation-only) receipt, never a resend.
                        resolved_adapter = (
                            pending_gate_payload.get("resolved_adapter")
                            if recovery == "dispatched" and isinstance(pending_gate_payload, dict)
                            else None
                        )
                        pending_recovery_delivery = (
                            pending_gate_payload.get("delivery", "adapter")
                            if recovery == "dispatched" and isinstance(pending_gate_payload, dict)
                            else "adapter"
                        )
                        self.state_store.set_status(trigger, "scheduled")
                        self.state_store.record_evidence(
                            EVIDENCE_DISPATCH,
                            board=trigger.board,
                            root_task_id=trigger.root_task_id,
                            review_key=trigger.review_key,
                            episode=trigger.episode,
                        )
                        self.state_store.mark_attempted(trigger.review_key)
                        try:
                            confirmed = await self.injector(
                                self.runner,
                                trigger,
                                parent_channel_id=self.parent_channel_id,
                                resolved_adapter=resolved_adapter,
                                delivery=pending_recovery_delivery,
                            )
                        except asyncio.CancelledError:
                            self.state_store.set_status(
                                trigger,
                                "uncertain",
                                error="pending injection cancelled without durable transcript confirmation",
                            )
                            raise
                        if confirmed:
                            self.state_store.set_status(trigger, "confirmed")
                            self.state_store.record_evidence(
                                EVIDENCE_CONFIRMATION,
                                board=trigger.board,
                                root_task_id=trigger.root_task_id,
                                review_key=trigger.review_key,
                                episode=trigger.episode,
                            )
                            report["triggers"] += 1
                        else:
                            self.state_store.set_status(
                                trigger,
                                "uncertain",
                                error="injection returned without durable transcript confirmation",
                            )
                    continue
                # Recovery model, exactly one authorized physical send
                # per receipt. The attempt ledger splits the old confirmation-
                # only fence into two provable classes (receipts that are NOT
                # pending; the pending class above is marker-first):
                #   attempted_at IS NULL -> the fence was committed but no
                #       injector call began. The episode's single promised
                #       send is still OWED: complete it through the full
                #       non-pending gate chain below (a crash between the
                #       durable schedule and delivery is repaired by the one
                #       send the episode already authorized).
                #   attempted_at NOT NULL -> the attempt began; whether it
                #       landed is unknowable, so the receipt is confirmation-
                #       only and unconfirmed receipts escalate (never resend).
                # A legacy row with no ledger entry migrates to attempted at
                # open (_backfill_attempted_at): the pre-ledger store cannot
                # prove its send never started, so it fails closed.
                attempted = self.state_store.receipt_attempted(trigger.review_key)
                if attempted:
                    gate, gate_reason = await self._confirm_nonpending_receipt(trigger)
                    if gate == "confirmed":
                        self.state_store.set_status(trigger, "confirmed")
                        report["triggers"] += 1
                        continue
                    # Proof-complete-but-unconfirmed, or an unproven current
                    # root: the receipt stays explicitly uncertain. Recovery
                    # is confirmation-only; the unresolved escalation below
                    # keeps the gate visible to the operator every poll.
                    if gate == "present":
                        error = "non-pending receipt confirmation absent after non-resend fence"
                    else:
                        error = f"non-pending receipt gate unproven: {gate_reason}"
                    self.state_store.set_status(trigger, "uncertain", error=error)
                    report["errors"].append(
                        f"{trigger.board}/{trigger.root_task_id}: {error}"
                    )
                    continue
                # Unattempted non-pending fence (scheduled /
                # awaiting_confirmation / uncertain): re-prove the CURRENT
                # root before the one owed send. The persisted target alone
                # cannot authorize anything; every gate below must hold
                # exactly as a fresh dispatch requires. This fence must NOT
                # serve the pending class — a pending receipt owes nothing
                # until the marker-first branch above has proved its
                # posture and marker.
                gate, gate_payload = await self._recovery_dispatch_gate(trigger)
                if gate != "dispatched":
                    refused_reason = (
                        gate_payload
                        if isinstance(gate_payload, str)
                        else "HOLD_CURRENT_ROOT_UNPROVEN"
                    )
                    error = f"unattempted fence held without dispatch: {refused_reason}"
                    self.state_store.set_status(trigger, "uncertain", error=error)
                    self.state_store.record_evidence(
                        EVIDENCE_RECOVERY_HOLD,
                        board=trigger.board,
                        root_task_id=trigger.root_task_id,
                        review_key=trigger.review_key,
                        episode=trigger.episode,
                    )
                    report["errors"].append(
                        f"{trigger.board}/{trigger.root_task_id}: {error}"
                    )
                    continue
                # The dispatched payload is the full channel
                # contract (same shape the pending branch forwards). The
                # channel must reach inject_trigger exactly: the adapter/api
                # identity pins the transport, delivery="host" with
                # resolved_adapter=None selects the host branch, and any
                # other shape fails closed BEFORE the attempt stamp — the
                # fence stays unattempted and owed, never spent on a guessed
                # or defaulted route.
                if not (
                    isinstance(gate_payload, dict)
                    and set(gate_payload.keys()) == {"resolved_adapter", "delivery"}
                    and isinstance(gate_payload.get("delivery"), str)
                    and gate_payload.get("delivery") in ("adapter", "host", "api")
                    and _channel_carries_adapter(gate_payload["delivery"])
                    == (gate_payload.get("resolved_adapter") is not None)
                ):
                    error = (
                        "unattempted fence held without dispatch: "
                        "HOLD_DISPATCH_PAYLOAD_INVALID"
                    )
                    self.state_store.set_status(trigger, "uncertain", error=error)
                    self.state_store.record_evidence(
                        EVIDENCE_RECOVERY_HOLD,
                        board=trigger.board,
                        root_task_id=trigger.root_task_id,
                        review_key=trigger.review_key,
                        episode=trigger.episode,
                    )
                    report["errors"].append(
                        f"{trigger.board}/{trigger.root_task_id}: {error}"
                    )
                    continue
                resolved_adapter = gate_payload["resolved_adapter"]
                recovery_delivery = gate_payload["delivery"]
                # The owed single send: complete the unattempted fence through
                # the same handler contract a fresh dispatch uses. The fence
                # is already non-pending; the attempt stamp goes in right
                # before the injector await (one-way).
                self.state_store.record_evidence(
                    EVIDENCE_DISPATCH,
                    board=trigger.board,
                    root_task_id=trigger.root_task_id,
                    review_key=trigger.review_key,
                    episode=trigger.episode,
                )
                self.state_store.mark_attempted(trigger.review_key)
                try:
                    confirmed = await self.injector(
                        self.runner,
                        trigger,
                        parent_channel_id=self.parent_channel_id,
                        resolved_adapter=resolved_adapter,
                        delivery=recovery_delivery,
                    )
                except asyncio.CancelledError:
                    self.state_store.set_status(
                        trigger,
                        "uncertain",
                        error="pending injection cancelled without durable transcript confirmation",
                    )
                    raise
                if confirmed:
                    self.state_store.set_status(trigger, "confirmed")
                    self.state_store.record_evidence(
                        EVIDENCE_CONFIRMATION,
                        board=trigger.board,
                        root_task_id=trigger.root_task_id,
                        review_key=trigger.review_key,
                        episode=trigger.episode,
                    )
                    report["triggers"] += 1
                else:
                    self.state_store.set_status(
                        trigger,
                        "uncertain",
                        error="recovery injection returned without durable transcript confirmation",
                    )
                    continue
                # Branch exit: only a durably confirmed
                # receipt reaches here (every other sub-branch above already
                # continues). That confirmation is terminal for this
                # receipt's recovery; the seeded pending class never enters
                # this fence at all (the marker-first pending branch above
                # owns it).
            except Exception as exc:
                self.state_store.set_status(trigger, "uncertain", error=_clip(exc, 300))
                report["errors"].append(
                    f"{trigger.board}/{trigger.root_task_id}: recovery {type(exc).__name__}: {_clip(exc, 200)}"
                )
        # Bounded operator visibility: every receipt that left
        # "pending" and has never reached "confirmed" is surfaced on EVERY
        # tick report until it resolves, together with the durable attempt
        # distinction and receipt age. This is a bounded report (compact
        # identifiers, no free-form payload), not a resend and not an
        # authority expansion; a permanently disconnected transport cannot
        # be guaranteed to deliver and this report says so durably instead
        # of letting the gate silently disappear.
        escalations = self.state_store.unresolved_escalations()
        if escalations:
            report["unresolved"] = [dict(entry) for entry in escalations]
            report["errors"].append(
                "operator escalation: %d unresolved non-pending receipt(s) with no durable "
                "acceptance; recovery is confirmation-only under the one-deduplicated-message "
                "authority and delivery cannot be guaranteed while the target transport is "
                "disconnected" % len([e for e in escalations if e.get("status") != "pending_unfinished"])
            )
        for board, path in sorted(paths.items()):
            try:
                tasks, links = await asyncio.to_thread(read_board_graph, Path(path))
                families = build_root_families(tasks, links)
                root_created_at = {
                    str(row.get("id") or ""): row.get("created_at")
                    for row in tasks
                }
                families = tuple(
                    sorted(
                        families,
                        key=lambda family: (
                            root_created_at.get(family.root_id)
                            if isinstance(root_created_at.get(family.root_id), (int, float))
                            else -1,
                            family.root_id,
                        ),
                        reverse=True,
                    )
                )
                seen_authorities: set[tuple[Any, ...]] = set()
                report["boards"] += 1
                for family in families:
                    # Explicit creator provenance is authoritative. It is
                    # searched over the family's actual ancestor subgraph:
                    # parentless members are producer-authored layout anchors,
                    # and exactly one valid envelope among them wins. Only a
                    # genuinely missing envelope across all anchors may use
                    # the persisted board Discord thread as the exact
                    # stored-session selector.
                    anchor_bodies, descendant_bodies = _provenance_family_bodies(
                        tasks, links, family.root_id
                    )
                    parsed = self._root_claim(
                        board,
                        None,
                        anchor_bodies=anchor_bodies,
                        descendant_bodies=descendant_bodies,
                    )
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
                    # Forward the family's own board links into the classifier;
                    # omitting them would let runnable members behind a
                    # verified block suppress it (fail open). The full vector
                    # must survive: `links` is reassigned per family so the
                    # NEXT family's provenance closure still sees the whole
                    # board graph, exactly as read.
                    family_links = family.links
                    snapshot = classify_family(
                        family.tasks, root_task_id=family.root_id, links=family_links, now=_now_int()
                    )
                    report["families"] += 1
                    authority = snapshot.gate_authority
                    if authority is not None:
                        authority_key = (
                            board,
                            target.key,
                            snapshot.state,
                            authority.task_id,
                            authority.event_id,
                            authority.event_run_id,
                            authority.event_kind,
                            json.dumps(
                                authority.payload,
                                ensure_ascii=False,
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                        )
                        if authority_key in seen_authorities:
                            continue
                        seen_authorities.add(authority_key)
                    trigger = self.state_store.observe(
                        board,
                        family.root_id,
                        family.root_title,
                        target,
                        snapshot,
                    )
                    if trigger is None:
                        # No new trigger: either debounce (observation 1) or a
                        # receipt for the exact current posture already exists
                        # (dedup/hold, incl. an unfinished one owned by
                        # recovery). Neither is a miss: an eligible posture is
                        # durably accounted for. Pure non-eligible postures
                        # never reach observe() as eligible.
                        continue
                    current = await self._current_root_decision(trigger)
                    # the fresh and retained gates branch on the delivery
                    # channel. Adapter targets keep the exact live-identity contract
                    # (adapter present, connected, identity retained);
                    # host targets require the absence of a transport
                    # object (host delivery carries none) and pass the
                    # channel into inject_trigger for the dispatch branch.
                    _delivery = getattr(current, "delivery", "adapter")
                    _channel_ok = _channel_carries_adapter(
                        _delivery
                    ) == (current.adapter is not None)
                    if (
                        not current.ready
                        or current.target is None
                        or not _channel_ok
                        or not _target_matches(current.target, trigger.target)
                    ):
                        reason = current.reason_code or "HOLD_CURRENT_TARGET_MISMATCH"
                        error = f"fresh origin/target/adapter gate refused dispatch: {reason}"
                        self.state_store.set_status(trigger, "uncertain", error=error)
                        self.state_store.record_evidence(
                            EVIDENCE_TARGET_HOLD,
                            board=trigger.board,
                            root_task_id=trigger.root_task_id,
                            review_key=trigger.review_key,
                            episode=trigger.episode,
                        )
                        report["errors"].append(
                            f"{board}/{family.root_id}: {error}"
                        )
                        continue
                    # Re-prove the posture on the decision's
                    # own bound graph immediately before the dispatch fence. A
                    # concurrent unblock/claim/new-runnable handoff, kind
                    # change, or archival between the family scan and this
                    # proof fails closed with zero injection.
                    if not current.current_gate_holds(trigger):
                        reason = current.reason_code or "HOLD_SEMANTIC_GATE_CHANGED"
                        error = f"fresh semantic gate re-proof refused dispatch: {reason}"
                        self.state_store.set_status(trigger, "uncertain", error=error)
                        report["errors"].append(
                            f"{board}/{family.root_id}: {error}"
                        )
                        continue
                    try:
                        self.state_store.set_status(trigger, "scheduled")
                        self.state_store.mark_attempted(trigger.review_key)
                        confirmed = await self.injector(
                            self.runner,
                            trigger,
                            parent_channel_id=self.parent_channel_id,
                            resolved_adapter=current.adapter if _channel_carries_adapter(_delivery) else None,
                            delivery=_delivery,
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
                            # Operator contract: a scheduling ACK is
                            # not delivery. Zero durable-transcript proof keeps
                            # the receipt typed uncertain and surfaces the
                            # unresolved eligible gate as a concrete tick
                            # failure — never an invisible success.
                            error = "injection returned without durable transcript confirmation"
                            self.state_store.set_status(trigger, "uncertain", error=error)
                            report["errors"].append(
                                f"{board}/{family.root_id}: {error}"
                            )
            except Exception as exc:
                report["errors"].append(f"{board}: {type(exc).__name__}: {_clip(exc, 200)}")
                logger.warning("kanban origin review tick failed for %s: %s", board, exc)
        if report["errors"]:
            report["status"] = "partial"
            report["status_code"] = 1
        return report

    async def _confirm_nonpending_receipt(
        self, trigger: Trigger
    ) -> tuple[str, str | None]:
        """Confirmation-only recovery for a non-pending unfinished receipt.

        The current root is re-proved FIRST. Only when the root exists, parses,
        resolves, carries supported provenance, resolves to exactly the
        persisted target, and the retained adapter is ready may the durable
        marker be consulted.

        Returns one of:
          ("confirmed", None)                 -- marker present after the full
                                                 current-root proof.
          ("present", None)                   -- proof complete, marker absent;
                                                 the caller demotes the receipt
                                                 to typed uncertain.
          ("unproven", <typed reason code>)   -- the current root could not be
                                                 proved (exact parse/resolve/
                                                 readiness/adapter reason);
                                                 zero actions, zero marker
                                                 consultation.
        """
        decision = await self._current_root_decision(trigger)
        # channel-aware retained-transport re-proof. Adapter targets
        # require the live adapter identity exactly; host targets
        # require no transport object plus the same full current proof.
        _delivery = getattr(decision, "delivery", "adapter")
        _channel_ok = _channel_carries_adapter(
            _delivery
        ) == (decision.adapter is not None)
        if (
            not decision.ready
            or decision.target is None
            or not _channel_ok
            or not _target_matches(decision.target, trigger.target)
        ):
            return "unproven", decision.reason_code or "HOLD_CURRENT_ROOT_UNPROVEN"
        # Confirmation also requires the receipt's posture
        # to hold on the CURRENT family graph; a posture that disappeared or
        # changed cannot turn an old receipt into a current confirmation.
        if not decision.current_gate_holds(trigger):
            return "unproven", "HOLD_SEMANTIC_GATE_CHANGED"
        if _channel_carries_adapter(_delivery):
            if decision.adapter is not self._retained_pending_adapter(trigger, _delivery):
                return "unproven", "HOLD_DESTINATION_UNUSABLE"
            if not _channel_adapter_usable(decision.adapter):
                return "unproven", "HOLD_DESTINATION_UNUSABLE"
        try:
            marker_confirmed = await trigger_is_confirmed(self.runner, trigger)
        except Exception:
            return "unproven", "HOLD_CURRENT_ROOT_UNPROVEN"
        # The marker lookup is an await boundary. Re-prove the canonical root
        # even when the marker was present; a changed authority root must not
        # turn an old confirmation into a current receipt.
        final = await self._current_root_decision(trigger)
        # await-boundary re-proof, channel-aware (same contract as the
        # first proof above).
        _delivery = getattr(final, "delivery", "adapter")
        _channel_ok = _channel_carries_adapter(
            _delivery
        ) == (final.adapter is not None)
        if (
            not final.ready
            or final.target is None
            or not _channel_ok
            or not _target_matches(final.target, trigger.target)
        ):
            return "unproven", final.reason_code or "HOLD_CURRENT_ROOT_UNPROVEN"
        # Await-boundary re-proof -- the marker lookup may have
        # raced a posture change; fail closed after it.
        if not final.current_gate_holds(trigger):
            return "unproven", "HOLD_SEMANTIC_GATE_CHANGED"
        if _channel_carries_adapter(_delivery):
            if final.adapter is not self._retained_pending_adapter(trigger, _delivery):
                return "unproven", "HOLD_DESTINATION_UNUSABLE"
            if not _channel_adapter_usable(final.adapter):
                return "unproven", "HOLD_DESTINATION_UNUSABLE"
        return ("confirmed", None) if marker_confirmed else ("present", None)

    async def _recovery_dispatch_gate(
        self, trigger: Trigger
    ) -> tuple[str, Any]:
        """Full current-root gate for completing the ONE unattempted send.

        A receipt fenced to "scheduled" whose attempt stamp is still
        NULL owes the episode's single promised physical send. This gate
        re-proves everything a fresh dispatch requires before that send may
        be completed by recovery. Payload shapes mirror
        _recover_pending_receipt: "dispatched" carries the channel-shaped
        resolved_adapter — the live adapter object for adapter/api channels,
        None for the host channel (delivery="host" tells inject_trigger to
        use its host branch, which requires adapter absence); every refusal
        carries its typed reason code.

        Returns one of:
          ("dispatched", {"resolved_adapter": <adapter|None>, "delivery": <channel>})
          ("refused", <typed reason code>)
        """
        decision = await self._current_root_decision(trigger)
        # channel-aware retained-transport re-proof (adapter identity vs
        # host sentinel), identical contract to the confirmation path.
        _delivery = getattr(decision, "delivery", "adapter")
        _channel_ok = _channel_carries_adapter(
            _delivery
        ) == (decision.adapter is not None)
        if (
            not decision.ready
            or decision.target is None
            or not _channel_ok
            or not _target_matches(decision.target, trigger.target)
        ):
            return "refused", decision.reason_code or "HOLD_CURRENT_ROOT_UNPROVEN"
        # The persisted receipt's own posture must hold on the CURRENT family
        # graph; a fence whose posture changed must never complete its send.
        if not decision.current_gate_holds(trigger):
            return "refused", "HOLD_SEMANTIC_GATE_CHANGED"
        # Host-delivered fences carry the sentinel (no transport object);
        # requiring decision.adapter == sentinel here would contradict the
        # decision's own None adapter. The sentinel comparison applies only
        # where the delivery channel actually carries an adapter object.
        if _channel_carries_adapter(_delivery):
            if decision.adapter is not self._retained_pending_adapter(trigger, _delivery):
                return "refused", "HOLD_DESTINATION_UNUSABLE"
            if not _channel_adapter_usable(decision.adapter):
                return "refused", "HOLD_DESTINATION_UNUSABLE"
        else:
            if decision.adapter is not None:
                return "refused", "HOLD_DESTINATION_UNUSABLE"
        final = await self._current_root_decision(trigger)
        # await-boundary re-proof, channel-aware (same contract as the
        # confirmation path): every await may have raced a posture or
        # target change; fail closed after each one.
        _delivery = getattr(final, "delivery", "adapter")
        _channel_ok = _channel_carries_adapter(
            _delivery
        ) == (final.adapter is not None)
        if (
            not final.ready
            or final.target is None
            or not _channel_ok
            or not _target_matches(final.target, trigger.target)
        ):
            return "refused", final.reason_code or "HOLD_CURRENT_ROOT_UNPROVEN"
        if not final.current_gate_holds(trigger):
            return "refused", "HOLD_SEMANTIC_GATE_CHANGED"
        if _channel_carries_adapter(_delivery):
            if final.adapter is not self._retained_pending_adapter(trigger, _delivery):
                return "refused", "HOLD_DESTINATION_UNUSABLE"
            if not _channel_adapter_usable(final.adapter):
                return "refused", "HOLD_DESTINATION_UNUSABLE"
        else:
            if final.adapter is not None:
                return "refused", "HOLD_DESTINATION_UNUSABLE"
        # DISPATCH FENCE: this receipt's attempt stamp
        # is still NULL, so this is the FIRST injector entry for the episode,
        # not a second one — completing it honors the one-deduplicated-
        # internal-message authority exactly. mark_attempted() is stamped by
        # the caller before the injector await.
        # The payload carries the full channel contract the caller
        # must forward to inject_trigger — the exact live adapter identity
        # for adapter/api channels (api_server dispatches through its own
        # wake loopback), resolved_adapter=None with delivery="host" for the
        # host channel (inject_trigger's host branch requires absence), and
        # the exact decision channel as delivery. The caller must not
        # default, guess, or translate the channel: an unknown/mismatched
        # shape fails closed below before any dispatch.
        return "dispatched", {
            "resolved_adapter": (
                final.adapter if _channel_carries_adapter(_delivery) else None
            ),
            "delivery": _delivery,
        }

    async def _recover_pending_receipt(
        self, trigger: Trigger
    ) -> tuple[str, Any]:
        """Re-prove a persisted pending receipt against the current root.

        Returns one typed outcome plus a payload (None unless "dispatched";
        for "refused" the exact typed reason code that failed the gate):
          "confirmed"  -- durable acceptance exists and was consulted only
                          after the full current-root/target/transport proof;
                          no dispatch.
          "dispatched" -- current-root provenance, target agreement and the
                          retained transport identity all re-proved; the
                          caller must forward the channel-shaped payload
                          verbatim (the exact live adapter object for
                          adapter/api channels, resolved_adapter=None with
                          delivery="host" for the host channel) so
                          inject_trigger rejects any swap before handle.
          "refused"    -- any gate failed; caller must hold the receipt
                          uncertain with zero handle_message and persist the
                          exact typed reason, never a generic collapse.
        """
        # Current root proof precedes marker lookup or dispatch.  A persisted
        # target alone cannot authorize recovery.
        decision = await self._current_root_decision(trigger)
        # channel/adapter pairing gate: a decision whose delivery channel
        # carries an adapter must have one, and a host decision must have
        # none — the retained-transport proof below branches the same way,
        # identical contract to the confirmation path.
        _delivery = getattr(decision, "delivery", "adapter")
        if _delivery not in ("adapter", "host", "api"):
            # An unsupported channel is never guessed, defaulted,
            # or translated — inject_trigger would reject it only AFTER the
            # attempt stamp was spent, so the pending gate refuses typed
            # BEFORE the marker lookup (zero actions, stamp untouched).
            return "refused", "HOLD_DISPATCH_CHANNEL_UNSUPPORTED"
        _channel_ok = _channel_carries_adapter(
            _delivery
        ) == (decision.adapter is not None)
        if (
            not decision.ready
            or decision.target is None
            or not _channel_ok
            or not _target_matches(decision.target, trigger.target)
        ):
            return "refused", decision.reason_code or "HOLD_CURRENT_ROOT_UNPROVEN"
        # The persisted receipt's own posture must hold on
        # the CURRENT family graph. An old receipt whose posture
        # disappeared or changed (blocked -> running, kind flip, membership
        # change) while the root stayed non-archived must never dispatch.
        if not decision.current_gate_holds(trigger):
            return "refused", "HOLD_SEMANTIC_GATE_CHANGED"
        # Exact agreement between the newly resolved target and the
        # persisted receipt target across the full stored identity.
        if not _target_matches(decision.target, trigger.target):
            return "refused", decision.reason_code or "HOLD_CURRENT_ROOT_UNPROVEN"
        # Same retained-current transport identity, channel-aware:
        # the host decision's adapter is None, so an unconditional
        # sentinel comparison refused every exact host pending before the
        # marker lookup and demoted it to the markerless unattempted path.
        # The host channel re-proves "no transport object" plus the full
        # current gate above; the sentinel retains only its
        # _retained_pending_adapter role (channel identity token), exactly
        # as the confirmation path and _recovery_dispatch_gate already
        # branch.
        if _channel_carries_adapter(_delivery):
            if decision.adapter is not self._retained_pending_adapter(trigger, _delivery):
                return "refused", "HOLD_DESTINATION_UNUSABLE"
            if not _channel_adapter_usable(decision.adapter):
                return "refused", "HOLD_DESTINATION_UNUSABLE"
        else:
            if decision.adapter is not None:
                return "refused", "HOLD_DESTINATION_UNUSABLE"
        # Proof complete: only now may a durable marker confirm without
        # resend.  trigger_is_confirmed re-reads the store around the marker
        # lookup, so a concurrent target change still fails closed here.
        try:
            marker_confirmed = await trigger_is_confirmed(self.runner, trigger)
        except Exception:
            return "refused", "HOLD_CURRENT_ROOT_UNPROVEN"
        # The marker lookup is an await boundary. Re-prove the canonical root
        # even when the marker was present; a changed authority root must not
        # turn an old confirmation into a current receipt.
        final = await self._current_root_decision(trigger)
        # await-boundary re-proof, channel/adapter pairing gate (same
        # contract as the first proof above).
        _delivery = getattr(final, "delivery", "adapter")
        if _delivery not in ("adapter", "host", "api"):
            # A channel flipped to an unsupported value across the
            # marker-lookup await is refused typed before the dispatch
            # payload is built (never guessed, never stamped).
            return "refused", "HOLD_DISPATCH_CHANNEL_UNSUPPORTED"
        _channel_ok = _channel_carries_adapter(
            _delivery
        ) == (final.adapter is not None)
        if (
            not final.ready
            or final.target is None
            or not _channel_ok
            or not _target_matches(final.target, trigger.target)
        ):
            return "refused", final.reason_code or "HOLD_CURRENT_ROOT_UNPROVEN"
        # After EVERY await boundary the posture must hold
        # again -- the marker lookup may have raced an unblock, claim, new
        # runnable handoff, kind change, or archival.
        if not final.current_gate_holds(trigger):
            return "refused", "HOLD_SEMANTIC_GATE_CHANGED"
        # Await-boundary re-proof, channel-aware: the identical
        # contract as the first proof above. The host decision's adapter is
        # None, so the sentinel comparison would have refused every exact
        # host pending here as well — after the marker had already said
        # "confirmed"; the host channel re-proves "no transport object" and
        # the adapter channel re-pins the exact live adapter identity.
        if _channel_carries_adapter(_delivery):
            if final.adapter is not self._retained_pending_adapter(trigger, _delivery):
                return "refused", "HOLD_DESTINATION_UNUSABLE"
            if not _channel_adapter_usable(final.adapter):
                return "refused", "HOLD_DESTINATION_UNUSABLE"
        else:
            if final.adapter is not None:
                return "refused", "HOLD_DESTINATION_UNUSABLE"
        # DISPATCH FENCE: returning "dispatched" authorizes an
        # injection. inject_trigger itself performs await-boundary work (session
        # load, transcript replay, message send) using the persisted target, so
        # an already-dispatched trigger must never re-enter this method and be
        # dispatched a second time. The persisted store state must therefore
        # have moved past "pending" by the time a later recovery consults the
        # marker: enforce the one-way pending -> scheduled fence here by
        # re-checking the receipt status recorded in the state store. If the
        # fenced status is missing (store replaced, import rollback, root
        # recreated), refuse instead of re-dispatching on outdated authority.
        # NOTE: this refusal is decided on the CURRENT call's own posture proof,
        # not on the marker: a confirmed marker returns confirmation-only.
        # The fence now carries the durable attempt stamp — the
        # caller marks attempted_at immediately before the injector await, so
        # a crash after this point leaves an attempted receipt (escalation-
        # only) rather than re-entering this pending path forever.
        if marker_confirmed:
            return "confirmed", None
        # the dispatched payload is channel-shaped. Adapter and api
        # targets return the exact live adapter (inject_trigger re-pins
        # identity); host targets return resolved_adapter=None with
        # delivery="host" so inject_trigger's host branch (which requires
        # adapter absence) is the only authorized dispatch. The caller
        # forwards both kwargs verbatim.
        return "dispatched", {
            "resolved_adapter": (
                final.adapter if _channel_carries_adapter(_delivery) else None
            ),
            "delivery": _delivery,
        }

    def _retained_pending_adapter(self, trigger: Trigger, delivery: str = "adapter") -> Any:
        """Resolve the retained transport identity through the exact registry.

        Adapter targets keep the exact live-identity contract (live adapter
        object). Host-delivered targets carry the host sentinel: the channel
        identity is re-proved by the full current gate around every use, so
        the sentinel only asserts "this receipt was host-delivered".
        """
        if delivery == "host":
            return HOST_DELIVERY_SENTINEL
        resolution = _resolve_registered_adapter(
            self.runner,
            trigger.target.platform,
            trigger.target.platform,
            trigger.target.profile,
        )
        if resolution.status != "READY":
            return None
        return resolution.adapter

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

