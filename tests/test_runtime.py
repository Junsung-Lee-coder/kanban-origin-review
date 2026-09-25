"""Isolated functional tests for the public plugin source.

Every test is synthetic and privacy-safe: fixture identifiers are invented
strings, no real board/session/Discord identifiers appear, and the suite
installs local shims for the Hermes Gateway types so no live Gateway,
plugin, board, or session state is ever opened.
"""
from __future__ import annotations

import asyncio
import enum
import hashlib
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import types
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
PACKET = PLUGIN_ROOT


# ---- isolated gateway dependency shims -----------------------------------------
hermes_cli = types.ModuleType("hermes_cli")
hermes_cli.__path__ = []
hermes_cli_db = types.ModuleType("hermes_cli.kanban_db")
hermes_cli_db._STALE_HEARTBEAT_GAP_SECONDS = 3600
sys.modules["hermes_cli"] = hermes_cli
sys.modules["hermes_cli.kanban_db"] = hermes_cli_db

gateway = types.ModuleType("gateway")
gateway.__path__ = []
gateway_config = types.ModuleType("gateway.config")
gateway_session = types.ModuleType("gateway.session")
gateway_platforms = types.ModuleType("gateway.platforms")
gateway_platforms.__path__ = []
gateway_base = types.ModuleType("gateway.platforms.base")


class Platform(enum.Enum):
    """Enum-shaped stand-in for the installed gateway Platform."""

    DISCORD = "discord"
    API_SERVER = "api_server"
    LOCAL = "local"
    RELAY = "relay"


gateway_config.Platform = Platform


@dataclass(frozen=True)
class SessionSource:
    platform: Any = None
    chat_id: str = ""
    chat_name: Any = None
    chat_type: str = ""
    thread_id: Any = None
    parent_chat_id: Any = None
    user_id: Any = None
    user_name: Any = None
    user_id_alt: Any = None
    chat_id_alt: Any = None
    chat_topic: Any = None
    scope_id: Any = None
    guild_id: Any = None
    profile: Any = None


def session_key_for_source(source: SessionSource) -> str:
    profile = source.profile or "default"
    return f"discord:{profile}:{source.chat_id}:{source.chat_id}"


gateway_session.SessionSource = SessionSource
gateway_session.build_session_key = session_key_for_source


@dataclass
class MessageEvent:
    text: str = ""
    message_type: Any = None
    source: Any = None
    message_id: Any = None
    internal: bool = False
    allow_gateway_control: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


class MessageType:
    TEXT = "text"


gateway_base.MessageEvent = MessageEvent
gateway_base.MessageType = MessageType
sys.modules["gateway"] = gateway
sys.modules["gateway.config"] = gateway_config
sys.modules["gateway.session"] = gateway_session
sys.modules["gateway.platforms"] = gateway_platforms
sys.modules["gateway.platforms.base"] = gateway_base


# Load plugin modules without importing the bootstrap entry point.
package_name = "_kanban_origin_review_under_test"
package = types.ModuleType(package_name)
package.__path__ = [str(PACKET)]
package.__package__ = package_name
sys.modules[package_name] = package

core_spec = importlib.util.spec_from_file_location(
    f"{package_name}.core", PACKET / "core.py"
)
assert core_spec is not None and core_spec.loader is not None
core_mod = __import__("importlib.util").util.module_from_spec(core_spec)
sys.modules[f"{package_name}.core"] = core_mod
core_spec.loader.exec_module(core_mod)

runtime_spec = importlib.util.spec_from_file_location(
    f"{package_name}.runtime", PACKET / "runtime.py"
)
assert runtime_spec is not None and runtime_spec.loader is not None
mod = __import__("importlib.util").util.module_from_spec(runtime_spec)
sys.modules[f"{package_name}.runtime"] = mod
runtime_spec.loader.exec_module(mod)


# ---- fixtures -------------------------------------------------------------------
PROFILE = "default"
CREATOR_CHAT = "synthetic-creator-chat"
WRONG_SINK_CHAT = "synthetic-wrong-sink"
SHARED_THREAD = "synthetic-shared-thread"


def make_source(
    chat_id: str,
    *,
    thread_id: str | None = None,
    chat_type: str = "thread",
    profile: str | None = PROFILE,
    chat_name: str | None = None,
    parent_chat_id: str | None = None,
    user_id: str | None = None,
    user_name: str | None = None,
    user_id_alt: str | None = None,
    chat_id_alt: str | None = None,
    chat_topic: str | None = None,
    scope_id: str | None = None,
    guild_id: str | None = None,
) -> SessionSource:
    return SessionSource(
        platform=Platform.DISCORD,
        chat_id=chat_id,
        chat_name=chat_name,
        chat_type=chat_type,
        thread_id=thread_id,
        parent_chat_id=parent_chat_id,
        user_id=user_id,
        user_name=user_name,
        user_id_alt=user_id_alt,
        chat_id_alt=chat_id_alt,
        chat_topic=chat_topic,
        scope_id=scope_id,
        guild_id=guild_id,
        profile=profile,
    )


class Entry:
    def __init__(self, session_id: str, session_key: str, origin: SessionSource):
        self.session_id = session_id
        self.session_key = session_key
        self.origin = origin


class Store:
    """Small current-SessionStore shape with a real lock and entry snapshot."""

    def __init__(self, entries: list[Entry]):
        self._lock = threading.RLock()
        self._entries = {entry.session_id: entry for entry in entries}
        self.loaded = False

    def _ensure_loaded_locked(self) -> None:
        self.loaded = True


class PropertyAdapter:
    """The installed ABI: is_connected is a boolean property, not a method."""

    def __init__(self, connectivity: Any = True):
        self.connectivity = connectivity
        self.raise_connectivity = False
        self.events: list[MessageEvent] = []

    @property
    def is_connected(self) -> Any:
        if self.raise_connectivity:
            raise RuntimeError("connectivity read failed")
        return self.connectivity

    async def handle_message(self, event: MessageEvent) -> bool:
        self.events.append(event)
        return True


class MethodOnlyAdapter:
    """A legacy callable-only shape that must be rejected by the property ABI."""

    def __init__(self):
        self.events: list[MessageEvent] = []

    def is_connected(self) -> bool:
        return True

    async def handle_message(self, event: MessageEvent) -> bool:
        self.events.append(event)
        return True


class Runner:
    def __init__(self, store: Store, adapter: Any = None):
        self.session_store = store
        self._adapters: dict[tuple[str, str], Any] = {}
        if adapter is not None:
            self._adapters[(Platform.DISCORD.value, PROFILE)] = adapter

    def _authorization_adapter(self, platform: Any, profile: str | None) -> Any:
        value = platform.value if isinstance(platform, Platform) else str(platform)
        return self._adapters.get((value, profile or PROFILE))

    def _session_key_for_source(self, source: SessionSource) -> str:
        return session_key_for_source(source)


class HostRunner:
    """A runner with no transport adapter at all: host-internal delivery."""

    def __init__(self, store: Store):
        self.session_store = store
        self.host_events: list[MessageEvent] = []

    def _authorization_adapter(self, platform: Any, profile: str | None) -> Any:
        return None

    def _session_key_for_source(self, source: SessionSource) -> str:
        return session_key_for_source(source)

    async def _handle_message(self, event: MessageEvent) -> None:
        self.host_events.append(event)


def session_key(chat_id: str) -> str:
    return session_key_for_source(make_source(chat_id))


KEY_CREATOR = session_key(CREATOR_CHAT)
KEY_WRONG = session_key(WRONG_SINK_CHAT)

DEFAULT_ENTRIES = [
    Entry("session-creator", KEY_CREATOR, make_source(CREATOR_CHAT, thread_id=CREATOR_CHAT)),
    Entry("session-wrong", KEY_WRONG, make_source(WRONG_SINK_CHAT, thread_id=WRONG_SINK_CHAT)),
    Entry("session-channel", session_key("synthetic-channel"), make_source("synthetic-channel", chat_type="channel")),
]


def provenance(
    *,
    profile: Any = PROFILE,
    source: Any = "discord",
    **selectors: Any,
) -> str:
    payload: dict[str, Any] = {"profile": profile, "source": source}
    payload.update(selectors)
    return (
        "[creator-session-provenance/v1]"
        + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        + "[/creator-session-provenance/v1]"
    )


def fresh(entries: list[Entry] | None = None, adapter: Any = None) -> Runner:
    return Runner(
        Store(list(DEFAULT_ENTRIES if entries is None else entries)),
        PropertyAdapter() if adapter is None else adapter,
    )


# Board rows: two-row family layout. The family ROOT (classification unit) is
# the CHILDLESS SINK; the parentless anchor row carries the provenance body.
PROVENANCE_BODY = provenance(thread_id=CREATOR_CHAT)
BOARD_LINKS = (("root-fixture", "child-fixture"),)


def family_rows(
    *,
    root_status: str = "ready",
    sink_status: str = "triage",
    root_body: str = PROVENANCE_BODY,
    sink_body: str = "",
) -> list[dict[str, Any]]:
    return [
        {"id": "root-fixture", "title": "root", "status": root_status, "assignee": "builder", "body": root_body},
        {"id": "child-fixture", "title": "child", "status": sink_status, "assignee": None, "body": sink_body},
    ]


LIVE_ROWS = family_rows(root_status="running", sink_status="running")
QUIET_ROWS = family_rows(root_status="ready", sink_status="triage")
QUIET_ROWS_NO_PROVENANCE = family_rows(root_status="ready", sink_status="triage", root_body="")
QUIET_ROWS_MALFORMED = family_rows(
    root_status="ready",
    sink_status="triage",
    root_body="[creator-session-provenance/v1]{bad}[/creator-session-provenance/v1]",
)


def classify_current(rows: list[dict[str, Any]]):
    return core_mod.classify_family(rows, root_task_id="child-fixture", links=BOARD_LINKS)


def make_trigger(target: Any, key: str = "synthetic-receipt") -> Any:
    snapshot = FamilySnapshot_quiet()
    return mod.Trigger(
        review_key=key,
        board="board-synthetic",
        root_task_id="child-fixture",
        root_title="child",
        target=target,
        outcome=snapshot.state,
        fingerprint=snapshot.fingerprint,
        episode=1,
        attempt=1,
        snapshot=snapshot,
    )


def FamilySnapshot_quiet():
    return core_mod.FamilySnapshot(
        state=core_mod.STATE_ACTION_REQUIRED,
        fingerprint="snapshot-synthetic",
        counts={"triage": 1},
        blocked_ids=(),
        live_task_ids=(),
        tasks=(),
    )


def make_live_snapshot():
    return core_mod.FamilySnapshot(
        state=core_mod.STATE_ACTIVE,
        fingerprint="live-synthetic",
        counts={"running": 2},
        blocked_ids=(),
        live_task_ids=(),
        tasks=(),
    )


def seed_pending_and_reopen(tmp: str, target: Any) -> tuple[Any, Any]:
    """Persist a real pending receipt through the public StateStore API, close,
    reopen (restart equivalent), and return the reopened store plus trigger."""
    dbpath = Path(tmp) / "state.sqlite3"
    state = mod.StateStore(str(dbpath))
    live = core_mod.classify_family(
        LIVE_ROWS, root_task_id="child-fixture", links=BOARD_LINKS
    )
    state.observe("board-synthetic", "child-fixture", "child", target, live)
    quiet = core_mod.classify_family(
        QUIET_ROWS, root_task_id="child-fixture", links=BOARD_LINKS
    )
    trigger = None
    for _ in range(3):
        trigger = state.observe("board-synthetic", "child-fixture", "child", target, quiet)
        if trigger is not None:
            break
    assert trigger is not None, "quiescent debounce did not emit trigger"
    statuses = [status for status, _ in state.unfinished()]
    assert statuses == ["pending"], statuses
    state.close()
    reopened = mod.StateStore(str(dbpath))
    reopened_triggers = [t for _s, t in reopened.unfinished()]
    assert len(reopened_triggers) == 1
    return reopened, reopened_triggers[0]


class RecordingResult(unittest.TextTestResult):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.outcomes: list[tuple[str, str]] = []

    def _record(self, test: unittest.TestCase, status: str) -> None:
        self.outcomes.append((test.id(), status))

    def addSuccess(self, test: unittest.TestCase) -> None:
        super().addSuccess(test)
        self._record(test, "ok")

    def addFailure(self, test: unittest.TestCase, err: Any) -> None:
        super().addFailure(test, err)
        self._record(test, "failure")

    def addError(self, test: unittest.TestCase, err: Any) -> None:
        super().addError(test, err)
        self._record(test, "error")

    def addSkip(self, test: unittest.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self._record(test, "skip")

    def addExpectedFailure(self, test: unittest.TestCase, err: Any) -> None:
        super().addExpectedFailure(test, err)
        self._record(test, "expected_failure")

    def addUnexpectedSuccess(self, test: unittest.TestCase) -> None:
        super().addUnexpectedSuccess(test)
        self._record(test, "unexpected_success")


# ---- acceptance cases -----------------------------------------------------------
class ProvenanceParsing(unittest.TestCase):
    """The strict provenance parser is the sole origin authority."""

    def test_missing_block_holds(self):
        self.assertEqual(
            mod.parse_creator_provenance("body without any envelope").reason_code,
            "HOLD_PROVENANCE_MISSING",
        )

    def test_malformed_json_holds(self):
        text = "[creator-session-provenance/v1]{\"profile\":[/creator-session-provenance/v1]"
        decision = mod.parse_creator_provenance(text)
        self.assertEqual(decision.reason_code, "HOLD_PROVENANCE_MALFORMED")

    def test_multiple_blocks_hold(self):
        text = provenance(session_key=KEY_CREATOR) + provenance(session_key=KEY_WRONG)
        self.assertEqual(mod.parse_creator_provenance(text).reason_code, "HOLD_PROVENANCE_MULTIPLE")

    def test_duplicate_json_key_holds(self):
        text = (
            "[creator-session-provenance/v1]"
            "{\"profile\":\"default\",\"profile\":\"default\",\"source\":\"discord\"}"
            "[/creator-session-provenance/v1]"
        )
        self.assertEqual(mod.parse_creator_provenance(text).reason_code, "HOLD_PROVENANCE_MALFORMED")

    def test_unknown_selector_key_is_malformed(self):
        text = (
            "[creator-session-provenance/v1]"
            + json.dumps({"profile": PROFILE, "source": "discord", "bogus": "x"}, separators=(",", ":"))
            + "[/creator-session-provenance/v1]"
        )
        self.assertEqual(mod.parse_creator_provenance(text).reason_code, "HOLD_PROVENANCE_MALFORMED")

    def test_empty_selector_value_is_malformed(self):
        text = (
            "[creator-session-provenance/v1]"
            + json.dumps({"profile": PROFILE, "source": "discord", "session_key": ""}, separators=(",", ":"))
            + "[/creator-session-provenance/v1]"
        )
        self.assertEqual(mod.parse_creator_provenance(text).reason_code, "HOLD_PROVENANCE_MALFORMED")

    def test_missing_required_key_is_malformed(self):
        text = (
            "[creator-session-provenance/v1]"
            + json.dumps({"profile": PROFILE}, separators=(",", ":"))
            + "[/creator-session-provenance/v1]"
        )
        self.assertEqual(mod.parse_creator_provenance(text).reason_code, "HOLD_PROVENANCE_MALFORMED")

    def test_non_dict_payload_is_malformed(self):
        text = (
            "[creator-session-provenance/v1]"
            + json.dumps([PROFILE, "discord"], separators=(",", ":"))
            + "[/creator-session-provenance/v1]"
        )
        self.assertEqual(mod.parse_creator_provenance(text).reason_code, "HOLD_PROVENANCE_MALFORMED")

    def test_authority_fields_are_preserved_in_parsed_claim(self):
        claim = mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        self.assertIsInstance(claim, mod.CreatorClaim)
        self.assertEqual(claim.profile, PROFILE)
        self.assertEqual(claim.source, "discord")
        self.assertEqual(claim.session_key, KEY_CREATOR)
        self.assertTrue(claim.provenance_sha256)

    def test_provenance_sha256_binds_canonical_claim_bytes(self):
        claim_a = mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        claim_b = mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        claim_c = mod.parse_creator_provenance(provenance(session_key=KEY_WRONG))
        self.assertEqual(claim_a.provenance_sha256, claim_b.provenance_sha256)
        self.assertNotEqual(claim_a.provenance_sha256, claim_c.provenance_sha256)


class LiteralAuthorityScope(unittest.TestCase):
    """Authority literals are exact: no case folding, no whitespace trimming."""

    def _decision(self, **kwargs: Any) -> Any:
        runner = fresh()
        return mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR, **kwargs))
        )

    def test_uppercase_profile_is_rejected(self):
        self.assertEqual(self._decision(profile="DEFAULT").reason_code, "SKIP_UNSUPPORTED_SOURCE")

    def test_mixed_case_profile_is_rejected(self):
        self.assertEqual(self._decision(profile="Default").reason_code, "SKIP_UNSUPPORTED_SOURCE")

    def test_profile_whitespace_is_rejected_without_normalization(self):
        self.assertEqual(self._decision(profile=" default").reason_code, "SKIP_UNSUPPORTED_SOURCE")

    def test_uppercase_source_is_rejected_without_normalization(self):
        self.assertEqual(self._decision(source="DISCORD").reason_code, "SKIP_UNSUPPORTED_SOURCE")

    def test_source_whitespace_is_rejected_without_normalization(self):
        self.assertEqual(self._decision(source=" discord").reason_code, "SKIP_UNSUPPORTED_SOURCE")

    def test_unregistered_source_is_rejected(self):
        self.assertEqual(self._decision(source="slack").reason_code, "SKIP_UNSUPPORTED_SOURCE")


class ExactSessionRouting(unittest.TestCase):
    """Exact selector matching over the current session store only."""

    def test_exact_session_key_resolves_to_one_stored_record(self):
        decision = mod.resolve_creator_target(
            fresh(), mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertEqual(decision.target.session_id, "session-creator")
        self.assertEqual(decision.target.session_key, KEY_CREATOR)

    def test_ready_decision_retains_exact_adapter_object(self):
        adapter = PropertyAdapter()
        runner = fresh(adapter=adapter)
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertIs(decision.adapter, adapter)
        self.assertEqual(decision.delivery, "adapter")

    def test_thread_as_chat_shape_resolves_uniquely(self):
        entry = Entry(
            "session-thread-chat",
            session_key("synthetic-thread-chat"),
            make_source("synthetic-thread-chat"),
        )
        decision = mod.resolve_creator_target(
            fresh(entries=[entry]),
            mod.parse_creator_provenance(provenance(thread_id="synthetic-thread-chat")),
        )
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertEqual(decision.target.session_id, "session-thread-chat")

    def test_explicit_thread_id_resolves_uniquely(self):
        entry = Entry(
            "session-explicit",
            session_key("synthetic-explicit"),
            make_source("synthetic-explicit", thread_id="synthetic-explicit-thread"),
        )
        decision = mod.resolve_creator_target(
            fresh(entries=[entry]),
            mod.parse_creator_provenance(provenance(thread_id="synthetic-explicit-thread")),
        )
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertEqual(decision.target.session_key, entry.session_key)

    def test_session_id_selector_resolves_uniquely(self):
        decision = mod.resolve_creator_target(
            fresh(), mod.parse_creator_provenance(provenance(session_id="session-creator"))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertEqual(decision.target.session_id, "session-creator")

    def test_two_creator_sessions_remain_isolated(self):
        first = Entry("session-one", session_key("synthetic-one"), make_source("synthetic-one", thread_id="thread-one"))
        second = Entry("session-two", session_key("synthetic-two"), make_source("synthetic-two", thread_id="thread-two"))
        runner = fresh(entries=[first, second])
        one = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=first.session_key))
        )
        two = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=second.session_key))
        )
        self.assertTrue(one.ready, one.reason_code)
        self.assertTrue(two.ready, two.reason_code)
        self.assertEqual(one.target.session_id, first.session_id)
        self.assertEqual(two.target.session_id, second.session_id)
        self.assertNotEqual(one.target.session_key, two.target.session_key)
        self.assertIs(one.adapter, two.adapter)


class UnresolvableAndAmbiguous(unittest.TestCase):
    """Contradictory, unknown, and ambiguous claims fail closed typed."""

    def test_wrong_sink_selector_conflict_is_unresolvable(self):
        decision = mod.resolve_creator_target(
            fresh(),
            mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR, chat_id=WRONG_SINK_CHAT)),
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_creator_thread_does_not_select_wrong_sink(self):
        entries = [
            Entry("creator-thread", session_key("synthetic-creator-thread"), make_source("synthetic-creator-thread")),
            Entry("wrong-thread", session_key("synthetic-wrong-thread"), make_source("synthetic-wrong-thread")),
        ]
        decision = mod.resolve_creator_target(
            fresh(entries=entries),
            mod.parse_creator_provenance(provenance(thread_id="synthetic-creator-thread")),
        )
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertEqual(decision.target.session_id, "creator-thread")

    def test_contradictory_session_id_and_key_holds(self):
        decision = mod.resolve_creator_target(
            fresh(),
            mod.parse_creator_provenance(provenance(session_id="session-creator", session_key=KEY_WRONG)),
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_session_key_whitespace_does_not_match_exact_record(self):
        decision = mod.resolve_creator_target(
            fresh(),
            mod.parse_creator_provenance(provenance(session_key=" " + KEY_CREATOR)),
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_empty_selectors_are_unresolvable(self):
        decision = mod.resolve_creator_target(fresh(), mod.parse_creator_provenance(provenance()))
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_missing_store_match_is_unresolvable(self):
        decision = mod.resolve_creator_target(
            fresh(),
            mod.parse_creator_provenance(provenance(session_key=session_key("synthetic-missing"))),
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_true_thread_selector_ambiguity_holds(self):
        entries = [
            Entry("ambiguous-one", session_key("synthetic-ambiguous-one"), make_source(SHARED_THREAD)),
            Entry("ambiguous-two", session_key("synthetic-ambiguous-two"), make_source(SHARED_THREAD)),
        ]
        adapter = PropertyAdapter()
        decision = mod.resolve_creator_target(
            fresh(entries=entries, adapter=adapter),
            mod.parse_creator_provenance(provenance(thread_id=SHARED_THREAD)),
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_AMBIGUOUS")
        self.assertIsNone(decision.target)
        self.assertEqual(adapter.events, [])

    def test_unusable_store_fails_closed(self):
        class NoLockStore:
            _lock = None

        decision = mod.resolve_creator_target(
            Runner(NoLockStore(), PropertyAdapter()),  # type: ignore[arg-type]
            mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR)),
        )
        self.assertFalse(decision.ready)
        self.assertIn(
            decision.reason_code,
            {"HOLD_SESSION_UNRESOLVABLE", "HOLD_DESTINATION_UNUSABLE"},
        )


class AmbiguityInjectsNothingOnTick(unittest.TestCase):
    """An ambiguous claim produces zero dispatch at the runtime boundary."""

    def test_ambiguous_selector_tick_has_zero_injection(self):
        entries = [
            Entry("ambiguous-one", session_key("synthetic-ambiguous-one"), make_source(SHARED_THREAD)),
            Entry("ambiguous-two", session_key("synthetic-ambiguous-two"), make_source(SHARED_THREAD)),
        ]
        adapter = PropertyAdapter()
        runner = fresh(entries=entries, adapter=adapter)
        with tempfile.TemporaryDirectory() as td:
            state = mod.StateStore(str(Path(td) / "state.sqlite3"))
            runtime = mod.OriginReviewRuntime(runner=runner, state_store=state, poll_seconds=1)
            runtime.board_paths = lambda: {"board-synthetic": Path("/not-opened.sqlite3")}
            original = mod.read_board_graph
            mod.read_board_graph = lambda _path: (family_rows(root_body=provenance(thread_id=SHARED_THREAD)), BOARD_LINKS)
            try:
                report = asyncio.run(runtime.tick())
            finally:
                mod.read_board_graph = original
                state.close()
        self.assertEqual(adapter.events, [])
        self.assertIn("HOLD_SESSION_AMBIGUOUS", json.dumps(report["errors"]))


class ProfileCanonicalization(unittest.TestCase):
    """Omitted, empty, and explicit-default stored profiles are one canonical
    default-namespace identity; any other stored name fails closed."""

    def _entry(self, profile: Any, *, chat_id: str = CREATOR_CHAT) -> Entry:
        source = make_source(chat_id, thread_id=chat_id, profile=profile)
        return Entry(f"session-{chat_id}-{profile or 'none'}", session_key_for_source(source), source)

    def _resolve(self, entries: list[Entry]):
        runner = fresh(entries=entries)
        decision = mod.resolve_creator_target(
            runner,
            mod.parse_creator_provenance(provenance(session_key=entries[0].session_key)),
        )
        return runner, decision

    def test_all_default_variants_bind_and_match(self):
        for profile in (None, "", "default"):
            with self.subTest(profile=profile):
                entry = self._entry(profile)
                runner, decision = self._resolve([entry])
                self.assertTrue(decision.ready, decision.reason_code)
                # The exact stored representation is preserved on the target.
                self.assertEqual(decision.target.profile, profile)
                self.assertTrue(mod._binding_matches(runner, decision.target))
                self.assertEqual(decision.target.session_key, entry.session_key)

    def test_non_default_stored_profile_fails_closed(self):
        entry = self._entry("work")
        _runner, decision = self._resolve([entry])
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_default_profile_claim_cannot_select_non_default_record(self):
        entry = self._entry("work")
        decision = mod.resolve_creator_target(
            fresh(entries=[entry]),
            mod.parse_creator_provenance(provenance(session_key=entry.session_key)),
        )
        self.assertFalse(decision.ready)

    def test_canonical_key_mismatch_fails_closed(self):
        entry = self._entry(None)
        decision = mod.resolve_creator_target(
            fresh(entries=[entry]),
            mod.parse_creator_provenance(provenance(session_key=KEY_WRONG)),
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_literal_authority_variants_still_rejected(self):
        runner = fresh()
        for kwargs in ({"profile": "DEFAULT"}, {"profile": " default"}, {"source": "DISCORD"}):
            decision = mod.resolve_creator_target(
                runner,
                mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR, **kwargs)),
            )
            self.assertFalse(decision.ready)
            self.assertEqual(decision.reason_code, "SKIP_UNSUPPORTED_SOURCE")


class AdapterPropertyABI(unittest.TestCase):
    """The installed adapter readiness contract: boolean property + callable handle."""

    def _resolve_rejected(self, adapter: Any) -> Any:
        runner = fresh(adapter=adapter)
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_DESTINATION_UNUSABLE")
        self.assertIsNone(decision.target)
        return decision

    def test_property_shaped_healthy_adapter_succeeds(self):
        adapter = PropertyAdapter(True)
        decision = mod.resolve_creator_target(
            fresh(adapter=adapter),
            mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR)),
        )
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertIs(decision.adapter, adapter)

    def test_callable_method_only_shape_is_rejected(self):
        self._resolve_rejected(MethodOnlyAdapter())

    def test_false_property_is_rejected(self):
        self._resolve_rejected(PropertyAdapter(False))

    def test_non_boolean_property_is_rejected(self):
        self._resolve_rejected(PropertyAdapter(1))

    def test_raising_property_is_rejected(self):
        adapter = PropertyAdapter(True)
        adapter.raise_connectivity = True
        self._resolve_rejected(adapter)

    def test_non_callable_handle_message_is_rejected(self):
        adapter = PropertyAdapter(True)
        adapter.handle_message = "not-callable"
        self._resolve_rejected(adapter)


class HostDeliveryFallback(unittest.TestCase):
    """A stored discord session with NO provisioned adapter object is delivered
    through the host-internal channel; present-but-unhealthy stays fail-closed."""

    def _host_runner(self) -> HostRunner:
        return HostRunner(Store(list(DEFAULT_ENTRIES)))

    def test_absent_adapter_resolves_ready_with_host_channel(self):
        runner = self._host_runner()
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertEqual(decision.delivery, "host")
        self.assertIsNone(decision.adapter)

    def test_present_but_disconnected_adapter_still_fails_closed(self):
        runner = fresh(adapter=PropertyAdapter(False))
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_DESTINATION_UNUSABLE")

    def test_host_injection_reaches_host_entrypoint_with_exact_source(self):
        runner = self._host_runner()
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        result = asyncio.run(
            mod.inject_trigger(
                runner,
                make_trigger(decision.target, key="synthetic-host-receipt"),
                parent_channel_id="unused-parent",
                confirmation_timeout=0,
                resolved_adapter=None,
                delivery="host",
            )
        )
        self.assertFalse(result)  # confirmation_timeout=0 never waits for a marker
        self.assertEqual(len(runner.host_events), 1)
        event = runner.host_events[0]
        self.assertTrue(event.internal)
        self.assertEqual(event.source.chat_id, CREATOR_CHAT)
        self.assertEqual(session_key_for_source(event.source), KEY_CREATOR)
        self.assertIn("[kanban-origin-review:synthetic-host-receipt]", event.text)

    def test_host_delivery_rejects_a_resolved_adapter(self):
        runner = self._host_runner()
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        )
        with self.assertRaises(RuntimeError):
            asyncio.run(
                mod.inject_trigger(
                    runner,
                    make_trigger(decision.target, key="synthetic-host-guard"),
                    parent_channel_id="unused-parent",
                    confirmation_timeout=0,
                    resolved_adapter=PropertyAdapter(),
                    delivery="host",
                )
            )
        self.assertEqual(runner.host_events, [])

    def test_unknown_delivery_channel_is_rejected(self):
        runner = self._host_runner()
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        )
        with self.assertRaises(RuntimeError):
            asyncio.run(
                mod.inject_trigger(
                    runner,
                    make_trigger(decision.target, key="synthetic-channel-guard"),
                    parent_channel_id="unused-parent",
                    confirmation_timeout=0,
                    resolved_adapter=None,
                    delivery="carrier-pigeon",
                )
            )
        self.assertEqual(runner.host_events, [])


class AdapterDispatchAndPreDispatchGuards(unittest.TestCase):
    """Injection carries the exact stored identity; every mutated capability
    re-proven at the dispatch boundary refuses with zero events."""

    def test_injection_receives_stored_source_identity_fields(self):
        source = make_source(
            "synthetic-inject-chat",
            thread_id="synthetic-inject-thread",
            parent_chat_id="synthetic-inject-parent",
            chat_name="Inject fixture",
            user_id="synthetic-inject-user",
            user_name="Inject User",
        )
        entry = Entry("session-inject", session_key_for_source(source), source)
        adapter = PropertyAdapter()
        runner = fresh(entries=[entry], adapter=adapter)
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=entry.session_key))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        asyncio.run(
            mod.inject_trigger(
                runner,
                make_trigger(decision.target, key="synthetic-source-receipt"),
                parent_channel_id="unused-parent",
                confirmation_timeout=0,
                resolved_adapter=decision.adapter,
            )
        )
        self.assertEqual(len(adapter.events), 1)
        event = adapter.events[0]
        self.assertTrue(event.internal)
        self.assertEqual(event.source.chat_id, source.chat_id)
        self.assertEqual(event.source.thread_id, source.thread_id)
        self.assertEqual(event.source.parent_chat_id, source.parent_chat_id)
        self.assertEqual(event.source.user_id, source.user_id)
        self.assertEqual(event.source.user_name, source.user_name)
        self.assertEqual(event.source.profile, source.profile)
        self.assertEqual(session_key_for_source(event.source), entry.session_key)
        self.assertIn("[kanban-origin-review:synthetic-source-receipt]", event.text)

    def _assert_rejected_before_dispatch(self, mutate: Any) -> None:
        adapter = PropertyAdapter(True)
        runner = fresh(adapter=adapter)
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        mutate(runner, adapter)
        with self.assertRaises(RuntimeError):
            asyncio.run(
                mod.inject_trigger(
                    runner,
                    make_trigger(decision.target, key="synthetic-race-receipt"),
                    parent_channel_id="unused-parent",
                    confirmation_timeout=0,
                    resolved_adapter=decision.adapter,
                )
            )
        self.assertEqual(adapter.events, [])

    def test_disconnect_after_resolution_holds_before_handle(self):
        self._assert_rejected_before_dispatch(
            lambda _runner, adapter: setattr(adapter, "connectivity", False)
        )

    def test_replacement_after_resolution_holds_on_identity(self):
        def swap(runner: Runner, _adapter: PropertyAdapter) -> None:
            runner._adapters[(Platform.DISCORD.value, PROFILE)] = PropertyAdapter(True)

        self._assert_rejected_before_dispatch(swap)

    def test_missing_adapter_after_resolution_holds_before_handle(self):
        self._assert_rejected_before_dispatch(lambda runner, _adapter: runner._adapters.clear())

    def test_non_callable_handle_after_resolution_holds_before_handle(self):
        self._assert_rejected_before_dispatch(
            lambda _runner, adapter: setattr(adapter, "handle_message", "nope")
        )

    def test_false_property_after_resolution_holds_before_handle(self):
        self._assert_rejected_before_dispatch(
            lambda _runner, adapter: setattr(adapter, "connectivity", False)
        )

    def test_raising_property_after_resolution_holds_before_handle(self):
        self._assert_rejected_before_dispatch(
            lambda _runner, adapter: setattr(adapter, "raise_connectivity", True)
        )


class ParentExactPreservation(unittest.TestCase):
    """Stored parent_chat_id travels byte-exactly (or as absence) through
    WakeTarget, the emitted MessageEvent.source, and receipt round trips;
    a caller/default parent never fills absence."""

    def _setup(self, parent_chat_id: Any):
        source = make_source(CREATOR_CHAT, thread_id=CREATOR_CHAT, parent_chat_id=parent_chat_id)
        entry = Entry("session-parent", session_key_for_source(source), source)
        adapter = PropertyAdapter()
        runner = fresh(entries=[entry], adapter=adapter)
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=entry.session_key))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        return runner, adapter, decision

    def _inject(self, runner, decision, key: str):
        return asyncio.run(
            mod.inject_trigger(
                runner,
                make_trigger(decision.target, key=key),
                parent_channel_id="caller-parent-must-not-fill",
                confirmation_timeout=0,
                resolved_adapter=decision.adapter,
            )
        )

    def test_nonempty_parent_stays_byte_exact(self):
        runner, adapter, decision = self._setup("stored-parent-123")
        self.assertEqual(decision.target.parent_chat_id, "stored-parent-123")
        self._inject(runner, decision, "parent-stored")
        self.assertEqual(len(adapter.events), 1)
        self.assertEqual(adapter.events[0].source.parent_chat_id, "stored-parent-123")

    def test_empty_string_parent_preserved_byte_exact(self):
        runner, adapter, decision = self._setup("")
        self.assertEqual(decision.target.parent_chat_id, "")
        self._inject(runner, decision, "parent-empty")
        self.assertEqual(len(adapter.events), 1)
        self.assertEqual(adapter.events[0].source.parent_chat_id, "")

    def test_whitespace_parent_preserved_byte_exact(self):
        runner, adapter, decision = self._setup("  parent-987  ")
        self.assertEqual(decision.target.parent_chat_id, "  parent-987  ")
        self._inject(runner, decision, "parent-whitespace")
        self.assertEqual(len(adapter.events), 1)
        self.assertEqual(adapter.events[0].source.parent_chat_id, "  parent-987  ")

    def test_none_parent_stays_none_and_caller_never_fills(self):
        runner, adapter, decision = self._setup(None)
        self.assertIsNone(decision.target.parent_chat_id)
        self._inject(runner, decision, "parent-none")
        self.assertEqual(len(adapter.events), 1)
        self.assertIsNone(adapter.events[0].source.parent_chat_id)
        self.assertNotEqual(adapter.events[0].source.parent_chat_id, "caller-parent-must-not-fill")

    def test_nonstring_parent_fails_closed(self):
        source = make_source(CREATOR_CHAT, thread_id=CREATOR_CHAT)
        object.__setattr__(source, "parent_chat_id", 12345)
        entry = Entry("session-parent-invalid", session_key_for_source(source), source)
        decision = mod.resolve_creator_target(
            fresh(entries=[entry]),
            mod.parse_creator_provenance(provenance(session_key=entry.session_key)),
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_parent_round_trips_through_receipt(self):
        for parent in (None, "", "  parent-987  "):
            with self.subTest(parent=parent):
                source = make_source(CREATOR_CHAT, thread_id=CREATOR_CHAT, parent_chat_id=parent)
                entry = Entry("session-parent-roundtrip", session_key_for_source(source), source)
                decision = mod.resolve_creator_target(
                    fresh(entries=[entry]),
                    mod.parse_creator_provenance(provenance(session_key=entry.session_key)),
                )
                self.assertTrue(decision.ready, decision.reason_code)
                with tempfile.TemporaryDirectory() as tmp:
                    state, trigger = seed_pending_and_reopen(tmp, decision.target)
                    try:
                        self.assertEqual(trigger.target.parent_chat_id, parent)
                    finally:
                        state.close()


class StoredProfileRoundTrip(unittest.TestCase):
    """The exact stored profile representation survives the receipt round trip."""

    def _entry(self, profile):
        source = make_source(CREATOR_CHAT, thread_id=CREATOR_CHAT, profile=profile)
        return Entry("session-profile-" + repr(profile), session_key_for_source(source), source)

    def test_profile_repr_round_trips_through_receipt(self):
        for profile in (None, "", "default"):
            with self.subTest(profile=profile):
                entry = self._entry(profile)
                decision = mod.resolve_creator_target(
                    fresh(entries=[entry]),
                    mod.parse_creator_provenance(provenance(session_key=entry.session_key)),
                )
                self.assertTrue(decision.ready, decision.reason_code)
                with tempfile.TemporaryDirectory() as tmp:
                    state, trigger = seed_pending_and_reopen(tmp, decision.target)
                    try:
                        self.assertEqual(trigger.target.profile, profile)
                    finally:
                        state.close()


class BoardEventVocabulary(unittest.TestCase):
    """read_board_graph retains the status-affecting vocabulary, drops pure
    audit noise, surfaces unknown kinds, and classify_family derives the
    eligible postures from native evidence only."""

    def _build_board(self, dbpath: Path, rows, events, links) -> None:
        conn = sqlite3.connect(dbpath)
        conn.executescript(
            """
            CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT, body TEXT, status TEXT,
                                assignee TEXT, session_id TEXT, created_at INTEGER,
                                block_kind TEXT, current_run_id INTEGER,
                                last_heartbeat_at INTEGER, result TEXT);
            CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT, kind TEXT,
                                      payload TEXT, run_id INTEGER, created_at INTEGER);
            CREATE TABLE task_links (parent_id TEXT, child_id TEXT);
            """
        )
        for row in rows:
            conn.execute("INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)", row)
        for event in events:
            conn.execute(
                "INSERT INTO task_events (task_id,kind,payload,run_id,created_at) VALUES (?,?,?,?,?)",
                event,
            )
        for link in links:
            conn.execute("INSERT INTO task_links VALUES (?,?)", link)
        conn.commit()
        conn.close()

    def test_noise_kinds_stay_out_of_latest_events(self):
        with tempfile.TemporaryDirectory() as td:
            dbpath = Path(td) / "board.sqlite3"
            self._build_board(
                dbpath,
                [
                    ("a", "A", "", "ready", "builder", None, 10, None, None, None, None),
                    ("b", "B", "", "todo", "builder", None, 11, None, None, None, None),
                ],
                [
                    ("a", "reasoning_effort_set", "{}", 1, 10),
                    ("a", "heartbeat", "{}", 1, 11),
                    ("a", "blocked", "{\"kind\":\"needs_input\"}", 2, 20),
                ],
                [("a", "b")],
            )
            tasks, links = mod.read_board_graph(dbpath)
            row_a = [t for t in tasks if t["id"] == "a"][0]
            self.assertNotIn("reasoning_effort_set", row_a["latest_events"])
            self.assertNotIn("heartbeat", row_a["latest_events"])
            self.assertIn("blocked", row_a["latest_events"])
            self.assertEqual(links, [("a", "b")])

    def test_external_blocked_requires_verified_current_gate(self):
        with tempfile.TemporaryDirectory() as td:
            dbpath = Path(td) / "board.sqlite3"
            self._build_board(
                dbpath,
                [
                    ("a", "A", "", "done", "builder", None, 10, None, None, None, None),
                    ("b", "B", "", "blocked", "builder", None, 11, "needs_input", None, None, None),
                ],
                [("b", "blocked", "{\"kind\":\"needs_input\"}", 3, 20)],
                [("a", "b")],
            )
            tasks, links = mod.read_board_graph(dbpath)
            snapshot = core_mod.classify_family(tasks, root_task_id="b", links=links)
            self.assertEqual(snapshot.state, core_mod.STATE_EXTERNAL_BLOCKED)
            self.assertIsNotNone(snapshot.gate_authority)
            self.assertEqual(snapshot.gate_authority.kind, core_mod.STATE_EXTERNAL_BLOCKED)
            self.assertEqual(snapshot.gate_authority.event_kind, "blocked")

    def test_newer_status_affecting_event_breaks_gate_currentness(self):
        with tempfile.TemporaryDirectory() as td:
            dbpath = Path(td) / "board.sqlite3"
            self._build_board(
                dbpath,
                [
                    ("a", "A", "", "done", "builder", None, 10, None, None, None, None),
                    ("b", "B", "", "blocked", "builder", None, 11, "needs_input", None, None, None),
                ],
                [
                    ("b", "blocked", "{\"kind\":\"needs_input\"}", 3, 20),
                    ("b", "claimed", "{}", 4, 30),
                ],
                [("a", "b")],
            )
            tasks, links = mod.read_board_graph(dbpath)
            snapshot = core_mod.classify_family(tasks, root_task_id="b", links=links)
            self.assertEqual(snapshot.state, core_mod.STATE_ACTIVE)
            self.assertIsNone(snapshot.gate_authority)

    def test_unknown_event_kind_fails_closed(self):
        with tempfile.TemporaryDirectory() as td:
            dbpath = Path(td) / "board.sqlite3"
            self._build_board(
                dbpath,
                [
                    ("a", "A", "", "done", "builder", None, 10, None, None, None, None),
                    ("b", "B", "", "blocked", "builder", None, 11, "needs_input", None, None, None),
                ],
                [
                    ("b", "blocked", "{\"kind\":\"needs_input\"}", 3, 20),
                    ("b", "brand_new_mystery_kind", "{}", 5, 31),
                ],
                [("a", "b")],
            )
            tasks, links = mod.read_board_graph(dbpath)
            row_b = [t for t in tasks if t["id"] == "b"][0]
            # The unknown kind is surfaced, never silently dropped...
            self.assertIn("brand_new_mystery_kind", row_b["latest_events"])
            # ...and the posture fails closed to active.
            snapshot = core_mod.classify_family(tasks, root_task_id="b", links=links)
            self.assertEqual(snapshot.state, core_mod.STATE_ACTIVE)

    def test_root_completed_requires_native_completion_evidence_on_every_member(self):
        with tempfile.TemporaryDirectory() as td:
            dbpath = Path(td) / "board.sqlite3"
            self._build_board(
                dbpath,
                [
                    ("a", "A", "", "done", "builder", None, 10, None, None, None, "summary-a"),
                    ("b", "B", "", "done", "builder", None, 11, None, None, None, None),
                ],
                [
                    ("a", "completed", "{\"summary\":\"summary-a\"}", 1, 15),
                    ("b", "completed", "{}", 2, 20),
                ],
                [("a", "b")],
            )
            tasks, links = mod.read_board_graph(dbpath)
            snapshot = core_mod.classify_family(tasks, root_task_id="b", links=links)
            self.assertEqual(snapshot.state, core_mod.STATE_ROOT_COMPLETED)
            self.assertEqual(snapshot.gate_authority.kind, core_mod.STATE_ROOT_COMPLETED)
            row_a = [t for t in tasks if t["id"] == "a"][0]
            self.assertEqual(row_a["summary"], "summary-a")

    def test_status_only_done_without_event_stays_active(self):
        with tempfile.TemporaryDirectory() as td:
            dbpath = Path(td) / "board.sqlite3"
            self._build_board(
                dbpath,
                [
                    ("a", "A", "", "done", "builder", None, 10, None, None, None, None),
                    ("b", "B", "", "done", "builder", None, 11, None, None, None, None),
                ],
                [("a", "completed", "{}", 1, 15)],
                [("a", "b")],
            )
            tasks, links = mod.read_board_graph(dbpath)
            snapshot = core_mod.classify_family(tasks, root_task_id="b", links=links)
            self.assertEqual(snapshot.state, core_mod.STATE_ACTIVE)

    def test_heartbeat_lost_requires_prior_activity_and_staleness(self):
        now = int(time.time())
        stale = now - 2 * hermes_cli_db._STALE_HEARTBEAT_GAP_SECONDS
        with tempfile.TemporaryDirectory() as td:
            dbpath = Path(td) / "board.sqlite3"
            self._build_board(
                dbpath,
                [
                    ("a", "A", "", "done", "builder", None, 10, None, None, None, None),
                    ("b", "B", "", "running", "builder", None, 11, None, 77, stale, None),
                ],
                [("b", "claimed", "{}", 3, stale)],
                [("a", "b")],
            )
            tasks, links = mod.read_board_graph(dbpath)
            snapshot = core_mod.classify_family(tasks, root_task_id="b", links=links, now=now)
            self.assertEqual(snapshot.state, core_mod.STATE_HEARTBEAT_LOST)

    def test_recent_heartbeat_stays_active(self):
        now = int(time.time())
        with tempfile.TemporaryDirectory() as td:
            dbpath = Path(td) / "board.sqlite3"
            self._build_board(
                dbpath,
                [
                    ("a", "A", "", "done", "builder", None, 10, None, None, None, None),
                    ("b", "B", "", "running", "builder", None, 11, None, 77, now - 30, None),
                ],
                [("b", "claimed", "{}", 3, now - 30)],
                [("a", "b")],
            )
            tasks, links = mod.read_board_graph(dbpath)
            snapshot = core_mod.classify_family(tasks, root_task_id="b", links=links, now=now)
            self.assertEqual(snapshot.state, core_mod.STATE_ACTIVE)

    def test_never_started_row_never_notifies(self):
        now = int(time.time())
        with tempfile.TemporaryDirectory() as td:
            dbpath = Path(td) / "board.sqlite3"
            self._build_board(
                dbpath,
                [
                    ("a", "A", "", "done", "builder", None, 10, None, None, None, None),
                    ("b", "B", "", "running", "builder", None, 11, None, None, None, None),
                ],
                [],
                [("a", "b")],
            )
            tasks, links = mod.read_board_graph(dbpath)
            snapshot = core_mod.classify_family(tasks, root_task_id="b", links=links, now=now)
            self.assertEqual(snapshot.state, core_mod.STATE_ACTIVE)


class CompletionWakeQualificationAndDedup(unittest.TestCase):
    """Full runtime flow: an eligible posture observed twice dispatches exactly
    one review turn; the same posture never re-dispatches; re-entry after the
    episode closes binds to a strictly NEW review key."""

    def _runtime(self, runner, state, injector=None):
        runtime = mod.OriginReviewRuntime(
            runner=runner, state_store=state, poll_seconds=1, injector=injector
        )
        runtime.board_paths = lambda: {"board-synthetic": Path("/not-opened.sqlite3")}
        return runtime

    def test_two_stable_observations_dispatch_exactly_once(self):
        adapter = PropertyAdapter()
        runner = fresh(adapter=adapter)
        dispatches: list[str] = []

        async def injector(_runner, trigger, **kwargs):
            dispatches.append(trigger.review_key)
            return await mod.inject_trigger(_runner, trigger, confirmation_timeout=0, **kwargs)

        with tempfile.TemporaryDirectory() as td:
            state = mod.StateStore(str(Path(td) / "state.sqlite3"))
            runtime = self._runtime(runner, state, injector=injector)
            original = mod.read_board_graph
            try:
                # Active posture closes any prior episode (non-eligible)...
                mod.read_board_graph = lambda _path: (LIVE_ROWS, BOARD_LINKS)
                live_tick = asyncio.run(runtime.tick())
                # ...then the eligible posture is observed twice: the second
                # stable observation completes the debounce and dispatches.
                mod.read_board_graph = lambda _path: (QUIET_ROWS, BOARD_LINKS)
                arm_tick = asyncio.run(runtime.tick())
                dispatch_tick = asyncio.run(runtime.tick())
            finally:
                mod.read_board_graph = original
                state.close()
        self.assertEqual(live_tick["triggers"], 0)
        self.assertEqual(arm_tick["triggers"], 0)
        self.assertEqual(len(dispatches), 1)
        self.assertEqual(len(adapter.events), 1)
        # confirmation_timeout=0 never waits for a marker: typed uncertain.
        self.assertEqual(dispatch_tick["triggers"], 0)

    def test_same_episode_dedup_never_redispatches(self):
        adapter = PropertyAdapter()
        runner = fresh(adapter=adapter)
        target = mod.resolve_creator_target(
            fresh(), mod.parse_creator_provenance(PROVENANCE_BODY)
        ).target
        with tempfile.TemporaryDirectory() as tmp:
            state = mod.StateStore(str(Path(tmp) / "state.sqlite3"))
            live = classify_current(LIVE_ROWS)
            quiet = classify_current(QUIET_ROWS)
            self.assertIsNone(state.observe("board-synthetic", "child-fixture", "child", target, live))
            self.assertIsNone(state.observe("board-synthetic", "child-fixture", "child", target, quiet))
            trigger = state.observe("board-synthetic", "child-fixture", "child", target, quiet)
            self.assertIsNotNone(trigger)
            self.assertIsNone(state.observe("board-synthetic", "child-fixture", "child", target, quiet))
            self.assertIsNone(state.observe("board-synthetic", "child-fixture", "child", target, quiet))
            self.assertEqual(len([t for _s, t in state.unfinished()]), 1)
            state.close()

    def test_reentry_after_episode_closure_binds_new_review_key(self):
        adapter = PropertyAdapter()
        runner = fresh(adapter=adapter)
        target = mod.resolve_creator_target(
            fresh(), mod.parse_creator_provenance(PROVENANCE_BODY)
        ).target
        with tempfile.TemporaryDirectory() as tmp:
            state = mod.StateStore(str(Path(tmp) / "state.sqlite3"))
            live = classify_current(LIVE_ROWS)
            quiet = classify_current(QUIET_ROWS)
            state.observe("board-synthetic", "child-fixture", "child", target, live)
            state.observe("board-synthetic", "child-fixture", "child", target, quiet)
            first = state.observe("board-synthetic", "child-fixture", "child", target, quiet)
            self.assertIsNotNone(first)
            state.set_status(first, "confirmed")
            # The confirmed episode closes on a non-eligible observation...
            self.assertIsNone(state.observe("board-synthetic", "child-fixture", "child", target, live))
            # ...and the next stable eligible posture binds a NEW review key.
            state.observe("board-synthetic", "child-fixture", "child", target, quiet)
            second = state.observe("board-synthetic", "child-fixture", "child", target, quiet)
            self.assertIsNotNone(second)
            self.assertNotEqual(second.review_key, first.review_key)
            state.close()


class PendingRecoveryMarkerFirst(unittest.TestCase):
    """A persisted pending receipt re-proves the full current root BEFORE any
    marker consultation or dispatch; every refusal stays typed uncertain with
    zero injection; confirmation and dispatch are each exactly-once."""

    def _harness(self, tmp, *, target, body=PROVENANCE_BODY, rows=None, graph_error=None,
                 confirm_marker=False, adapter=None):
        adapter = PropertyAdapter() if adapter is None else adapter
        runner = fresh(adapter=adapter)
        calls: list[str] = []
        timeline: list[str] = []
        state, trigger = seed_pending_and_reopen(tmp, target)

        async def injector(_runner, trig, **kwargs):
            calls.append("dispatch")
            timeline.append("dispatch")
            return await mod.inject_trigger(_runner, trig, confirmation_timeout=0, **kwargs)

        runtime = mod.OriginReviewRuntime(
            runner=runner, state_store=state, poll_seconds=1, injector=injector
        )
        runtime.board_paths = lambda: {"board-synthetic": Path("/not-opened.sqlite3")}
        old_graph = mod.read_board_graph
        old_confirm = mod.trigger_is_confirmed

        def read_graph(_path):
            timeline.append("read_root")
            if graph_error is not None:
                raise graph_error
            return (rows if rows is not None else (family_rows(root_body=body))), BOARD_LINKS

        async def confirmed(_runner, trig, **_kwargs):
            timeline.append("confirm_consulted")
            return confirm_marker

        mod.read_board_graph = read_graph
        mod.trigger_is_confirmed = confirmed

        async def atick():
            report = await runtime.tick()
            row = state.conn.execute(
                "SELECT status,error FROM family_receipts WHERE review_key=?",
                (trigger.review_key,),
            ).fetchone()
            return report, (row["status"] if row else None), (row["error"] if row else None)

        return {"atick": atick, "close": lambda: (mod.read_board_graph.__setattr__("__name__", mod.read_board_graph.__name__) or None), "adapter": adapter,
                "calls": calls, "timeline": timeline, "state": state, "trigger": trigger,
                "restore": lambda: None}

    def _run(self, tmp, **kwargs):
        harness = self._harness(tmp, **kwargs)
        try:
            report, status, error = asyncio.run(harness["atick"]())
        finally:
            mod.read_board_graph = mod.read_board_graph  # patched handles restored below
        # restore module-level patches (the harness replaced them at build time)
        state = harness["state"]
        state.close()
        return report, status, error, harness

    def setUp(self):
        self._original_graph = mod.read_board_graph
        self._original_confirm = mod.trigger_is_confirmed

    def tearDown(self):
        mod.read_board_graph = self._original_graph
        mod.trigger_is_confirmed = self._original_confirm

    def _creator_target(self) -> Any:
        decision = mod.resolve_creator_target(
            fresh(), mod.parse_creator_provenance(PROVENANCE_BODY)
        )
        self.assertTrue(decision.ready, decision.reason_code)
        return decision.target

    def _wrong_sink_target(self) -> Any:
        return mod._session_entry_to_target(DEFAULT_ENTRIES[1])

    def test_missing_current_root_provenance_refuses_with_zero_injection(self):
        with tempfile.TemporaryDirectory() as tmp:
            report, status, error, harness = self._run(
                tmp, target=self._creator_target(), body="", rows=QUIET_ROWS_NO_PROVENANCE
            )
        self.assertEqual(harness["adapter"].events, [])
        self.assertEqual(status, "uncertain")
        self.assertIn("HOLD_PROVENANCE_MISSING", (error or "") + json.dumps(report["errors"]))

    def test_malformed_current_root_provenance_refuses_with_zero_injection(self):
        with tempfile.TemporaryDirectory() as tmp:
            report, status, error, harness = self._run(
                tmp, target=self._creator_target(), rows=QUIET_ROWS_MALFORMED
            )
        self.assertEqual(harness["adapter"].events, [])
        self.assertEqual(status, "uncertain")
        self.assertIn("HOLD_PROVENANCE_MALFORMED", (error or "") + json.dumps(report["errors"]))

    def test_unreadable_board_refuses_with_zero_injection(self):
        with tempfile.TemporaryDirectory() as tmp:
            report, status, error, harness = self._run(
                tmp, target=self._creator_target(), graph_error=OSError("synthetic board outage")
            )
        self.assertNotIn("dispatch", harness["calls"])
        self.assertEqual(harness["adapter"].events, [])
        self.assertEqual(status, "uncertain")

    def test_persisted_wrong_sink_target_with_positive_marker_refuses(self):
        # The marker claims confirmation, but the persisted target names a
        # different sink than the current root proves: refusal BEFORE the
        # marker is consulted, zero dispatch.
        with tempfile.TemporaryDirectory() as tmp:
            report, status, error, harness = self._run(
                tmp, target=self._wrong_sink_target(), confirm_marker=True
            )
        self.assertNotIn("confirm_consulted", harness["timeline"])
        self.assertNotIn("dispatch", harness["calls"])
        self.assertEqual(harness["adapter"].events, [])
        self.assertEqual(status, "uncertain")

    def test_marker_confirms_only_after_full_root_proof(self):
        with tempfile.TemporaryDirectory() as tmp:
            report, status, error, harness = self._run(
                tmp, target=self._creator_target(), confirm_marker=True
            )
        self.assertIn("read_root", harness["timeline"])
        self.assertIn("confirm_consulted", harness["timeline"])
        self.assertNotIn("dispatch", harness["calls"])
        self.assertEqual(harness["adapter"].events, [])
        self.assertEqual(status, "confirmed")
        self.assertEqual(report["triggers"], 1)

    def test_matching_pending_target_dispatches_exactly_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            harness = self._harness(tmp, target=self._creator_target(), confirm_marker=False)
            report, status, error = asyncio.run(harness["atick"]())
            state, trigger = harness["state"], harness["trigger"]
            row = state.conn.execute(
                "SELECT status,error,attempted_at FROM family_receipts WHERE review_key=?",
                (trigger.review_key,),
            ).fetchone()
            state.close()
        self.assertIn("dispatch", harness["calls"])
        self.assertEqual(len(harness["adapter"].events), 1)
        # Without a durable marker the dispatch is typed uncertain...
        self.assertEqual(row["status"], "uncertain")
        # ...and the attempt ledger durably records the ONE physical send.
        self.assertIsNotNone(row["attempted_at"])

    def test_attempted_uncertain_receipt_is_confirmation_only(self):
        # Second tick after an attempted-but-unconfirmed dispatch: no resend,
        # escalation only; the receipt is never re-dispatched.
        with tempfile.TemporaryDirectory() as tmp:
            harness = self._harness(tmp, target=self._creator_target(), confirm_marker=False)
            asyncio.run(harness["atick"]())
            harness["calls"].clear()
            report, status, error = asyncio.run(harness["atick"]())
            state = harness["state"]
            row = state.conn.execute(
                "SELECT status FROM family_receipts WHERE review_key=?",
                (harness["trigger"].review_key,),
            ).fetchone()
            state.close()
        self.assertEqual(harness["calls"], [])  # zero injector entries on the second pass
        self.assertEqual(len(harness["adapter"].events), 1)  # still exactly one physical send
        self.assertIn("unresolved", json.dumps(report.get("errors", [])) + json.dumps(report.get("unresolved", [])))


def iter_tests(suite: unittest.TestSuite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from iter_tests(item)
        else:
            yield item


def run_suite() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    declared_ids = sorted(test.id() for test in iter_tests(suite))
    print("TEST_IDS_SHA256=" + hashlib.sha256("\n".join(declared_ids).encode()).hexdigest())
    runner = unittest.TextTestRunner(verbosity=2, resultclass=RecordingResult)
    result: RecordingResult = runner.run(suite)  # type: ignore[assignment]
    outcomes = sorted(result.outcomes)
    print("TEST_COUNT=" + str(result.testsRun))
    print("RESULTS_SHA256=" + hashlib.sha256(json.dumps(outcomes, separators=(",", ":")).encode()).hexdigest())
    print("RESULTS_STATUS=" + json.dumps(outcomes, separators=(",", ":")))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(run_suite())
