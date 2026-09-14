"""Isolated functional tests for the public plugin source."""
from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import os
import sys
import threading
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


class Platform:
    def __init__(self, value: Any):
        self.value = str(value)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Platform) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)

    def __repr__(self) -> str:
        return f"Platform({self.value!r})"


Platform.DISCORD = Platform("discord")
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
CREATOR_CHAT = "fixture-creator-chat"
WRONG_SINK_CHAT = "fixture-wrong-sink"
SHARED_THREAD = "fixture-shared-thread"


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
        platform=Platform("discord"),
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
            self._adapters[("discord", PROFILE)] = adapter

    def _authorization_adapter(self, platform: Platform, profile: str | None) -> Any:
        return self._adapters.get((platform.value, profile or PROFILE))

    def _session_key_for_source(self, source: SessionSource) -> str:
        return session_key_for_source(source)


def session_key(chat_id: str) -> str:
    return session_key_for_source(make_source(chat_id))


KEY_CREATOR = session_key(CREATOR_CHAT)
KEY_WRONG = session_key(WRONG_SINK_CHAT)

DEFAULT_ENTRIES = [
    Entry("session-creator", KEY_CREATOR, make_source(CREATOR_CHAT, thread_id=CREATOR_CHAT)),
    Entry("session-wrong", KEY_WRONG, make_source(WRONG_SINK_CHAT, thread_id=WRONG_SINK_CHAT)),
    Entry("session-channel", session_key("fixture-channel"), make_source("fixture-channel", chat_type="channel")),
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


def root_rows(body: str) -> list[dict[str, Any]]:
    return [
        {"id": "root-fixture", "title": "root", "status": "ready", "assignee": "builder", "body": body},
        {"id": "child-fixture", "title": "child", "status": "todo", "assignee": "builder", "body": ""},
    ]


def make_trigger(target: Any, key: str = "receipt-fixture") -> Any:
    snapshot = core_mod.FamilySnapshot(
        state="heartbeat_quiescent",
        fingerprint="snapshot-fixture",
        counts={"ready": 1},
        blocked_ids=(),
        live_task_ids=(),
        tasks=(),
    )
    return mod.Trigger(
        review_key=key,
        board="board-fixture",
        root_task_id="root-fixture",
        root_title="root",
        target=target,
        outcome="heartbeat_quiescent",
        fingerprint=snapshot.fingerprint,
        episode=1,
        attempt=1,
        snapshot=snapshot,
    )


async def run_tick(runner: Runner, state_path: Path, tasks: list[dict[str, Any]]) -> dict[str, Any]:
    state = mod.StateStore(str(state_path))
    runtime = mod.OriginReviewRuntime(runner=runner, state_store=state, poll_seconds=1)
    runtime.board_paths = lambda: {"board-fixture": Path("/not-opened.sqlite3")}
    original = mod.read_board_graph
    mod.read_board_graph = lambda _path: (tasks, ())
    try:
        return await runtime.tick()
    finally:
        mod.read_board_graph = original
        state.close()


# ---- acceptance cases -----------------------------------------------------------
class ExactKeyAndAdapterRetention(unittest.TestCase):
    def test_exact_session_key_resolves_to_one_stored_record(self):
        runner = fresh()
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
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


class ThreadOnlyResolution(unittest.TestCase):
    def test_thread_as_chat_shape_resolves_uniquely(self):
        entry = Entry("session-thread-chat", session_key("fixture-thread-chat"), make_source("fixture-thread-chat"))
        runner = fresh(entries=[entry])
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(thread_id="fixture-thread-chat"))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertEqual(decision.target.session_id, "session-thread-chat")

    def test_explicit_thread_id_resolves_uniquely(self):
        entry = Entry("session-explicit", session_key("fixture-explicit"), make_source("fixture-explicit", thread_id="fixture-explicit-thread"))
        runner = fresh(entries=[entry])
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(thread_id="fixture-explicit-thread"))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertEqual(decision.target.session_key, entry.session_key)

    def test_true_thread_selector_ambiguity_holds_and_injects_zero(self):
        entries = [
            Entry("ambiguous-one", session_key("fixture-ambiguous-one"), make_source(SHARED_THREAD)),
            Entry("ambiguous-two", session_key("fixture-ambiguous-two"), make_source(SHARED_THREAD)),
        ]
        adapter = PropertyAdapter()
        runner = fresh(entries=entries, adapter=adapter)
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(thread_id=SHARED_THREAD))
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_AMBIGUOUS")
        self.assertIsNone(decision.target)
        self.assertEqual(adapter.events, [])
        with __import__("tempfile").TemporaryDirectory() as td:
            report = asyncio.run(run_tick(runner, Path(td) / "state.sqlite3", root_rows(provenance(thread_id=SHARED_THREAD))))
        self.assertEqual(report["triggers"], 0)
        self.assertEqual(adapter.events, [])
        self.assertIn("HOLD_SESSION_AMBIGUOUS", json.dumps(report["errors"]))


class ConjunctiveAndWrongSink(unittest.TestCase):
    def test_wrong_sink_selector_conflict_is_unresolvable(self):
        runner = fresh()
        decision = mod.resolve_creator_target(
            runner,
            mod.parse_creator_provenance(
                provenance(session_key=KEY_CREATOR, chat_id=WRONG_SINK_CHAT)
            ),
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_creator_thread_does_not_select_wrong_sink(self):
        entries = [
            Entry("creator-thread", session_key("fixture-creator-thread"), make_source("fixture-creator-thread")),
            Entry("wrong-thread", session_key("fixture-wrong-thread"), make_source("fixture-wrong-thread")),
        ]
        runner = fresh(entries=entries)
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(thread_id="fixture-creator-thread"))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertEqual(decision.target.session_id, "creator-thread")
        self.assertNotEqual(decision.target.session_id, "wrong-thread")

    def test_contradictory_session_id_and_key_holds(self):
        runner = fresh()
        decision = mod.resolve_creator_target(
            runner,
            mod.parse_creator_provenance(
                provenance(session_id="session-creator", session_key=KEY_WRONG)
            ),
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_session_key_whitespace_does_not_match_exact_record(self):
        runner = fresh()
        decision = mod.resolve_creator_target(
            runner,
            mod.parse_creator_provenance(provenance(session_key=" " + KEY_CREATOR)),
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_unknown_selector_key_is_malformed(self):
        text = (
            "[creator-session-provenance/v1]"
            + json.dumps({"profile": PROFILE, "source": "discord", "bogus": "x"}, separators=(",", ":"))
            + "[/creator-session-provenance/v1]"
        )
        decision = mod.parse_creator_provenance(text)
        self.assertEqual(decision.reason_code, "HOLD_PROVENANCE_MALFORMED")


class ProvenanceAndUnresolvable(unittest.TestCase):
    def test_missing_block_holds(self):
        self.assertEqual(mod.parse_creator_provenance("no block").reason_code, "HOLD_PROVENANCE_MISSING")

    def test_malformed_json_holds(self):
        text = "[creator-session-provenance/v1]{\"profile\":[/creator-session-provenance/v1]"
        self.assertEqual(mod.parse_creator_provenance(text).reason_code, "HOLD_PROVENANCE_MALFORMED")

    def test_multiple_blocks_hold(self):
        text = provenance(session_key=KEY_CREATOR) + provenance(session_key=KEY_WRONG)
        self.assertEqual(mod.parse_creator_provenance(text).reason_code, "HOLD_PROVENANCE_MULTIPLE")

    def test_duplicate_json_key_holds(self):
        text = "[creator-session-provenance/v1]{\"profile\":\"default\",\"profile\":\"default\",\"source\":\"discord\"}[/creator-session-provenance/v1]"
        self.assertEqual(mod.parse_creator_provenance(text).reason_code, "HOLD_PROVENANCE_MALFORMED")

    def test_empty_selectors_are_unresolvable(self):
        runner = fresh()
        decision = mod.resolve_creator_target(runner, mod.parse_creator_provenance(provenance()))
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_conversation_id_is_unresolvable(self):
        runner = fresh()
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(conversation_id="fixture-conversation"))
        )
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_missing_store_match_is_unresolvable(self):
        runner = fresh()
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=session_key("fixture-missing")))
        )
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_missing_provenance_tick_has_zero_injection(self):
        adapter = PropertyAdapter()
        runner = fresh(adapter=adapter)
        with __import__("tempfile").TemporaryDirectory() as td:
            report = asyncio.run(run_tick(runner, Path(td) / "state.sqlite3", root_rows("body without provenance")))
        self.assertEqual(report["triggers"], 0)
        self.assertEqual(report["families"], 0)
        self.assertEqual(adapter.events, [])
        self.assertIn("HOLD_PROVENANCE_MISSING", json.dumps(report["errors"]))


class ConcurrentSessionIsolation(unittest.TestCase):
    def test_two_creator_sessions_remain_isolated(self):
        first = Entry("session-one", session_key("fixture-one"), make_source("fixture-one", thread_id="thread-one"))
        second = Entry("session-two", session_key("fixture-two"), make_source("fixture-two", thread_id="thread-two"))
        runner = fresh(entries=[first, second])
        one = mod.resolve_creator_target(runner, mod.parse_creator_provenance(provenance(session_key=first.session_key)))
        two = mod.resolve_creator_target(runner, mod.parse_creator_provenance(provenance(session_key=second.session_key)))
        self.assertTrue(one.ready and two.ready)
        self.assertEqual(one.target.session_id, first.session_id)
        self.assertEqual(two.target.session_id, second.session_id)
        self.assertNotEqual(one.target.session_key, two.target.session_key)
        self.assertIs(one.adapter, two.adapter)


class StoredSessionSource(unittest.TestCase):
    def test_selected_source_fields_and_canonical_key_are_preserved(self):
        source = make_source(
            "fixture-rich-chat",
            thread_id="fixture-rich-thread",
            chat_type="thread",
            profile=PROFILE,
            chat_name="Rich fixture",
            parent_chat_id="fixture-parent",
            user_id="fixture-user",
            user_name="Fixture User",
            user_id_alt="fixture-user-alt",
            chat_id_alt="fixture-chat-alt",
            chat_topic="Fixture topic",
            scope_id="fixture-scope",
            guild_id="fixture-guild",
        )
        entry = Entry("session-rich", session_key_for_source(source), source)
        runner = fresh(entries=[entry])
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=entry.session_key))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        target = decision.target
        self.assertEqual(target.session_id, entry.session_id)
        self.assertEqual(target.session_key, entry.session_key)
        self.assertEqual(target.chat_id, source.chat_id)
        self.assertEqual(target.thread_id, source.thread_id)
        self.assertEqual(target.chat_name, source.chat_name)
        self.assertEqual(target.parent_chat_id, source.parent_chat_id)
        self.assertEqual(target.user_id, source.user_id)
        self.assertEqual(target.user_name, source.user_name)
        self.assertEqual(target.user_id_alt, source.user_id_alt)
        self.assertEqual(target.chat_id_alt, source.chat_id_alt)
        self.assertEqual(target.chat_topic, source.chat_topic)
        self.assertEqual(target.scope_id, source.scope_id)
        self.assertEqual(target.guild_id, source.guild_id)
        self.assertIs(decision.adapter, runner._adapters[("discord", PROFILE)])

    def test_injection_receives_stored_source_identity_fields(self):
        source = make_source(
            "fixture-inject-chat",
            thread_id="fixture-inject-thread",
            parent_chat_id="fixture-inject-parent",
            chat_name="Inject fixture",
            user_id="fixture-inject-user",
            user_name="Inject User",
        )
        entry = Entry("session-inject", session_key_for_source(source), source)
        adapter = PropertyAdapter()
        runner = fresh(entries=[entry], adapter=adapter)
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=entry.session_key))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        result = asyncio.run(
            mod.inject_trigger(
                runner,
                make_trigger(decision.target, key="receipt-source"),
                parent_channel_id="unused-parent",
                confirmation_timeout=0,
                resolved_adapter=decision.adapter,
            )
        )
        self.assertFalse(result)
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
        self.assertIn("[kanban-origin-review:receipt-source]", event.text)


class LiteralAuthorityScope(unittest.TestCase):
    def _decision(self, **kwargs: Any) -> Any:
        runner = fresh()
        return mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR, **kwargs))
        )

    def test_uppercase_profile_is_rejected(self):
        decision = self._decision(profile="DEFAULT")
        self.assertEqual(decision.reason_code, "SKIP_UNSUPPORTED_SOURCE")

    def test_mixed_case_profile_is_rejected(self):
        decision = self._decision(profile="Default")
        self.assertEqual(decision.reason_code, "SKIP_UNSUPPORTED_SOURCE")

    def test_profile_whitespace_is_rejected_without_normalization(self):
        decision = self._decision(profile=" default")
        self.assertEqual(decision.reason_code, "SKIP_UNSUPPORTED_SOURCE")

    def test_uppercase_source_is_rejected_without_normalization(self):
        decision = self._decision(source="DISCORD")
        self.assertEqual(decision.reason_code, "SKIP_UNSUPPORTED_SOURCE")

    def test_source_whitespace_is_rejected_without_normalization(self):
        decision = self._decision(source=" discord")
        self.assertEqual(decision.reason_code, "SKIP_UNSUPPORTED_SOURCE")

    def test_other_source_is_rejected(self):
        decision = self._decision(source="slack")
        self.assertEqual(decision.reason_code, "SKIP_UNSUPPORTED_SOURCE")

    def test_authority_fields_are_preserved_in_parsed_claim(self):
        claim = mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        self.assertEqual(claim.profile, PROFILE)
        self.assertEqual(claim.source, "discord")


class AdapterPropertyABI(unittest.TestCase):
    def _resolve(self, adapter: Any) -> Any:
        runner = fresh(adapter=adapter)
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_DESTINATION_UNUSABLE")
        return decision

    def test_property_shaped_healthy_adapter_succeeds(self):
        adapter = PropertyAdapter(True)
        runner = fresh(adapter=adapter)
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertIs(decision.adapter, adapter)

    def test_callable_method_only_shape_is_rejected(self):
        self._resolve(MethodOnlyAdapter())

    def test_false_property_is_rejected(self):
        self._resolve(PropertyAdapter(False))

    def test_non_boolean_property_is_rejected(self):
        self._resolve(PropertyAdapter(1))

    def test_raising_property_is_rejected(self):
        adapter = PropertyAdapter(True)
        adapter.raise_connectivity = True
        self._resolve(adapter)

    def test_missing_adapter_is_rejected(self):
        runner = Runner(Store(list(DEFAULT_ENTRIES)), None)
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_DESTINATION_UNUSABLE")

    def test_non_callable_handle_message_is_rejected(self):
        adapter = PropertyAdapter(True)
        adapter.handle_message = "not-callable"
        self._resolve(adapter)


class PreDispatchReadinessRace(unittest.TestCase):
    def _assert_rejected_before_dispatch(self, mutate: Any, *, replacement: Any = None) -> None:
        adapter = PropertyAdapter(True)
        runner = fresh(adapter=adapter)
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertIs(decision.adapter, adapter)
        mutate(runner, adapter)
        if replacement is not None:
            tracked = [adapter, replacement]
        else:
            tracked = [adapter]
        confirmations: list[str] = []
        original_confirm = mod.trigger_is_confirmed

        async def unexpected_confirmation(*_args: Any, **_kwargs: Any) -> bool:
            confirmations.append("called")
            return True

        mod.trigger_is_confirmed = unexpected_confirmation
        try:
            with self.assertRaises(RuntimeError):
                asyncio.run(
                    mod.inject_trigger(
                        runner,
                        make_trigger(decision.target, key="race-receipt"),
                        parent_channel_id="unused-parent",
                        confirmation_timeout=0,
                        resolved_adapter=decision.adapter,
                    )
                )
        finally:
            mod.trigger_is_confirmed = original_confirm
        self.assertEqual(confirmations, [])
        self.assertEqual(sum(len(item.events) for item in tracked), 0)

    def test_disconnect_after_resolution_holds_before_handle(self):
        self._assert_rejected_before_dispatch(lambda _runner, adapter: setattr(adapter, "connectivity", False))

    def test_replacement_after_resolution_holds_on_identity(self):
        replacement = PropertyAdapter(True)
        self._assert_rejected_before_dispatch(
            lambda runner, _adapter: runner._adapters.__setitem__(("discord", PROFILE), replacement),
            replacement=replacement,
        )

    def test_missing_adapter_after_resolution_holds_before_handle(self):
        self._assert_rejected_before_dispatch(lambda runner, _adapter: runner._adapters.clear())

    def test_non_callable_handle_after_resolution_holds_before_handle(self):
        self._assert_rejected_before_dispatch(lambda _runner, adapter: setattr(adapter, "handle_message", "nope"))

    def test_false_property_after_resolution_holds_before_handle(self):
        self._assert_rejected_before_dispatch(lambda _runner, adapter: setattr(adapter, "connectivity", False))

    def test_non_boolean_property_after_resolution_holds_before_handle(self):
        self._assert_rejected_before_dispatch(lambda _runner, adapter: setattr(adapter, "connectivity", 1))

    def test_raising_property_after_resolution_holds_before_handle(self):
        self._assert_rejected_before_dispatch(lambda _runner, adapter: setattr(adapter, "raise_connectivity", True))


class NoHardcodedFixtureIdentity(unittest.TestCase):
    def test_runtime_has_no_fixture_identity_literals(self):
        text = (PACKET / "runtime.py").read_text(encoding="utf-8")
        for value in (CREATOR_CHAT, WRONG_SINK_CHAT, SHARED_THREAD, "fixture-creator"):
            self.assertNotIn(value, text)


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



# ---- pending receipt recovery cases ---------------------------------------------
# tick() ordering note: recovery (unfinished receipts, including a board read)
# runs BEFORE the fresh-observation board read, so a refusing pending receipt
# records read_root twice (recovery read + observation read).

def _make_quiet_snapshot():
    return core_mod.FamilySnapshot(
        state="heartbeat_quiescent",
        fingerprint="quiet-fixture",
        counts={"ready": 1},
        blocked_ids=(),
        live_task_ids=(),
        tasks=(),
    )


def _make_live_snapshot():
    return core_mod.FamilySnapshot(
        state=core_mod.HEARTBEAT_LIVE_STATE,
        fingerprint="live-fixture",
        counts={"running": 1},
        blocked_ids=(),
        live_task_ids=("root-fixture",),
        tasks=(),
    )


def _creator_target_for_body(body: str) -> Any:
    runner = fresh()
    decision = mod.resolve_creator_target(runner, mod.parse_creator_provenance(body))
    assert decision.ready, decision.reason_code
    return decision.target


def _seed_pending_and_reopen(tmp: str, target: Any) -> tuple[Any, Any]:
    """Persist a real pending receipt through the public StateStore API, close,
    reopen (restart equivalent), and return the reopened store plus trigger."""
    dbpath = Path(tmp) / "state.sqlite3"
    state = mod.StateStore(str(dbpath))
    created = state.observe("board-fixture", "root-fixture", "root", target, _make_live_snapshot())
    assert created is None, "live observation must arm only"
    quiet = _make_quiet_snapshot()
    trigger = None
    for _ in range(2):
        trigger = state.observe("board-fixture", "root-fixture", "root", target, quiet)
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


class _PendingHarness:
    """tick() recovery against a fixture board graph with call recording."""

    def __init__(
        self,
        tmp: str,
        *,
        target: Any,
        adapter: PropertyAdapter | None = None,
        body: Any = provenance(thread_id=CREATOR_CHAT),
        graph_error: Any = None,
        swap_adapter_before_inject: PropertyAdapter | None = None,
    ):
        self.adapter = PropertyAdapter() if adapter is None else adapter
        self.runner = fresh(adapter=self.adapter)
        self.swap_adapter_before_inject = swap_adapter_before_inject
        self.body = body
        self.graph_error = graph_error
        self.calls: list[str] = []
        self.confirmation_calls = 0
        self.state, self.trigger = _seed_pending_and_reopen(tmp, target)
        self.runtime = mod.OriginReviewRuntime(
            runner=self.runner,
            state_store=self.state,
            poll_seconds=1,
            injector=self._dispatch,
        )
        self.runtime.board_paths = lambda: {"board-fixture": Path("/not-opened.sqlite3")}
        self._old_graph = mod.read_board_graph
        self._old_confirm = mod.trigger_is_confirmed
        mod.read_board_graph = self._read_graph

        async def confirmed(_runner, trigger, **_kwargs):
            self.confirmation_calls += 1
            return any(e.message_id == trigger.review_key for e in self.adapter.events)

        mod.trigger_is_confirmed = confirmed

    def _read_graph(self, _path):
        self.calls.append("read_root")
        if self.graph_error is not None:
            raise self.graph_error
        return root_rows(self.body), ()

    async def _dispatch(self, runner, trigger, **kwargs):
        self.calls.append("dispatch")
        if self.swap_adapter_before_inject is not None:
            runner._adapters[("discord", PROFILE)] = self.swap_adapter_before_inject
        return await mod.inject_trigger(runner, trigger, confirmation_timeout=0, **kwargs)

    async def atick(self):
        report = await self.runtime.tick()
        row = self.state.conn.execute(
            "SELECT status,error FROM family_receipts WHERE review_key=?",
            (self.trigger.review_key,),
        ).fetchone()
        return report, (row["status"] if row else None), (row["error"] if row else None)

    def close(self):
        mod.read_board_graph = self._old_graph
        mod.trigger_is_confirmed = self._old_confirm
        self.state.close()


class PendingRecoveryProvenance(unittest.TestCase):
    """A persisted pending receipt dispatches only after the current root
    re-proves the exact same origin through the request-local parser/resolver
    pair, with full target agreement and the retained ready adapter."""

    def _persisted_wrong_sink_target(self) -> Any:
        return mod._session_entry_to_target(DEFAULT_ENTRIES[1])

    def _creator_target(self) -> Any:
        return _creator_target_for_body(provenance(thread_id=CREATOR_CHAT))

    def _run(self, harness: _PendingHarness):
        try:
            return asyncio.run(harness.atick())
        finally:
            harness.close()

    def _assert_refused(self, harness, report, status):
        self.assertNotIn("dispatch", harness.calls)
        self.assertEqual(harness.adapter.events, [])
        self.assertEqual(status, "uncertain")
        self.assertIn("HOLD_PENDING_ORIGIN_UNPROVEN", json.dumps(report["errors"]))

    def test_read_precedes_dispatch_on_happy_path(self):
        adapter = PropertyAdapter()
        with __import__("tempfile").TemporaryDirectory() as tmp:
            harness = _PendingHarness(tmp, target=self._creator_target(), adapter=adapter)
            try:
                report, status, _error = asyncio.run(harness.atick())
            finally:
                harness.close()
        self.assertEqual(harness.calls, ["read_root", "dispatch", "read_root"])
        self.assertEqual(len(adapter.events), 1)
        self.assertEqual(status, "confirmed")
        self.assertEqual(report["triggers"], 1)

    def test_missing_current_root_provenance_refuses_with_zero_injection(self):
        with __import__("tempfile").TemporaryDirectory() as tmp:
            harness = _PendingHarness(tmp, target=self._creator_target(), body="")
            report, status, _error = self._run(harness)
        self.assertEqual(harness.calls, ["read_root", "read_root"])
        self._assert_refused(harness, report, status)

    def test_malformed_current_root_provenance_refuses_with_zero_injection(self):
        with __import__("tempfile").TemporaryDirectory() as tmp:
            harness = _PendingHarness(
                tmp,
                target=self._creator_target(),
                body="[creator-session-provenance/v1]{bad}[/creator-session-provenance/v1]",
            )
            report, status, _error = self._run(harness)
        self.assertEqual(harness.calls, ["read_root", "read_root"])
        self._assert_refused(harness, report, status)

    def test_multiple_conflicting_blocks_refuse_with_zero_injection(self):
        body = provenance(thread_id=CREATOR_CHAT) + provenance(thread_id=WRONG_SINK_CHAT)
        with __import__("tempfile").TemporaryDirectory() as tmp:
            harness = _PendingHarness(tmp, target=self._creator_target(), body=body)
            report, status, _error = self._run(harness)
        self.assertEqual(harness.calls, ["read_root", "read_root"])
        self._assert_refused(harness, report, status)

    def test_unreadable_board_refuses_with_zero_injection(self):
        with __import__("tempfile").TemporaryDirectory() as tmp:
            harness = _PendingHarness(
                tmp,
                target=self._creator_target(),
                graph_error=OSError("local unavailable-board fixture"),
            )
            report, status, _error = self._run(harness)
        self.assertEqual(harness.calls, ["read_root", "read_root"])
        self.assertNotIn("dispatch", harness.calls)
        self.assertEqual(harness.adapter.events, [])
        self.assertEqual(status, "uncertain")
        self.assertIn("HOLD_PENDING_ORIGIN_UNPROVEN", json.dumps(report["errors"]))

    def test_persisted_wrong_sink_target_mismatch_refuses_with_zero_injection(self):
        with __import__("tempfile").TemporaryDirectory() as tmp:
            harness = _PendingHarness(tmp, target=self._persisted_wrong_sink_target())
            report, status, _error = self._run(harness)
        self.assertIn("read_root", harness.calls)
        self._assert_refused(harness, report, status)

    def test_unresolvable_current_selector_refuses_with_zero_injection(self):
        with __import__("tempfile").TemporaryDirectory() as tmp:
            harness = _PendingHarness(
                tmp,
                target=self._creator_target(),
                body=provenance(thread_id="fixture-unknown-thread"),
            )
            _report, status, _error = self._run(harness)
        self.assertNotIn("dispatch", harness.calls)
        self.assertEqual(harness.adapter.events, [])
        self.assertEqual(status, "uncertain")

    def test_adapter_replacement_mid_dispatch_holds_uncertain_zero_events(self):
        replacement = PropertyAdapter(True)
        with __import__("tempfile").TemporaryDirectory() as tmp:
            harness = _PendingHarness(
                tmp,
                target=self._creator_target(),
                swap_adapter_before_inject=replacement,
            )
            report, status, error = self._run(harness)
        self.assertIn("dispatch", harness.calls)
        self.assertEqual(harness.adapter.events, [])
        self.assertEqual(replacement.events, [])
        self.assertEqual(status, "uncertain")
        self.assertIn("target adapter binding changed", json.dumps(report["errors"]))

    def test_disconnected_adapter_refuses_pending_dispatch(self):
        adapter = PropertyAdapter(False)
        with __import__("tempfile").TemporaryDirectory() as tmp:
            harness = _PendingHarness(tmp, target=self._creator_target(), adapter=adapter)
            _report, status, _error = self._run(harness)
        self.assertNotIn("dispatch", harness.calls)
        self.assertEqual(adapter.events, [])
        self.assertEqual(status, "uncertain")

    def test_non_callable_handle_refuses_pending_dispatch(self):
        adapter = PropertyAdapter(True)
        adapter.handle_message = "nope"
        with __import__("tempfile").TemporaryDirectory() as tmp:
            harness = _PendingHarness(tmp, target=self._creator_target(), adapter=adapter)
            _report, status, _error = self._run(harness)
        self.assertNotIn("dispatch", harness.calls)
        self.assertEqual(adapter.events, [])
        self.assertEqual(status, "uncertain")

    def test_matching_pending_target_dispatches_exactly_once(self):
        adapter = PropertyAdapter()
        with __import__("tempfile").TemporaryDirectory() as tmp:
            harness = _PendingHarness(tmp, target=self._creator_target(), adapter=adapter)
            try:
                _report, first_status, _e = asyncio.run(harness.atick())
                _report2, second_status, _e2 = asyncio.run(harness.atick())
            finally:
                harness.close()
        self.assertEqual(len(adapter.events), 1)
        self.assertEqual(first_status, "confirmed")
        self.assertEqual(second_status, "confirmed")

    def test_uncertain_receipt_is_confirmation_only_never_redispatched(self):
        adapter = PropertyAdapter()
        runner = fresh(adapter=adapter)
        target = self._creator_target()
        dispatches: list[str] = []

        async def _dispatch(_runner, _trigger, **_kwargs):
            dispatches.append("dispatch")
            return False

        with __import__("tempfile").TemporaryDirectory() as tmp:
            state = mod.StateStore(str(Path(tmp) / "state.sqlite3"))
            state.observe("board-fixture", "root-fixture", "root", target, _make_live_snapshot())
            quiet = _make_quiet_snapshot()
            trigger = None
            for _ in range(2):
                trigger = state.observe("board-fixture", "root-fixture", "root", target, quiet)
                if trigger is not None:
                    break
            assert trigger is not None
            state.set_status(trigger, "uncertain", error="fixture-seeded")
            runtime = mod.OriginReviewRuntime(
                runner=runner, state_store=state, poll_seconds=1, injector=_dispatch
            )
            runtime.board_paths = lambda: {"board-fixture": Path("/not-opened.sqlite3")}
            old_graph = mod.read_board_graph
            mod.read_board_graph = lambda _path: (root_rows(provenance(thread_id=CREATOR_CHAT)), ())
            try:
                report = asyncio.run(runtime.tick())
            finally:
                mod.read_board_graph = old_graph
            row = state.conn.execute(
                "SELECT status FROM family_receipts WHERE review_key=?", (trigger.review_key,)
            ).fetchone()
            state.close()
        self.assertEqual(dispatches, [])
        self.assertEqual(adapter.events, [])
        self.assertEqual(row["status"], "uncertain")
        self.assertEqual(report["triggers"], 0)


class ProfileBridge(unittest.TestCase):
    """Omitted, empty, and explicit-default stored profiles are one canonical
    default-namespace identity and must bind and dispatch identically, with
    the stored SessionSource representation preserved exactly; a
    default-profile claim must never select a differently-named record."""

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

    def _assert_default_variant_binds(self, profile: Any) -> None:
        entry = self._entry(profile)
        runner, decision = self._resolve([entry])
        self.assertTrue(decision.ready, decision.reason_code)
        # Preserve the exact stored representation.
        expected = profile
        self.assertEqual(decision.target.profile, expected)
        self.assertTrue(mod._binding_matches(runner, decision.target))
        self.assertEqual(decision.target.session_key, entry.session_key)

    def test_omitted_none_stored_profile_binds_and_matches(self):
        self._assert_default_variant_binds(None)

    def test_empty_stored_profile_binds_and_matches(self):
        self._assert_default_variant_binds("")

    def test_explicit_default_stored_profile_binds_and_matches(self):
        self._assert_default_variant_binds("default")

    def test_omitted_profile_end_to_end_dispatch_preserves_exact_source(self):
        entry = self._entry(None)
        adapter = PropertyAdapter()
        runner = fresh(entries=[entry], adapter=adapter)
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=entry.session_key))
        )
        self.assertTrue(decision.ready, decision.reason_code)
        asyncio.run(
            mod.inject_trigger(
                runner,
                make_trigger(decision.target, key="receipt-profile"),
                parent_channel_id="unused-parent",
                confirmation_timeout=0,
                resolved_adapter=decision.adapter,
            )
        )
        self.assertEqual(len(adapter.events), 1)
        self.assertIsNone(adapter.events[0].source.profile)
        self.assertEqual(session_key_for_source(adapter.events[0].source), entry.session_key)

    def test_non_default_stored_profile_fails_closed(self):
        entry = self._entry("work")
        _runner, decision = self._resolve([entry])
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_default_profile_claim_cannot_select_non_default_record(self):
        entry = self._entry("work")
        runner = fresh(entries=[entry])
        decision = mod.resolve_creator_target(
            runner,
            mod.parse_creator_provenance(provenance(session_key=entry.session_key)),
        )
        self.assertFalse(decision.ready)

    def test_canonical_key_mismatch_fails_closed(self):
        entry = self._entry(None)
        runner = fresh(entries=[entry])
        decision = mod.resolve_creator_target(
            runner,
            mod.parse_creator_provenance(provenance(session_key=KEY_WRONG)),
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_ambiguous_same_key_records_fail_closed(self):
        first = self._entry(None)
        second = Entry(first.session_id + "-alt", first.session_key, self._entry("default").origin)
        runner = fresh(entries=[first, second])
        decision = mod.resolve_creator_target(
            runner,
            mod.parse_creator_provenance(provenance(session_key=first.session_key)),
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_AMBIGUOUS")

    def test_literal_authority_variants_still_rejected(self):
        runner = fresh()
        for kwargs in ({"profile": "DEFAULT"}, {"profile": " default"}, {"source": "DISCORD"}):
            decision = mod.resolve_creator_target(
                runner,
                mod.parse_creator_provenance(provenance(session_key=KEY_CREATOR, **kwargs)),
            )
            self.assertFalse(decision.ready)
            self.assertEqual(decision.reason_code, "SKIP_UNSUPPORTED_SOURCE")


class ParentSourcePreservation(unittest.TestCase):
    """MessageEvent.source.parent_chat_id preserves the stored target
    representation exactly; the caller/default parent never fills absence."""

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

    def _inject(self, runner, adapter, decision, key: str):
        return asyncio.run(
            mod.inject_trigger(
                runner,
                make_trigger(decision.target, key=key),
                parent_channel_id="caller-parent-must-not-fill",
                confirmation_timeout=0,
                resolved_adapter=decision.adapter,
            )
        )

    def test_absent_none_parent_stays_none(self):
        runner, adapter, decision = self._setup(None)
        self.assertIsNone(decision.target.parent_chat_id)
        self._inject(runner, adapter, decision, "parent-none")
        self.assertEqual(len(adapter.events), 1)
        self.assertIsNone(adapter.events[0].source.parent_chat_id)

    def test_nonempty_parent_stays_byte_exact(self):
        runner, adapter, decision = self._setup("stored-parent-123")
        self.assertEqual(decision.target.parent_chat_id, "stored-parent-123")
        self._inject(runner, adapter, decision, "parent-stored")
        self.assertEqual(len(adapter.events), 1)
        self.assertEqual(adapter.events[0].source.parent_chat_id, "stored-parent-123")

    def test_whitespace_only_parent_preserved_byte_exact(self):
        # Preserve whitespace-only string parents byte-for-byte.
        runner, adapter, decision = self._setup("   ")
        self.assertEqual(decision.target.parent_chat_id, "   ")
        self._inject(runner, adapter, decision, "parent-blank")
        self.assertEqual(len(adapter.events), 1)
        self.assertEqual(adapter.events[0].source.parent_chat_id, "   ")

    def test_caller_fallback_never_fills_absence(self):
        runner, adapter, decision = self._setup(None)
        self._inject(runner, adapter, decision, "parent-no-fill")
        self.assertEqual(len(adapter.events), 1)
        self.assertNotEqual(adapter.events[0].source.parent_chat_id, "caller-parent-must-not-fill")
        self.assertIsNone(adapter.events[0].source.parent_chat_id)

    def test_receipt_round_trip_preserves_parent_none(self):
        source = make_source(CREATOR_CHAT, thread_id=CREATOR_CHAT, parent_chat_id=None)
        entry = Entry("session-parent-rt", session_key_for_source(source), source)
        runner = fresh(entries=[entry])
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(session_key=entry.session_key))
        )
        self.assertTrue(decision.ready)
        with __import__("tempfile").TemporaryDirectory() as tmp:
            state, trigger = _seed_pending_and_reopen(tmp, decision.target)
            try:
                self.assertIsNone(trigger.target.parent_chat_id)
            finally:
                state.close()



# ---- pending confirmation and representation cases ------------------------------

def _make_quiet_snapshot_representation():
    return core_mod.FamilySnapshot(
        state="heartbeat_quiescent",
        fingerprint="quiet-fixture",
        counts={"ready": 1},
        blocked_ids=(),
        live_task_ids=(),
        tasks=(),
    )


def _make_live_snapshot_representation():
    return core_mod.FamilySnapshot(
        state=core_mod.HEARTBEAT_LIVE_STATE,
        fingerprint="live-fixture",
        counts={"running": 1},
        blocked_ids=(),
        live_task_ids=("root-fixture",),
        tasks=(),
    )


class PendingMarkerFalseConfirmation(unittest.TestCase):
    """For a pending receipt no durable
    marker may confirm anything before the current canonical root is read,
    strictly parsed/resolved, and fully agreed with the persisted receipt
    target; marker-confirm-without-resend exists only AFTER that full proof.
    Every refusal stays typed uncertain/HOLD with zero dispatch; non-pending
    unfinished receipts keep confirmation-only/no-resend semantics."""

    def _persisted_wrong_sink_target(self):
        return mod._session_entry_to_target(DEFAULT_ENTRIES[1])

    def _creator_target(self):
        runner = fresh()
        decision = mod.resolve_creator_target(
            runner, mod.parse_creator_provenance(provenance(thread_id=CREATOR_CHAT))
        )
        assert decision.ready, decision.reason_code
        return decision.target

    def _seed_pending(self, tmp, target):
        dbpath = Path(tmp) / "state.sqlite3"
        state = mod.StateStore(str(dbpath))
        created = state.observe(
            "board-fixture", "root-fixture", "root", target, _make_live_snapshot_representation()
        )
        assert created is None, "live observation must arm only"
        quiet = _make_quiet_snapshot_representation()
        trigger = None
        for _ in range(2):
            trigger = state.observe("board-fixture", "root-fixture", "root", target, quiet)
            if trigger is not None:
                break
        assert trigger is not None, "quiescent debounce did not emit trigger"
        assert [s for s, _ in state.unfinished()] == ["pending"]
        state.close()
        reopened = mod.StateStore(str(dbpath))
        return reopened, [t for _s, t in reopened.unfinished()][0]

    def _run_recovery(
        self,
        tmp,
        *,
        target,
        body=provenance(thread_id=CREATOR_CHAT),
        confirm_marker=False,
    ):
        """Seed a pending receipt, reopen, run one tick against a fixture
        board with the durable marker stubbed.  Returns (report, status,
        events, dispatches, timeline, adapter).  The timeline records the
        exact interleaving of board reads ("read_root") and durable-marker
        consultations ("confirm_consulted"), so ordering can be asserted
        directly."""
        adapter = PropertyAdapter()
        runner = fresh(adapter=adapter)
        dispatches: list[str] = []
        timeline: list[str] = []

        async def _dispatch(_runner, _trigger, **_kwargs):
            dispatches.append("dispatch")
            timeline.append("dispatch")
            return await mod.inject_trigger(
                _runner, _trigger, confirmation_timeout=0
            )

        state, trigger = self._seed_pending(tmp, target)
        runtime = mod.OriginReviewRuntime(
            runner=runner, state_store=state, poll_seconds=1, injector=_dispatch
        )
        runtime.board_paths = lambda: {"board-fixture": Path("/not-opened.sqlite3")}
        old_graph = mod.read_board_graph
        old_confirm = mod.trigger_is_confirmed

        def read_graph(_path):
            timeline.append("read_root")
            return root_rows(body), ()

        async def confirmed(_runner, _trig):
            timeline.append("confirm_consulted")
            return confirm_marker

        mod.read_board_graph = read_graph
        mod.trigger_is_confirmed = confirmed
        try:
            report = asyncio.run(runtime.tick())
            row = state.conn.execute(
                "SELECT status,error FROM family_receipts WHERE review_key=?",
                (trigger.review_key,),
            ).fetchone()
        finally:
            mod.read_board_graph = old_graph
            mod.trigger_is_confirmed = old_confirm
            state.close()
        return (
            report,
            (row["status"] if row else None),
            adapter.events,
            dispatches,
            timeline,
            adapter,
        )

    # --- confirmation at a mismatched sink -------------------------------
    def test_marker_at_wrong_sink_cannot_confirm_pending_receipt(self):
        with __import__("tempfile").TemporaryDirectory() as tmp:
            report, status, events, dispatches, timeline, _a = self._run_recovery(
                tmp,
                target=self._persisted_wrong_sink_target(),
                confirm_marker=True,
            )
        self.assertEqual(dispatches, [])
        self.assertEqual(events, [])
        self.assertEqual(status, "uncertain")
        self.assertIn(
            "HOLD_PENDING_ORIGIN_UNPROVEN", json.dumps(report["errors"])
        )
        # The marker is NEVER consulted: the root proof refuses first.  The
        # second read is the fresh-observation pass of the same tick.
        self.assertEqual(timeline, ["read_root", "read_root"])

    # --- marker-confirm-without-resend exists only AFTER the full proof ---
    def test_marker_consulted_only_after_full_root_proof(self):
        with __import__("tempfile").TemporaryDirectory() as tmp:
            _report, status, events, dispatches, timeline, _a = self._run_recovery(
                tmp,
                target=self._creator_target(),
                confirm_marker=True,
            )
        # Ordering: the current root is read FIRST; only then is the marker
        # consulted (post-proof confirm-without-resend), and never dispatched.
        self.assertEqual(
            timeline, ["read_root", "confirm_consulted", "read_root"]
        )
        self.assertEqual(dispatches, [])
        self.assertEqual(events, [])
        self.assertEqual(status, "confirmed")

    def test_marker_false_positive_on_unproven_root_keeps_zero_dispatch(self):
        # The marker claims confirmation, but the current root is malformed:
        # the receipt must stay uncertain with zero dispatch and the marker
        # must not have been consulted at all.
        with __import__("tempfile").TemporaryDirectory() as tmp:
            _report, status, events, dispatches, timeline, _a = self._run_recovery(
                tmp,
                target=self._creator_target(),
                body="[creator-session-provenance/v1]{bad}[/creator-session-provenance/v1]",
                confirm_marker=True,
            )
        self.assertNotIn("confirm_consulted", timeline)
        self.assertEqual(dispatches, [])
        self.assertEqual(events, [])
        self.assertEqual(status, "uncertain")

    def test_marker_false_positive_on_missing_root_keeps_zero_dispatch(self):
        with __import__("tempfile").TemporaryDirectory() as tmp:
            _report, status, events, dispatches, timeline, _a = self._run_recovery(
                tmp,
                target=self._creator_target(),
                body="",
                confirm_marker=True,
            )
        self.assertNotIn("confirm_consulted", timeline)
        self.assertEqual(dispatches, [])
        self.assertEqual(events, [])
        self.assertEqual(status, "uncertain")

    def test_marker_false_positive_on_target_mismatch_keeps_zero_dispatch(self):
        with __import__("tempfile").TemporaryDirectory() as tmp:
            _report, status, events, dispatches, timeline, _a = self._run_recovery(
                tmp,
                target=self._persisted_wrong_sink_target(),
                confirm_marker=True,
            )
        self.assertNotIn("confirm_consulted", timeline)
        self.assertEqual(dispatches, [])
        self.assertEqual(events, [])
        self.assertEqual(status, "uncertain")

    def test_confirmation_only_semantics_preserved_for_uncertain_receipt(self):
        adapter = PropertyAdapter()
        runner = fresh(adapter=adapter)
        target = self._creator_target()
        dispatches: list[str] = []
        confirm_calls: list[str] = []

        async def _dispatch(_runner, _trigger, **_kwargs):
            dispatches.append("dispatch")
            return False

        with __import__("tempfile").TemporaryDirectory() as tmp:
            state = mod.StateStore(str(Path(tmp) / "state.sqlite3"))
            state.observe(
                "board-fixture", "root-fixture", "root", target, _make_live_snapshot_representation()
            )
            quiet = _make_quiet_snapshot_representation()
            trigger = None
            for _ in range(2):
                trigger = state.observe(
                    "board-fixture", "root-fixture", "root", target, quiet
                )
                if trigger is not None:
                    break
            assert trigger is not None
            state.set_status(trigger, "uncertain", error="fixture-seeded")
            runtime = mod.OriginReviewRuntime(
                runner=runner, state_store=state, poll_seconds=1, injector=_dispatch
            )
            runtime.board_paths = lambda: {"board-fixture": Path("/not-opened.sqlite3")}
            old_graph = mod.read_board_graph
            old_confirm = mod.trigger_is_confirmed
            mod.read_board_graph = lambda _path: (
                root_rows(provenance(thread_id=CREATOR_CHAT)),
                (),
            )

            async def confirmed(_runner, trig):
                confirm_calls.append(trig.review_key)
                return True

            mod.trigger_is_confirmed = confirmed
            try:
                report = asyncio.run(runtime.tick())
            finally:
                mod.read_board_graph = old_graph
                mod.trigger_is_confirmed = old_confirm
            row = state.conn.execute(
                "SELECT status FROM family_receipts WHERE review_key=?",
                (trigger.review_key,),
            ).fetchone()
            state.close()
        self.assertEqual(dispatches, [])
        self.assertEqual(adapter.events, [])
        self.assertEqual(row["status"], "confirmed")
        self.assertEqual(report["triggers"], 1)
        self.assertEqual(confirm_calls, [trigger.review_key])


class StoredProfileRepresentation(unittest.TestCase):
    """The exact stored profile
    representation (None, empty string, explicit default) is carried through
    WakeTarget and the emitted MessageEvent.source; canonical default-namespace
    identity is compared separately."""

    def _entry(self, profile):
        source = make_source(CREATOR_CHAT, thread_id=CREATOR_CHAT, profile=profile)
        return Entry(
            "session-profile-" + repr(profile),
            session_key_for_source(source),
            source,
        )

    def _resolve(self, entries):
        runner = fresh(entries=entries)
        decision = mod.resolve_creator_target(
            runner,
            mod.parse_creator_provenance(
                provenance(session_key=entries[0].session_key)
            ),
        )
        return runner, decision

    def _inject(self, runner, decision, key):
        adapter = None
        return asyncio.run(
            mod.inject_trigger(
                runner,
                make_trigger(decision.target, key=key),
                parent_channel_id="unused-parent",
                confirmation_timeout=0,
                resolved_adapter=decision.adapter,
            )
        )

    def _captured_source(self, runner, decision, key):
        asyncio.run(
            mod.inject_trigger(
                runner,
                make_trigger(decision.target, key=key),
                parent_channel_id="unused-parent",
                confirmation_timeout=0,
                resolved_adapter=decision.adapter,
            )
        )
        events = decision.adapter.events
        assert len(events) == 1, len(events)
        return events[0].source

    def test_all_default_variants_bind_and_match(self):
        for profile in (None, "", "default"):
            with self.subTest(profile=profile):
                entry = self._entry(profile)
                runner, decision = self._resolve([entry])
                self.assertTrue(decision.ready, decision.reason_code)
                self.assertEqual(decision.target.profile, profile)
                self.assertTrue(mod._binding_matches(runner, decision.target))
                self.assertEqual(decision.target.session_key, entry.session_key)

    def test_empty_profile_preserved_end_to_end(self):
        entry = self._entry("")
        runner, decision = self._resolve([entry])
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertEqual(decision.target.profile, "")
        source = self._captured_source(runner, decision, "profile-empty")
        self.assertEqual(source.profile, "")

    def test_explicit_default_profile_preserved_end_to_end(self):
        entry = self._entry("default")
        runner, decision = self._resolve([entry])
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertEqual(decision.target.profile, "default")
        source = self._captured_source(runner, decision, "profile-explicit")
        self.assertEqual(source.profile, "default")

    def test_omitted_profile_preserved_end_to_end(self):
        entry = self._entry(None)
        runner, decision = self._resolve([entry])
        self.assertTrue(decision.ready, decision.reason_code)
        self.assertIsNone(decision.target.profile)
        source = self._captured_source(runner, decision, "profile-none")
        self.assertIsNone(source.profile)

    def test_profile_repr_round_trips_through_receipt(self):
        for profile in (None, "", "default"):
            with self.subTest(profile=profile):
                entry = self._entry(profile)
                runner, decision = self._resolve([entry])
                assert decision.ready
                with __import__("tempfile").TemporaryDirectory() as tmp:
                    dbpath = Path(tmp) / "state.sqlite3"
                    state = mod.StateStore(str(dbpath))
                    state.observe(
                        "board-fixture",
                        "root-fixture",
                        "root",
                        decision.target,
                        _make_live_snapshot_representation(),
                    )
                    quiet = _make_quiet_snapshot_representation()
                    trigger = None
                    for _ in range(2):
                        trigger = state.observe(
                            "board-fixture",
                            "root-fixture",
                            "root",
                            decision.target,
                            quiet,
                        )
                        if trigger is not None:
                            break
                    state.close()
                    assert trigger is not None
                    self.assertEqual(trigger.target.profile, profile)


class ParentExactPreservation(unittest.TestCase):
    """Stored parent_chat_id is carried
    exactly when None or str (including empty and surrounding whitespace,
    byte-for-byte) through WakeTarget, receipt round trip, and the emitted
    MessageEvent.source; non-string values fail closed; a caller/default
    parent never fills absence."""

    def _setup(self, parent_chat_id):
        source = make_source(
            CREATOR_CHAT, thread_id=CREATOR_CHAT, parent_chat_id=parent_chat_id
        )
        entry = Entry(
            "session-parent", session_key_for_source(source), source
        )
        adapter = PropertyAdapter()
        runner = fresh(entries=[entry], adapter=adapter)
        decision = mod.resolve_creator_target(
            runner,
            mod.parse_creator_provenance(provenance(session_key=entry.session_key)),
        )
        return runner, adapter, decision

    def _inject(self, runner, decision, key):
        return asyncio.run(
            mod.inject_trigger(
                runner,
                make_trigger(decision.target, key=key),
                parent_channel_id="caller-parent-must-not-fill",
                confirmation_timeout=0,
                resolved_adapter=decision.adapter,
            )
        )

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
        self.assertEqual(
            adapter.events[0].source.parent_chat_id, "  parent-987  "
        )

    def test_none_parent_stays_none(self):
        runner, adapter, decision = self._setup(None)
        self.assertIsNone(decision.target.parent_chat_id)
        self._inject(runner, decision, "parent-none")
        self.assertEqual(len(adapter.events), 1)
        self.assertIsNone(adapter.events[0].source.parent_chat_id)

    def test_caller_fallback_never_fills_absence(self):
        runner, adapter, decision = self._setup(None)
        self._inject(runner, decision, "parent-no-fill")
        self.assertEqual(len(adapter.events), 1)
        self.assertIsNone(adapter.events[0].source.parent_chat_id)

    def test_nonstring_parent_fails_closed(self):
        source = make_source(CREATOR_CHAT, thread_id=CREATOR_CHAT)
        object.__setattr__(source, "parent_chat_id", 12345)
        entry = Entry("session-parent-invalid", session_key_for_source(source), source)
        runner = fresh(entries=[entry])
        decision = mod.resolve_creator_target(
            runner,
            mod.parse_creator_provenance(provenance(session_key=entry.session_key)),
        )
        self.assertFalse(decision.ready)
        self.assertEqual(decision.reason_code, "HOLD_SESSION_UNRESOLVABLE")

    def test_parent_round_trips_through_receipt(self):
        for parent in (None, "", "  parent-987  "):
            with self.subTest(parent=parent):
                source = make_source(
                    CREATOR_CHAT, thread_id=CREATOR_CHAT, parent_chat_id=parent
                )
                entry = Entry(
                    "session-parent-roundtrip",
                    session_key_for_source(source),
                    source,
                )
                runner = fresh(entries=[entry])
                decision = mod.resolve_creator_target(
                    runner,
                    mod.parse_creator_provenance(
                        provenance(session_key=entry.session_key)
                    ),
                )
                assert decision.ready
                with __import__("tempfile").TemporaryDirectory() as tmp:
                    dbpath = Path(tmp) / "state.sqlite3"
                    state = mod.StateStore(str(dbpath))
                    state.observe(
                        "board-fixture",
                        "root-fixture",
                        "root",
                        decision.target,
                        _make_live_snapshot_representation(),
                    )
                    quiet = _make_quiet_snapshot_representation()
                    trigger = None
                    for _ in range(2):
                        trigger = state.observe(
                            "board-fixture",
                            "root-fixture",
                            "root",
                            decision.target,
                            quiet,
                        )
                        if trigger is not None:
                            break
                    state.close()
                    assert trigger is not None
                    self.assertEqual(trigger.target.parent_chat_id, parent)


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
