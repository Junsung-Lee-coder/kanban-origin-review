# kanban-origin-review

A Hermes Agent plugin that watches Kanban root families. After a family has been active and then remains heartbeat-quiescent for two observations, the plugin records a durable receipt and injects one bounded internal review turn into the root creator's existing Discord session.

> **Compatibility:** this plugin uses Hermes host internals. Pin installs to an immutable commit and validate that commit against your Hermes version before enabling it.

## Behavior

- Reads Kanban databases through isolated, query-only SQLite snapshots.
- Requires an exact creator-session provenance block in the root task body.
- Fails closed when provenance, the current session, or the Discord adapter cannot be proven.
- Records receipt state before dispatch, deduplicates review turns, and confirms delivery from durable session history.
- Does not expose model-callable tools.

The plugin uses Hermes host internals and currently targets Linux/POSIX environments. Python 3.10 or newer is required.

## Install

Install from GitHub, preferably pinned to its full immutable commit SHA:

```bash
hermes plugins install Junsung-Lee-coder/kanban-origin-review --ref <40-character-commit-sha> --no-enable
hermes plugins list
hermes plugins enable kanban-origin-review
```

Plugins execute local Python code. Review the pinned source before enabling it. Installation and enablement are separate operations.

## Configuration

Optional environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `HERMES_KANBAN_ORIGIN_REVIEW_POLL_SECONDS` | `180` | Watch interval in seconds; values are clamped to at least one second. |
| `HERMES_KANBAN_ORIGIN_REVIEW_STATE_DB` | Hermes state directory | SQLite receipt and observation state. |
| `HERMES_KANBAN_ORIGIN_REVIEW_PARENT_CHANNEL_ID` | empty | Legacy fallback parent-channel value; exact creator-session delivery preserves the stored session source. |
| `HERMES_KANBAN_ORIGIN_REVIEW_ROUTE_STATE` | Hermes state directory | Legacy route-state path; it is not targeting authority for creator provenance. |

Keep the state database private. It can contain task and session metadata and must not be committed or published.

## Root provenance

Put exactly one tagged JSON block in each root task body. Use one or more selectors that all identify the same stored Discord session, and remove unused selector keys.

```text
[creator-session-provenance/v1]
{"profile":"default","source":"discord","session_id":"<session-id>"}
[/creator-session-provenance/v1]
```

Supported optional selectors are `session_id`, `session_key`, `chat_id`, `thread_id`, and `conversation_id`. Values are exact and case-sensitive. `conversation_id` is parsed but is not resolvable by this build. Duplicate tags, duplicate JSON keys, unknown keys, empty values, or contradictory selectors hold delivery without injection.

## Test

The tests install local shims for Hermes Gateway types and never open live Hermes, Kanban, plugin, or session state.

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -B -m unittest discover -s tests -p 'test_*.py'
```

## License

MIT
