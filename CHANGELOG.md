# Changelog

All notable changes to this project are documented here.

## 0.2.0

- Route quiescent root-family review turns only to the creator session proven by a strict `[creator-session-provenance/v1]` block.
- Resolve thread-only claims to exactly one canonical stored session and fail closed on missing, stale, contradictory, or ambiguous selectors.
- Preserve stored session-source identity fields when dispatching the internal event.
- Validate the retained adapter and connection state immediately before dispatch.
- Keep pending receipts confirmation-only and prevent durable markers from confirming a mismatched destination.
- Add isolated regression coverage for exact-session routing, ambiguous provenance, adapter failures, receipt deduplication, and concurrent session isolation.
