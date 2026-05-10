# Scaling Controls in TUI / GUI

Backend support for autonomous scaling is in place. The user-facing control surface is intentionally deferred.

## Current State

- CLI/headless can already pass:
  - `connections`
  - `min_connections`
  - `max_connections`
  - `max_total_connections`
  - `bandwidth_limit`
  - `per_download_limit`
- Engine/scaler backend applies those values at task creation time.
- Runtime scaling works without requiring TUI or GUI support yet.

## Deferred UI Work

- Add TUI controls for per-download:
  - min connections
  - max connections
  - per-download bandwidth cap
- Add TUI/global controls for:
  - max total connections
  - global bandwidth cap
- Show current live scaler state in UI:
  - active connections
  - scaler action (`grow`, `shrink`, `hold`)
  - EWMA throughput
  - current quota / throttle state
- Add validation in UI before submission:
  - `min_connections <= max_connections`
  - non-zero sensible limits
- Add a future command path for live mutation of scaler config after a task has already started.

## Important Note

For now, the backend is the priority and the UI is intentionally not the source of truth for scaling. TUI/GUI should be added only after the backend behavior is benchmarked and considered stable.
