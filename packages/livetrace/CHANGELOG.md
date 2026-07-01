# Changelog

All notable changes are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows [SemVer](https://semver.org/spec/v2.0.0.html). This file is maintained by [release-please](https://github.com/googleapis/release-please).

## [0.1.2](https://github.com/Necmttn/livetrace/compare/livetrace-v0.1.1...livetrace-v0.1.2) (2026-07-01)


### Features

* Effect v4 (4.0.0-beta.84) support ([#5](https://github.com/Necmttn/livetrace/issues/5)) ([dbb0e30](https://github.com/Necmttn/livetrace/commit/dbb0e3081547c22bf96e503a0fbaf001e52252f4))
* **site:** lock hero layout + wire Transports section ([8fbd670](https://github.com/Necmttn/livetrace/commit/8fbd6708577ac1aa04ef03d8df8380bf250e0f17))
* **transports:** replace in-memory durable stub with @durable-streams/client transport ([5eb3bfc](https://github.com/Necmttn/livetrace/commit/5eb3bfc2ad318e21283b2982048f4f12cf1c1519))


### Bug Fixes

* **sink:** bound trace buffer + scope cache to prevent unbounded memory growth ([#1](https://github.com/Necmttn/livetrace/issues/1)) ([9ee6f8c](https://github.com/Necmttn/livetrace/commit/9ee6f8c79354611051087ecd2d7be7460878d228))

## 0.1.1

### Fixed

- Bound the `TraceSink` event buffer (default 10,000 events, configurable via `maxBufferEvents`) with oldest-event eviction, preventing unbounded memory growth when a transport is unavailable or slower than producers. Ports [noktadev/quera#1064](https://github.com/noktadev/quera/pull/1064) (RCA #808 - caused `noktaapp-runner` OOM in production).
- Bound the durable-streams trace-scope cache at 10,000 entries with oldest-entry eviction, and forget completed traces even when a send fails.

## 0.1.0

Initial public release. Extracted from internal Quera codebase.

### Added

- `LiveTraceLayer` - Effect Tracer decorator that wraps any base tracer (native or OTel).
- `withTrace` / `step` - user-facing scope helpers.
- `TraceSink` + `TraceSinkLive` - buffered event sink with configurable flush.
- `ConsoleTransportLayer` - debugging transport.
- `SSETransportLayer` + in-process `SseBroker` - server-sent events transport with per-scope routing.
- `liveTraceLogger` - bridges `Effect.log` calls inside traced scopes to `SpanEvent`.
- `livetrace/react` - `TraceStore`, `useActiveTraces`, `useTrace`, `useTraceSteps`, `useSpanTree`.
- `livetrace/types` - dependency-free wire-format types for non-TS/non-Effect backends.
- Effect `Schema` definitions for runtime validation.
