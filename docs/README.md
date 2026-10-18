# NexusPulse documentation

NexusPulse is a real-time telemetry and anomaly detection platform written in
TypeScript. This directory holds the design and operating documents.

| Document | Purpose |
|---|---|
| [architecture.md](architecture.md) | Module map, planned data flow and design decisions |
| [security-model.md](security-model.md) | What each security control protects against, verified findings, and known limits |

## Requirements

- **Node.js 22.12 or later.** The plugin sandbox relies on the `--permission`
  flag and `process.getBuiltinModule`. The test suite has been verified on
  Node 22.22 and Node 24.16.
- npm, with the committed `package-lock.json`.

## Commands

```bash
npm ci                          # install exact dependencies from the lockfile
npm run typecheck               # strict TypeScript check (no emit)
npm test                        # run the full Vitest suite once
npm run test:coverage           # same, with V8 coverage in coverage/
npm run test:watch              # re-run tests on change
npm run token -- help           # API token helper (see below)
```

Continuous integration (`.github/workflows/ci.yml`) runs `npm ci`, the
type-check and the coverage run on every push to `main` and every pull request.

### API tokens

The HTTP API can require bearer tokens. Create a token file with mode 0600, then
check it:

```bash
npm run token -- new ./tokens.json ci-reader=reader ops-writer=writer
npm run token -- check ./tokens.json
```

`new` never prints secrets. It refuses to overwrite an existing file. The
server reads the file when `NEXUSPULSE_TOKENS_FILE` points at it, and refuses to
start if the file is readable by group or others.

## Status

| Module | Source | Tests | Notes |
|---|---|---|---|
| Stream engine | `src/core/engine.ts` | `tests/test_engine.ts` | Self-contained |
| Alert rules | `src/core/rules.ts` | `tests/core/test_rules.ts` | Evaluated for TCP and HTTP events; cooldown per rule and source |
| Envelope vault | `src/encryption/vault.ts` | `tests/encryption/` | PBKDF2 only; no Argon2 |
| Framed transport | `src/grpc/transport.ts` | `tests/grpc/` | Custom protocol, not gRPC; no TLS; no real-socket test |
| Plugin sandbox | `src/plugins/sandbox.ts` | `tests/plugins/` | Child processes; network not restricted |
| Analytics | `src/analytics/metrics_engine.ts` | `tests/analytics/` | Self-contained |
| Logger | `src/utils/logger.ts` | `tests/utils/test_logger.ts` | |
| Resilience | `src/utils/resilience.ts` | `tests/utils/` | |
| Schema | `src/db/schema.ts` | `tests/db/test_schema.ts` | |
| CRC-32 | `src/db/crc32.ts` | `tests/db/test_crc32.ts` | Checked against `zlib.crc32` |
| Record store | `src/db/record_store.ts` | `tests/db/test_storage.ts` | Append-only log; no compaction under concurrent writers |
| HTTP router | `src/api/http_router.ts` | `tests/api/test_router.ts` | Own `node:http` server; no TLS |
| Telemetry API | `src/api/telemetry_api.ts` | `tests/api/test_telemetry_api.ts`, `tests/api/test_telemetry_auth.ts` | `/healthz` public; `/v1/*` guarded when auth is on |
| API auth | `src/api/auth.ts` | `tests/api/test_auth.ts` | Bearer tokens, two roles; no token rotation |
| Config manager | `src/config/manager.ts` | `tests/config/test_config.ts` | |
| TCP ingest | `src/ingest/tcp_ingest.ts` | `tests/ingest/test_tcp_ingest.ts` | No authentication; no TLS |
| App layer | `src/app/` | `tests/app/` | Settings, server lifecycle, token file, rule evaluation over HTTP |
| Token CLI | `src/cli/` | `tests/cli/test_token_cli.ts` | Entry point: `npm run token` |
| Alert notifier | `src/alerts/notifier.ts` | `tests/alerts/test_notifier.ts` | In-process queue; at-most-once delivery |
| Metrics export | `src/observability/prometheus.ts` | `tests/observability/test_prometheus.ts` | Prometheus text format; served at `/metrics` |
| Worker queue | `src/queue/worker.ts` | `tests/test_worker.ts` | Dispatch on a microtask so bursts are priority-sorted |
| Consensus | `src/mesh/consensus.ts`, `src/mesh/driver.ts` | `tests/mesh/test_election.ts`, `test_replication.ts`, `test_driver.ts`, `test_consensus.ts` | Raft election, replication and commit rules; leader barrier |
| Mesh transport | `src/mesh/peer_transport.ts` | `tests/mesh/test_peer_transport.ts` | Length-prefixed JSON over TCP, every frame signed with the cluster secret; no TLS |
| Replicated store | `src/mesh/state_machine.ts`, `src/mesh/snapshot.ts`, `src/mesh/snapshot_chunks.ts` | `tests/mesh/test_state_machine.ts`, `test_snapshots.ts`, `test_snapshot_chunks.ts`, `test_snapshot_transfer.ts`, `test_snapshot_cluster.ts`, `test_replicated_state.ts` | Key-value store; compaction and chunked snapshot transfer |
| Durable state | `src/mesh/durable_store.ts` | `tests/mesh/test_durable_store.ts`, `test_durable_restart.ts` | Atomic `state.json` in `dataDir`; fail-stop on write errors |
| Node runner | `src/mesh/node_runner.ts`, `src/mesh/cli.ts`, `src/mesh/node_config.ts` | `tests/mesh/test_node_runner.ts`, `test_node_config.ts` | One member from a JSON config |
| Key-value API | `src/mesh/kv_api.ts`, `src/mesh/status.ts` | `tests/mesh/test_kv_api.ts`, `test_status.ts` | `/v1/kv/:key` and `/v1/status`; no authentication |

**Not yet built:** deployment guide; TLS for the TCP ingest port and mesh peers;
authentication for the TCP ingest port; Argon2; a `RouterEngine` or
`TimeSeriesStorageEngine` (design not yet agreed).
