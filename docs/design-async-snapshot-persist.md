# Design: non-blocking snapshot persistence

Status: proposal, for review. No code has changed.

## Problem

Snapshot persistence blocks the event loop, which is the failure mode in
`docs/incident-postmortem-raft-deadlock.md`. A blocked loop cannot send heartbeats, so a leader can
lose its followers. The blocking work has three parts:

1. **In-memory copy.** `StateHost.maybeCompact` calls `store.image()`, which copies the whole key-value
   map synchronously. This happens inside `applyCommitted`, so it runs during a request.
2. **Durable snapshot write.** `durable_store.ts` serializes the snapshot and writes it with
   `fs.writeSync` plus `fs.fsyncSync`. The driver calls this before every reply to a peer, so the
   write sits on the reply path.
3. **State file rewrite.** `state.json` is rewritten after every change to the term, vote or log.
   It is small, so it is not the main cost, but it must stay synchronous with replies.

`src/core/snapshot.ts` (`SnapshotManager`) already writes in batches and yields between them. It is
not used by the mesh. The mesh has its own snapshot file format, so the two must not both write
snapshots.

## Current ordering rules (must hold after the change)

From `durable_store.ts` and `driver.ts`:

- R1. `state.json` may refer to snapshot N only after `snapshot-N.json` is on disk.
- R2. Older snapshot files are removed only after `state.json` refers to a newer one.
- R3. The driver saves state before it sends any reply to a peer. A reply never gets ahead of the
  state behind it.
- R4. A failed save stops the driver for good (`persistState`). The node must not answer after its
  memory may be ahead of its disk.

R3 is the constraint. If the snapshot write becomes asynchronous, a reply must still not go out
before the state it depends on is durable.

## Key observation

A snapshot is needed only so the log can be trimmed. The log in `state.json` already holds every
entry the snapshot covers, until it is trimmed. So the trim can wait for the snapshot without
weakening R3:

- While a snapshot write is in flight, `state.json` keeps the full log. It is safe to reply, because
  everything needed to replay is on disk.
- Once `snapshot-N.json` is durable, the node saves `state.json` with the trimmed log, which now
  refers to N (R1 holds).

This avoids making the reply path async. Replies stay synchronous and only the disk trim is deferred.

## Proposed design (option 1: deferred trim)

1. **Snapshot without blocking.** On compaction, `StateHost` takes the image and hands it to a
   background writer. The writer writes `snapshot-N.tmp` in batches, yielding with `setImmediate`
   between batches, then fsyncs and renames it to `snapshot-N.json`. This follows `SnapshotManager`.
2. **Pending compaction.** The node records "compaction to N pending". It does not call
   `node.compactTo(N)` yet. The in-memory log is kept whole, and `state.json` is saved with the full log.
3. **Commit the trim.** When the writer finishes, the host calls `node.compactTo(N)` and saves
   `state.json` (R1 holds). Older snapshot files are removed after that save (R2).
4. **Failure.** A failed snapshot write removes its temp file and leaves the pending compaction
   cancelled. The log stays whole and nothing is lost. The error is reported through `onError`. The
   node keeps running and retries at the next threshold. This is not the R4 case: state is still
   complete on disk.
5. **Replies.** Unchanged. The driver still saves before replying (R3, R4).

The leader's in-memory `latestSnapshot` is still used for `InstallSnapshot`. It is set when the
image is taken, not when the file is written, so a peer can be sent a snapshot that is not yet on disk.
That is acceptable, because the leader's memory is correct. A crash only affects what the leader
can reload after restart, and the file is then missing, so the node falls back to the log.

## Open problems to decide before coding

- **The image copy is the largest blocker, and the deferred trim does not fix it.** Measured on this
  workspace (Node 22, 100-byte values, median of 3 runs; scripts are outside the repo):

  | Entries | `store.image()` deep copy | `JSON.stringify` of the image | Shallow copy of map references |
  |---|---|---|---|
  | 10,000 | 16.8 ms | 19 ms | not measured |
  | 100,000 | 178.7 ms | 306 ms | 16.4 ms |
  | 1,000,000 | 2,420 ms | 3,808 ms | 210.5 ms |

  At 1M entries the deep copy alone is longer than the default 1,000 ms election timeout. The
  serialized image is about 129 MB.

  `apply` replaces a stored value (`structuredClone` on write) rather than mutating it, so a copy
  of the map's references is a stable point-in-time view. Serializing from those references later is
  safe, and `image()` can stop deep-copying on the loop. The remaining reference copy is still
  synchronous, at 210 ms for 1M entries, so it should be batched or measured again at real sizes.
- **Log growth while a write is pending.** Under a slow disk the log grows until the write finishes.
  A bound is needed, either a maximum pending time or a rule that defers a second compaction until
  the first finishes.
- **Crash during the trim.** The crash cases must be listed and each tested. For example: snapshot
  written but `state.json` not yet saved; `state.json` saved and the old snapshot file still present.
  The design says both should load, but this must be proven with tests.
- **Leader after a restart with a pending snapshot.** On restart the node sees the full log and no
  pending snapshot. It should simply compact again when the threshold is met.

## Test plan

- Unit: pending compaction keeps the full log in `state.json`; committing the trim shrinks it.
- Ordering: a `state.json` on disk never refers to a snapshot file that is missing (R1), checked after
  each step with the file system in a simulated crash state.
- Event loop: the existing timing tests (`tests/core/raft_snapshot_deadlock.test.ts`) run against the
  mesh path, with a blocking-writer control.
- Three-node cluster: a follower that falls behind during a pending compaction still catches up.

## Recommendation

Build it in four commits, each with its tests:

0. Replace the deep copy in `store.image()` with a copy of map references, and keep the
   serialization from those references. Add a test that a value applied after the copy is not in
   the copy. This is the largest single improvement (2.4 s to 210 ms at 1M entries).
1. Background snapshot writer used by `StateHost`, writing in batches with yields.
2. Pending-compaction state and the trim commit, with the crash-state tests.
3. Cluster test for a follower catching up during a pending compaction.

Decisions still needed from you:

- **Maximum time a compaction may stay pending.** Proposed default: 10 seconds, after which the
  node logs a warning and keeps the log whole. It stays configurable.
- **Batching the reference copy (stage 0).** At 1M entries it still takes 210 ms on the loop, so
  either batch it or accept that cost for now. I suggest accepting it for stage 0 and measuring
  again at real sizes before stage 1.
