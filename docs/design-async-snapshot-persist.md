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

- **The image copy is still synchronous (item 1).** `store.image()` must be measured. If it is large
  enough to matter, the copy itself needs to be batched, or the store must be iterated lazily.
  This is a separate measurement step, not something the deferred trim fixes.
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

Build the deferred trim in three commits, each with its tests:

1. Background snapshot writer used by `StateHost`, with the image copy measured and reported.
2. Pending-compaction state and the trim commit, with the crash-state tests.
3. Cluster test for a follower catching up during a pending compaction.

Decisions needed from you: the maximum time a compaction may stay pending, and whether the image
copy should be batched in this stage or measured first.
