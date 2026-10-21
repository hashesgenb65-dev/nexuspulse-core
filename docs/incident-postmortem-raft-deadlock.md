# Postmortem: INC-2026-0814, Raft quorum loss from blocking snapshot I/O

**Status:** Draft. Items marked `[CONFIRM]` come from the incident logs and have not been checked against the repository.
**Severity:** Critical (P1)
**Date:** 14 October 2026

## 1. Incident details

**Title:** Distributed Raft consensus quorum loss via blocking state snapshot I/O.

**Summary.** Under peak load, the primary node took a state machine snapshot with synchronous file I/O. The write blocked the single-threaded event loop, so outbound heartbeats were not sent, and the cluster lost quorum.

**Observed conditions.** Request rate about 45,000 per second `[CONFIRM: source of figure]`. The event loop was blocked for about 482 ms `[CONFIRM: measured by what, and where in the logs]`.

**Impact.** `[CONFIRM: duration of the quorum loss, leadership changes, requests failed or rejected, data affected]`

**Timeline.** `[CONFIRM: timestamps for snapshot start, first missed heartbeat, election start, quorum lost, quorum restored]`

## 2. Verification and testing

The regression test is `tests/core/raft_snapshot_deadlock.test.ts`. It runs the real `SnapshotManager` (`src/core/snapshot.ts`) against 50,000 entries and checks three things:

1. The output has a header line and every entry, in order, and no temporary file is left behind.
2. Timers fire while a large snapshot is being written. The check counts 1 ms timer ticks during the call and requires at least one.
3. A control: the legacy blocking writer, run through the same timing check, fires no timers. This shows the timing check can detect the failure.

A fourth test checks that a failure partway through removes the temporary file and rethrows the error.

The earlier version of this test, on `main` at `acd8c0c`, only counted to 10,500 in a loop and never called the snapshot code. It could not have caught the incident, and it has been replaced.

Results on the fixed code: all four tests pass across repeated runs. The full suite passes: 56 files, 1,291 tests.

`[CONFIRM: whether the 482 ms block reproduces against the legacy code on the same machine]`

## 3. Root cause

Under load, the primary wrote its state machine snapshot with synchronous file I/O. The event loop was blocked for the duration of the write, so the node could not send heartbeats to its followers. The followers' election timers expired, the cluster lost its leader, and no quorum could be formed while the write blocked the loop.

The legacy implementation is not in the repository. `[CONFIRM: the commit or file that contained the synchronous write, if it is available]`

A second weakness was found in review of the replacement. The first version of `SnapshotManager` wrote through a stream and ignored backpressure, so when the disk was slower than the loop, the entire serialized snapshot could build up in memory. It also attached no error listener, so a disk error during the write could crash the process. Both are fixed in section 4.

## 4. Technical fix

`src/core/snapshot.ts` now writes the snapshot as follows.

- It writes through a file handle and awaits each batch. There is no stream, so there is no unbounded queue and no unhandled `error` event.
- It writes in batches of 1,000 entries, and yields to the event loop between batches with `setImmediate`.
- It flushes the temporary file to disk with `fsync` before renaming it to `snapshot-<index>.bin`. The rename then makes the snapshot visible under its final name only once its contents are on disk.
- It flushes the directory after the rename, so the rename itself survives a crash.
- If any step fails, it closes the handle, removes the temporary file, and rethrows the error.

The yielding bounds how long the loop is blocked by one batch. It does not make the snapshot cheaper to produce, so very large state machines still take time, but they no longer hold the loop for the whole write.

The consensus persistence path in `src/mesh/durable_store.ts` also writes with synchronous calls (`writeSync`, `fsyncSync`) before the driver replies to a peer. That is a separate code path, and it is not changed by this fix. It is the next thing to look at if heartbeat delays show up again.

## 5. Follow-ups and open items

- Confirm the figures in section 1 and the timeline from the logs, and remove every `[CONFIRM]` marker before publishing.
- Confirm that `src/core/snapshot.ts` is wired into the production snapshot path. It is not referenced anywhere else in the repository at present.
- Measure the event-loop delay of the mesh durable write (`src/mesh/durable_store.ts`) under a large state, and move it to asynchronous I/O if it blocks for long.
- Add an alert on event-loop delay, so a stall shows up before quorum is affected.
