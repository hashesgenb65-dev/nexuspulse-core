# Postmortem: INC-2026-0814, Raft quorum loss from blocking snapshot I/O

**Status:** Final for review
**Severity:** Critical (P1)
**Date:** 14 October 2026

## 1. Incident details

**Title:** Raft quorum loss linked to blocking state snapshot I/O.

**Summary.** The node's single-threaded event loop was blocked by snapshot writes. A blocked loop cannot send heartbeats to followers. The incident report links this to the loss of quorum. The fix in this repository removes the blocking behaviour from the snapshot write path.

**Scope of this document.** This repository does not contain the logs, metrics or code that ran during the incident. The request rate, the length of the event-loop block, the timeline and the impact are therefore not stated here. They belong in the incident record.

## 2. Verification and testing

The regression test is `tests/core/raft_snapshot_deadlock.test.ts`. It runs the real `SnapshotManager` (`src/core/snapshot.ts`) with 50,000 entries and checks four things:

1. The output has a header line followed by every entry, in order, and no temporary file is left behind.
2. A 1 ms timer fires at least once while the snapshot is being written. This shows the event loop runs during the write.
3. A control runs a blocking writer (`fs.writeFileSync`) through the same timing check and expects zero timer ticks. This shows the check can detect a blocked loop. The control is a stand-in for the blocking approach. It is not the original incident code.
4. When writing fails partway, the temporary file is removed and the error is rethrown.

The test that was on `main` at `acd8c0c` counted to 10,500 in a loop and did not call any snapshot code. It did not test the fix and has been replaced.

The full suite passes in CI on commit `8d42bfc`.

## 3. Root cause

The incident report attributes the quorum loss to the event loop being blocked during a synchronous snapshot write, which stopped heartbeats from being sent. The repository does not contain the code that ran during the incident, so this repository does not verify that link. The fix below addresses the blocking behaviour directly.

## 4. Technical fix

`src/core/snapshot.ts` writes the snapshot as follows.

- It writes through a file handle and awaits each write. It does not use a stream, so no unbounded queue builds up in memory.
- It writes entries in batches of 1,000 (`SNAPSHOT_BATCH_ENTRIES`) and yields to the event loop with `setImmediate` after each batch.
- It calls `fsync` on the temporary file before renaming it to `snapshot-<index>.bin`, so the final name only refers to a fully flushed file.
- It calls `fsync` on the directory after the rename.
- If any step fails, it closes the handle, removes the temporary file, and rethrows the error.

Yielding limits how long the loop is blocked by a single batch. It does not reduce the total cost of producing a snapshot, so a large state machine still takes time to write.

`src/core/snapshot.ts` is not yet imported by any production code. The mesh's snapshot path does not use it.

The consensus persistence path in `src/mesh/durable_store.ts` still uses synchronous calls (`fs.writeSync` and `fs.fsyncSync`). These run before the node replies to a peer. This change does not touch that path.

## 5. Follow-ups

- Wire `SnapshotManager` into the production snapshot path, then add a test that exercises the mesh path.
- Measure event-loop delay during durable writes in `src/mesh/durable_store.ts` with a large state, and move those writes to asynchronous I/O if they block for long.
- Add an alert on event-loop delay, so a stall is visible before it affects quorum.
- Record the incident's measured figures and timeline in the incident record, and link that record from this document.
