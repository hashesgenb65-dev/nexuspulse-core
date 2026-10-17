# Mesh node runner and key-value API

One mesh member runs from a JSON config file and serves two things: peer RPCs for consensus, and an HTTP key-value API for clients.

## Config

```json
{
  "nodeId": "node-1",
  "rpc":  { "host": "10.0.0.1", "port": 7001 },
  "http": { "host": "10.0.0.1", "port": 8001 },
  "peers": [
    { "id": "node-2", "host": "10.0.0.2", "rpcPort": 7001, "httpPort": 8001 },
    { "id": "node-3", "host": "10.0.0.3", "rpcPort": 7001, "httpPort": 8001 }
  ],
  "electionTimeoutMs": 1000,
  "heartbeatIntervalMs": 100,
  "requestTimeoutMs": 500
}
```

- `nodeId`: 1 to 63 lowercase letters, digits or hyphens. Unique in the cluster.
- `rpc`, `http`: where this node listens. A port of `0` picks a free port.
- `peers`: every other member, with its RPC and HTTP ports. At most 32. Peer ports must be real ports.
- Timing fields are optional. Each must be an integer from 1 to 60000. `heartbeatIntervalMs` must be shorter than `electionTimeoutMs`.
- Every problem in the file is reported together, with the field path.

## Running

`src/mesh/cli.ts` is the entry point. Compile it or run it with your existing TypeScript runner, then:

```
node-runner node-1.json
```

It exits with 2 for a bad config and 1 if it cannot bind its ports. On SIGINT or SIGTERM it stops cleanly and exits with 0.

## HTTP API

| Method | Path | Behaviour |
|---|---|---|
| GET | `/v1/kv/:key` | Reads the value this node has applied. Returns `appliedIndex`. A follower may lag the leader by a few entries. |
| PUT | `/v1/kv/:key` | Body `{"value": <any JSON>}`. Replicated SET. |
| DELETE | `/v1/kv/:key` | Replicated DELETE. Deleting a missing key succeeds. |

Keys are 1 to 256 characters after URL decoding. Bodies are limited to 64 KiB.

### Status

`GET /v1/status` reports this member's view of the cluster: role, term, leader, vote, log and commit indexes, applied index, snapshot index, peers, quorum size, and whether state is durable. It reads local state only and does not contact peers, so a stale or partitioned member reports what it last knew.

Writes go through the leader:

- A follower answers **307** with the leader's address in `location`. Repeat the same request there.
- A write answers **200** only after it commits and applies on the node that received it.
- **409**: a later leader replaced the write before it committed. It was not applied. Retry.
- **504**: the write did not commit within 2 seconds. The outcome is unknown.
- **503**: no leader is known yet. Retry after the `retry-after` delay.

## Snapshots and compaction

Once a node has applied `compactThreshold` entries above its last snapshot (default 1000), it folds the store into a snapshot and drops those entries from its log. The leader keeps its latest snapshot. A peer whose next needed entry was dropped receives that snapshot in an `installSnapshot` RPC, then continues with normal replication.

- The threshold is set per process with `startMeshNode(config, { compactThreshold })`. It is not part of the config file yet.
- The snapshot must fit in one peer frame (1 MiB by default). A store with a large value set can exceed that. The leader then fails to send the snapshot and reports the error. Raising the frame limit or chunking the snapshot is the fix if that happens.
- Snapshots are saved to the state file too (see Durable state below).

## Peer authentication

A member with peers must set `peerSecretFile` in its config. The file holds a cluster secret as hex text, at least 32 bytes, and must not be readable by group or others (`chmod 600`). Every member must use the same secret.

Each frame between members carries an HMAC-SHA256 tag over its whole contents, plus a timestamp. A frame without a valid tag, or with a timestamp more than 30 seconds from the receiver's clock, is refused. A server drops the connection on a bad request; a client fails the request with `AUTH_FAILED` on a bad reply.

- Keep the members' clocks within a few seconds of each other (NTP), or valid frames will be refused as stale.
- A captured frame can be replayed within the 30-second window. Raft tolerates this, but it is a reason to keep the window short.
- Peer traffic is still plain TCP. The tags stop forgery and tampering, not eavesdropping. Use a private network or a tunnel until TLS is added.
- The secret authenticates members to one another. It does not protect the HTTP API, which has no authentication.

## Durable state

Set `dataDir` in the config to keep the node's term, vote, log and latest snapshot in `<dataDir>/state.json`. The file is rewritten atomically after every change, and always before the node answers a peer. A restarted node reads it back and rejoins the cluster with its term intact, so it cannot vote twice in one term.

- Without `dataDir` the node keeps nothing across restarts and catches up from the leader instead.
- A corrupt state file stops start-up before any port is bound. Fix or remove the file to start fresh; removing it means the node may vote again in a term it already voted in, so only do that when the rest of the cluster is healthy.
- If a write fails (for example, the disk is full), the node stops answering peers and must be restarted.

## Leader barrier

A new leader appends a `BARRIER` entry before it accepts writes. It changes nothing in the store, but committing it commits every entry from earlier terms. Without it, entries from an earlier term could stay uncommitted after a failover until the next write.

Because the barrier takes index 1 in a fresh cluster, the first client write is index 2.

## Not included

- **No authentication** on the HTTP API or the peer RPCs. Bind both to a trusted network only.
- **Reads are not linearizable.** A GET on a follower can return an older value.
