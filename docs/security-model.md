# NexusPulse security model

This document states what each security control does, how it was verified, and
what it does **not** protect against. Where an earlier design claim was wrong,
the correction is recorded here rather than removed.

## 1. Plugin sandbox (`src/plugins/sandbox.ts`)

### Controls

| Control | Mechanism | Verified by |
|---|---|---|
| Process isolation | Each plugin runs in its own child process, so a plugin cannot block the host event loop. | `tests/plugins/test_sandbox.ts` (watchdog and sibling-isolation tests) |
| Heap limit | Child started with `--max-old-space-size=<memoryMb>`. Exceeding it aborts the child; the host reports `PluginMemoryLimitError`. | Memory-limit test, plus a standalone probe that showed SIGABRT with "heap out of memory" |
| Execution watchdog | Host timer per call; on expiry the host sends SIGKILL. | Synchronous `while (true)` test |
| Least-privilege process | Child started with `--permission` and no grants, and with an empty environment. | Three escape regression tests: file read, subprocess spawn, environment secret |
| Capability checks | Every `api.*` call is checked against the manifest on the host. | Capability denial tests |
| Integrity pinning | Optional SHA-256 pin, checked before a process is spawned. | Pin match and mismatch tests |
| Quotas | KV byte quota (exact UTF-8 accounting); bounded log buffer. | Quota tests |

### Corrections to earlier design claims

1. **Worker-thread memory limits were not enforced.** The first design used
   `worker_threads` with `resourceLimits.maxOldGenerationSizeMb`. On Node 22.22,
   a worker reported an 8198 MB heap limit whether 16 MB or 64 MB was requested,
   and a runaway allocation exhausted the host process. Plugins therefore run in
   child processes instead.

2. **The capability layer could be bypassed from inside a plugin.** The first
   version stated that plugins had no ambient authority. That was false. A
   plugin reached the child's real `process` object through
   `api.log.constructor('return process')()` and read `/etc/hostname`. The
   `vm` context is not a security boundary, because host functions handed to a
   plugin carry their realm's `Function` constructor. The fix is containment at
   the process level (control 4 above), not a stronger `vm` context.

### Known limits (not protected by the sandbox)

- **Network access is not restricted.** An escaped plugin can open sockets.
- **Forged IPC is possible.** An escaped plugin can send arbitrary messages to
  the host. The host still checks capabilities on each call, but a plugin with
  escape code can spoof results it returns to callers.
- **The host cannot distinguish a compromised plugin from a healthy one.** The
  sandbox limits blast radius; it does not prove plugin behaviour.

**Deployment requirement:** treat plugin code as trusted-but-fallible unless it
runs inside an OS-level boundary with no outbound network, such as a container
with a network policy, seccomp, or a dedicated unprivileged user.

## 2. Envelope encryption vault (`src/encryption/vault.ts`)

- AES-256-GCM with a 96-bit random IV and a 128-bit tag for both the payload and
  the wrapped data-encryption key (DEK).
- Each `encrypt` call generates a fresh DEK. The DEK is wrapped under the
  current key-encryption key (KEK), and the envelope records the KEK version ID.
- Key rotation adds a new KEK version and re-wraps stored DEKs. Old KEK versions
  stay in process memory so that existing envelopes can still be decrypted.
- Master keys come from PBKDF2-HMAC-SHA512 (minimum 210,000 iterations) or from
  a caller-supplied 32-byte key.

### Gaps against the original specification

- **Argon2 is not implemented.** The specification named PBKDF2 or Argon2. Only
  PBKDF2 is provided.
- **"Secure memory wiping" is best effort.** Buffers are zeroed, but V8 may
  have copied them before that. A passphrase passed as a JavaScript `string`
  cannot be zeroed at all. Callers that need zeroing should pass a `Buffer`.
- **The key store is in process memory.** Nothing is persisted. A process
  restart loses every key, so any production deployment needs an external KMS
  or HSM for KEK custody.
- **Old KEK versions are never retired.** Rotation keeps them indefinitely. A
  retirement policy is needed before production use.

## 3. Transport (`src/grpc/transport.ts`)

- The module implements a custom binary framing protocol. **It is not gRPC and
  is not HTTP/2-compatible.** The file name is historical.
- The protocol has **no encryption and no authentication**. It must run over a
  TLS connection (or a mutually authenticated tunnel) in any deployment.
- The unit tests run against in-memory duplex streams. There is no end-to-end
  test over a real TCP socket yet.
- Frames are capped at 4 MiB, and a parser rejects bad magic values and
  oversized length fields.

## 4. Reporting a vulnerability

Do not open a public issue for a suspected sandbox escape or key-handling flaw.
Report it privately to the repository owner, and include a reproduction. The
escape probe described in section 1 is a good template for a reproduction.
