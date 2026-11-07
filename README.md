# tiny-ringbuf

A fixed-capacity ring buffer for one producer and one consumer, in about 200
lines of safe-ish Rust. No allocation after construction, no mutex, no wait
queue: a full buffer hands the value back and the caller decides how to back
off.

## Why

Moving small payloads (log records, telemetry frames, parsed packets) between a
poll loop and a worker is the one place where a `Mutex<VecDeque>` is both too
slow and too willing to block. An SPSC queue makes the ownership rule explicit
and the fast path a load, a store and two counter bumps.

The trade-off is real: exactly one thread may push and exactly one may pop. If
that does not hold, use `crossbeam-channel` or `tokio::sync::mpsc`.

## Invariants

* Capacity is a power of two and fixed at construction. Slot lookup is
  `index & mask`, so indices wrap without a modulo.
* `head` only moves in `pop`, `tail` only moves in `push`. Both are monotonic
  `usize` counters; the live length is `tail - head` with wrapping arithmetic.
* `tail - head <= capacity` means the producer can test fullness without a
  CAS and the slot at `tail` is provably empty.
* A slot is written before `tail` is published with `Release`; the consumer
  reads it only after an `Acquire` load of `tail`. Slot reuse follows the same
  rule for `head`.

The counters sit on separate cache lines. `T: Send` is required for `Sync`; the
buffer itself is not `Clone` and neither are the counters.

## Failure modes

* **Two producers or two consumers.** Counters desync; items are lost or read
  twice. Nothing detects it at runtime, because a CAS would cost the property
  that makes this worth having.
* **Dropping while borrowed elsewhere.** Counters are stable during `Drop`, but
  only if every thread that touched the buffer has been joined.
* **Spin-only backoff on a constrained core count.** A pure spin loop starves
  the other side; the binaries yield after 64 failed attempts.
* **Large `T`.** Slots are `MaybeUninit<T>`, so a 4 KiB frame in a 64 KiB queue
  occupies the full 64 KiB up front. Size the capacity accordingly.
* **`Drop` cost.** Dropping the queue drains it, so pending values run their
  destructors. An overloaded queue can make shutdown slow.

## Install

No dependencies; edition 2021.

```sh
cargo add tiny-ringbuf
```

Vendoring works too: `src/lib.rs` has no crate-internal imports.

## Usage

```rust
use std::sync::Arc;
use tiny_ringbuf::RingBuffer;

let queue = Arc::new(RingBuffer::with_capacity(256));

queue.push(7).expect("empty queue accepts");
assert_eq!(queue.pop(), Some(7));
assert_eq!(queue.pop(), None);
```

A full queue returns the value instead of blocking:

```rust
use tiny_ringbuf::RingBuffer;
let queue = RingBuffer::with_capacity(1);
assert!(queue.push(1).is_ok());
assert_eq!(queue.push(2), Err(2));
```

`drain_with` consumes the remainder in order, and `Debug` prints capacity and
length without requiring `T: Debug`.

## Benchmarks

```sh
cargo run --release --bin bench -- 64 256 1024 4096
```

Both sides spin for 64 iterations, then `yield_now`. On one vCPU, `--release`:
~25 Mmsg/s at capacity 64, ~77 at 256, ~163 at 1024 and ~219 at 4096. Small
capacities spend their time in the retry loop; large ones stop fitting in L2
and gain less than the extra memory costs. Without the yields the same box
collapses to under 1 Mmsg/s because the spinning side never gives up the core,
so the policy matters more than the capacity. Re-measure before quoting.

## Layout

```
src/lib.rs        queue, invariants, Drop
src/main.rs       producer/consumer demo, prints throughput
src/bin/bench.rs  capacity sweep
tests/ring.rs     full/empty transitions and slot reuse
```

## License

MIT, see `LICENSE`.
