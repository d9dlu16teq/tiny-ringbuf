//! A fixed-capacity, allocation-free single-producer/single-consumer ring buffer.
//!
//! Used to hand buffers between a poll loop and a worker without a mutex and
//! without touching the allocator. The capacity is chosen at construction and
//! never changes.
//!
//! # Invariants
//!
//! 1. `capacity` is a power of two, so slot lookup is a mask instead of a
//!    modulo and indices can wrap freely.
//! 2. `head` advances only in [`RingBuffer::pop`], `tail` only in
//!    [`RingBuffer::push`]. Both counters are monotonic; the live length is
//!    `tail - head` (wrapping).
//! 3. `tail - head <= capacity` always holds, so a full check needs no CAS and
//!    a slot is free exactly when the producer is allowed to write it.
//! 4. The producer writes the slot *before* publishing `tail` with `Release`;
//!    the consumer reads the slot only after an `Acquire` load of `tail`. The
//!    same holds for `head` and slot reuse.
//!
//! # Failure modes
//!
//! * Two producers or two consumers at once: counters desync, values are lost
//!   or read twice. The type only checks this with `debug_assert`s where it is
//!   cheap; split ownership yourself.
//! * Dropping the buffer while a producer or consumer still holds `&self` is
//!   not allowed. Join the threads first.
//! * A full buffer rejects the value instead of blocking. Backoff policy
//!   belongs to the caller; see `src/main.rs` for a spin/yield loop.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Keeps the producer and consumer counters on separate cache lines.
#[repr(align(64))]
struct Padded<T>(T);

/// A bounded SPSC queue.
pub struct RingBuffer<T> {
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    head: Padded<AtomicUsize>,
    tail: Padded<AtomicUsize>,
}

// SAFETY: the slot payload is only reachable through `push`/`pop`, which take
// `&self`; slot access is ordered by the counter publication in invariant 4.
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// Creates a queue holding at most `capacity` values.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is not a power of two or is zero.
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(
            capacity.is_power_of_two(),
            "capacity must be a power of two, got {capacity}"
        );
        let slots = (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            slots,
            mask: capacity - 1,
            head: Padded(AtomicUsize::new(0)),
            tail: Padded(AtomicUsize::new(0)),
        }
    }

    /// Number of values the queue can hold.
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// Number of values currently queued. Exact for the consumer, a lower
    /// bound for the producer.
    pub fn len(&self) -> usize {
        self.tail
            .0
            .load(Ordering::Acquire)
            .wrapping_sub(self.head.0.load(Ordering::Acquire))
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    /// Enqueues `value`, handing it back if the queue is full.
    ///
    /// Never allocates and never blocks. Only the producer may call this.
    pub fn push(&self, value: T) -> Result<(), T> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Acquire);
        if tail.wrapping_sub(head) > self.mask {
            return Err(value);
        }
        let slot = &self.slots[tail & self.mask];
        // SAFETY: the length check proved the slot empty, and only the
        // producer writes it before publishing `tail`.
        unsafe { (*slot.get()).write(value) };
        self.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Dequeues the oldest value. Only the consumer may call this.
    pub fn pop(&self) -> Option<T> {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let slot = &self.slots[head & self.mask];
        // SAFETY: `head != tail` means the producer published this slot, and
        // the `Acquire` load ordered its contents before this read. Handing
        // the slot back happens after `head` is bumped, so no aliasing.
        let value = unsafe { (*slot.get()).assume_init_read() };
        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        Some(value)
    }

    /// Empties the queue, calling `f` for every remaining value in order.
    pub fn drain_with(&self, mut f: impl FnMut(T)) {
        while let Some(value) = self.pop() {
            f(value);
        }
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        // Counters are stable here: no other thread may hold a borrow.
        while self.pop().is_some() {}
    }
}

impl<T> std::fmt::Debug for RingBuffer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingBuffer")
            .field("capacity", &self.capacity())
            .field("len", &self.len())
            .finish()
    }
}
