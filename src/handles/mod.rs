//! Queue ends with their role baked into the type.
//!
//! [`RingBuffer`] is correct only with one producer thread and one consumer
//! thread, which until now lived in prose. [`RingBuffer::split`] hands out a
//! [`Producer`] and a [`Consumer`] borrow instead, and each side exposes only
//! its own operations. Passing the wrong end to the wrong thread then fails to
//! compile rather than corrupting the counters.
//!
//! `split` is not counted: calling it twice yields four handles, and the
//! one-each rule applies per pair. Reach for a `Split` guard if you need the
//! compiler to enforce a single pair.
//!
//! Both ends are `Send` whenever `T: Send`, so they can cross a
//! `thread::scope` boundary.

use crate::RingBuffer;

/// The writing end, as produced by [`RingBuffer::split`].
pub struct Producer<'a, T> {
    ring: &'a RingBuffer<T>,
}

/// The reading end, as produced by [`RingBuffer::split`].
pub struct Consumer<'a, T> {
    ring: &'a RingBuffer<T>,
}

impl<T> RingBuffer<T> {
    /// Borrows the queue as one producer half and one consumer half.
    ///
    /// The halves live as long as `self`, so the borrow checker keeps the
    /// queue alive while either side is in use.
    pub fn split(&self) -> (Producer<'_, T>, Consumer<'_, T>) {
        (Producer { ring: self }, Consumer { ring: self })
    }
}

impl<T> Producer<'_, T> {
    /// See [`RingBuffer::push`].
    pub fn push(&self, value: T) -> Result<(), T> {
        self.ring.push(value)
    }

    /// See [`RingBuffer::is_full`].
    pub fn is_full(&self) -> bool {
        self.ring.is_full()
    }

    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }
}

impl<T> Consumer<'_, T> {
    /// See [`RingBuffer::pop`].
    pub fn pop(&self) -> Option<T> {
        self.ring.pop()
    }

    /// See [`RingBuffer::is_empty`].
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// See [`RingBuffer::drain_with`].
    pub fn drain_with(&self, f: impl FnMut(T)) {
        self.ring.drain_with(f)
    }
}
