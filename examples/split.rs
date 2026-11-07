//! Moves 10 000 values through scoped threads using the split API.
//!
//! `cargo run --release --example split`
//!
//! The producer only ever sees a `Producer`, the consumer only a `Consumer`,
//! so the single-writer rule is checked at compile time. `thread::scope` lets
//! both halves borrow the stack-allocated queue.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use tiny_ringbuf::RingBuffer;

const ITEMS: usize = 10_000;

fn main() {
    let queue = RingBuffer::with_capacity(64);
    let received = AtomicUsize::new(0);

    thread::scope(|scope| {
        let (producer, consumer) = queue.split();

        scope.spawn(move || {
            let mut next = 0usize;
            let mut spins = 0u32;
            while next < ITEMS {
                match producer.push(next) {
                    Ok(()) => {
                        next += 1;
                        spins = 0;
                    }
                    Err(_) if spins < 64 => {
                        spins += 1;
                        std::hint::spin_loop();
                    }
                    Err(_) => thread::yield_now(),
                }
            }
        });

        while received.load(Ordering::Relaxed) < ITEMS {
            match consumer.pop() {
                Some(value) => {
                    assert_eq!(value, received.load(Ordering::Relaxed), "out of order");
                    received.store(value + 1, Ordering::Relaxed);
                }
                None => thread::yield_now(),
            }
        }
    });

    println!(
        "received {} values in order",
        received.load(Ordering::Relaxed)
    );
}
