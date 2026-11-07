//! Throughput sweep over several capacities.
//!
//! `cargo run --release --bin bench -- 64 256 1024 4096`
//!
//! Each run measures a pinned producer/consumer pair moving `ITEMS` values;
//! the numbers move with core layout, so read them as a comparison between
//! capacities, not as an absolute claim.

use std::env;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use tiny_ringbuf::RingBuffer;

const ITEMS: usize = 1 << 20;

fn measure(capacity: usize) -> f64 {
    let queue = Arc::new(RingBuffer::with_capacity(capacity));
    let started = Instant::now();

    let producer = {
        let queue = Arc::clone(&queue);
        thread::spawn(move || {
            for i in 0..ITEMS {
                let mut pending = i;
                let mut spins = 0u32;
                while let Err(handed_back) = queue.push(pending) {
                    pending = handed_back;
                    if spins < 64 {
                        spins += 1;
                        std::hint::spin_loop();
                    } else {
                        thread::yield_now();
                    }
                }
            }
        })
    };

    let consumer = {
        let queue = Arc::clone(&queue);
        thread::spawn(move || {
            let mut received = 0usize;
            let mut spins = 0u32;
            while received < ITEMS {
                if queue.pop().is_some() {
                    received += 1;
                    spins = 0;
                } else if spins < 64 {
                    spins += 1;
                    std::hint::spin_loop();
                } else {
                    thread::yield_now();
                }
            }
        })
    };

    producer.join().expect("producer panicked");
    consumer.join().expect("consumer panicked");

    ITEMS as f64 / started.elapsed().as_secs_f64() / 1e6
}

fn main() {
    let capacities: Vec<usize> = env::args()
        .skip(1)
        .map(|raw| raw.parse().expect("capacity must be an integer"))
        .collect();
    let capacities = if capacities.is_empty() {
        vec![64, 256, 1024, 4096]
    } else {
        capacities
    };

    println!("capacity Mmsg/s");
    for capacity in capacities {
        println!("{capacity:>8} {:.1}", measure(capacity));
    }
}
