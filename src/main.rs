//! Hands 1 Mi values from a producer thread to a consumer thread through a
//! 1024-slot queue and reports the observed throughput.
//!
//! Run with `cargo run --release`. A debug build spends most of its time in
//! bounds checks and is not representative.

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use tiny_ringbuf::RingBuffer;

const CAPACITY: usize = 1024;
const ITEMS: usize = 1 << 20;

fn main() {
    let queue = Arc::new(RingBuffer::with_capacity(CAPACITY));
    let started = Instant::now();

    let producer = {
        let queue = Arc::clone(&queue);
        thread::spawn(move || {
            for i in 0..ITEMS {
                let mut pending = i;
                let mut spins = 0u32;
                loop {
                    match queue.push(pending) {
                        Ok(()) => break,
                        Err(handed_back) => {
                            pending = handed_back;
                            if spins < 64 {
                                spins += 1;
                                std::hint::spin_loop();
                            } else {
                                thread::yield_now();
                            }
                        }
                    }
                }
            }
        })
    };

    let consumer = {
        let queue = Arc::clone(&queue);
        thread::spawn(move || {
            let mut received = 0usize;
            let mut checksum = 0usize;
            let mut spins = 0u32;
            while received < ITEMS {
                match queue.pop() {
                    Some(value) => {
                        checksum = checksum.wrapping_add(value);
                        received += 1;
                        spins = 0;
                    }
                    None if spins < 64 => {
                        spins += 1;
                        std::hint::spin_loop();
                    }
                    None => thread::yield_now(),
                }
            }
            (received, checksum)
        })
    };

    producer.join().expect("producer panicked");
    let (received, checksum) = consumer.join().expect("consumer panicked");

    let elapsed = started.elapsed();
    println!("moved {received} values, checksum {checksum:#x}");
    println!(
        "{:.1} Mmsg/s",
        received as f64 / elapsed.as_secs_f64() / 1e6
    );
}
