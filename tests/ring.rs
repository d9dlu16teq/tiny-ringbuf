use tiny_ringbuf::RingBuffer;

#[test]
fn values_come_back_in_order() {
    let queue = RingBuffer::with_capacity(8);
    for value in 0..8 {
        assert_eq!(queue.push(value), Ok(()));
    }
    for value in 0..8 {
        assert_eq!(queue.pop(), Some(value));
    }
    assert!(queue.is_empty());
}

#[test]
fn full_queue_hands_the_value_back() {
    let queue = RingBuffer::with_capacity(4);
    for value in 0..4 {
        assert_eq!(queue.push(value), Ok(()));
    }
    assert!(queue.is_full());
    assert_eq!(queue.push(99), Err(99));
    assert_eq!(queue.len(), 4);
}

#[test]
fn empty_queue_returns_none() {
    let queue: RingBuffer<u32> = RingBuffer::with_capacity(2);
    assert_eq!(queue.pop(), None);
    assert!(queue.is_empty());
}

#[test]
fn drained_queue_reuses_its_slots() {
    let queue = RingBuffer::with_capacity(4);
    queue.drain_with(|_| ());
    for round in 0..3 {
        for offset in 0..4 {
            assert_eq!(queue.push(round * 10 + offset), Ok(()));
        }
        queue.drain_with(|_| ());
    }
    assert_eq!(queue.len(), 0);
}

#[test]
fn drop_runs_destructors_of_pending_values() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Debug)]
    struct Counted(Arc<AtomicUsize>);

    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let queue = RingBuffer::with_capacity(4);
    for _ in 0..3 {
        queue.push(Counted(Arc::clone(&drops))).unwrap();
    }
    drop(queue);
    assert_eq!(drops.load(Ordering::Relaxed), 3);
}

#[test]
#[should_panic(expected = "power of two")]
fn non_power_of_two_capacity_panics() {
    let _ = RingBuffer::<u8>::with_capacity(3);
}
