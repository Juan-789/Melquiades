//! Bounded single-producer/single-consumer handoff for [`SlotId`] values.
//!
//! This queue transfers ownership of a location in the frame pool. It never
//! transfers a frame's pixels themselves. The capacity is deliberately the
//! same as the frame-pool size: four IDs can be either free, ready, or owned by
//! one of the two stages, but they must never be duplicated.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::capture::SlotId;
use crate::config::FRAME_POOL_SLOTS;

const MASK: usize = FRAME_POOL_SLOTS - 1;

/// A fixed-capacity queue with exactly one producer and one consumer.
///
/// `head` and `tail` are monotonically increasing logical cursors. Their
/// physical cell is computed with `cursor & MASK`, so the hot path has no
/// division. With four cells, `tail - head` is the current occupancy:
///
/// ```text
/// 0 = empty
/// 4 = full
/// ```
///
/// # Safety
///
/// A cell is written only by the producer, after it has acquired space from
/// the consumer's released `head`. A cell is read only by the consumer, after
/// it has acquired the producer's released `tail`. This makes it safe to use
/// `UnsafeCell` without a lock, but only under the strict one-producer,
/// one-consumer contract.
pub struct SpscSlotRing {
    cells: [UnsafeCell<MaybeUninit<SlotId>>; FRAME_POOL_SLOTS],
    head: AtomicUsize,
    tail: AtomicUsize,
}

// `UnsafeCell` prevents Rust from automatically declaring this type Sync.
// The SPSC ownership rules documented above make concurrent access safe:
// producer and consumer never access the same cell at the same time.
unsafe impl Send for SpscSlotRing {}
unsafe impl Sync for SpscSlotRing {}

impl SpscSlotRing {
    pub fn new() -> Self {
        assert!(
            FRAME_POOL_SLOTS.is_power_of_two(),
            "masked SPSC indexing requires a power-of-two capacity"
        );

        Self {
            cells: [const { UnsafeCell::new(MaybeUninit::uninit()) }; FRAME_POOL_SLOTS],
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    /// Publishes one slot ID for the consumer.
    ///
    /// Returns the ID unchanged if the ring is full, so the caller cannot
    /// silently lose ownership of it. A full `FreeSlots` ring is a violated
    /// ownership invariant; a full `ReadySlots` ring is handled by the capture
    /// stage's explicit drop policy once that ring is connected.
    pub fn try_push(&self, id: SlotId) -> Result<(), SlotId> {
        // Only the producer writes `tail`, so this load needs no synchronization.
        let tail = self.tail.load(Ordering::Relaxed);
        // Acquire pairs with the consumer's Release after it has finished
        // reading a cell, making that cell safe for the producer to reuse.
        let head = self.head.load(Ordering::Acquire);

        if tail.wrapping_sub(head) >= FRAME_POOL_SLOTS {
            return Err(id);
        }

        let cell = tail & MASK;
        // SAFETY: the fullness check and acquired head prove the consumer has
        // released this physical cell before the producer reuses it.
        unsafe { (*self.cells[cell].get()).write(id) };

        // Publish the written ID only after its cell contents are visible.
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Takes the next slot ID in FIFO order, without waiting.
    pub fn try_pop(&self) -> Option<SlotId> {
        // Only the consumer writes `head`, so this load needs no synchronization.
        let head = self.head.load(Ordering::Relaxed);
        // Acquire pairs with the producer's Release after it wrote the cell.
        let tail = self.tail.load(Ordering::Acquire);

        if head == tail {
            return None;
        }

        let cell = head & MASK;
        // SAFETY: the acquired tail proves the producer initialized this cell
        // before publishing it, and only this consumer reads it.
        let id = unsafe { (*self.cells[cell].get()).assume_init_read() };

        // Release tells the producer this physical cell may be overwritten.
        self.head.store(head.wrapping_add(1), Ordering::Release);
        Some(id)
    }
}

impl Default for SpscSlotRing {
    fn default() -> Self {
        Self::new()
    }
}
