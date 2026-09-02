//! Ownership-preserving capture-to-sender handoff.
//!
//! The only code that crosses from a SlotId to an `UnsafeCell<FrameSlot>` is
//! here. Capture and sender receive safe, role-specific operations instead of
//! arbitrary pool access.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::capture::{FrameInfo, FramePool, FrameSource, SlotId};
use crate::config::{FRAME_POOL_SLOTS, FRAME_SIZE};
use crate::spsc::SpscSlotRing;

pub struct Pipeline {
    capture: CapturePort,
    sender: SenderPort,
}

pub struct CapturePort {
    pool: Arc<FramePool>,
    free_slots: Arc<SpscSlotRing>,
    ready_slots: Arc<SpscSlotRing>,
    counters: Arc<HandoffCounters>,
    capture_running: Arc<AtomicBool>,
}

pub struct SenderPort {
    pool: Arc<FramePool>,
    free_slots: Arc<SpscSlotRing>,
    ready_slots: Arc<SpscSlotRing>,
    counters: Arc<HandoffCounters>,
    capture_running: Arc<AtomicBool>,
}

struct HandoffCounters {
    capture_dropped_no_free_slot: AtomicU64,
    sender_dropped_stale_ready: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
pub struct HandoffSnapshot {
    pub capture_dropped_no_free_slot: u64,
    pub sender_dropped_stale_ready: u64,
}

impl Pipeline {
    pub fn new() -> Self {
        let pool = Arc::new(FramePool::new(FRAME_POOL_SLOTS, FRAME_SIZE));
        let free_slots = Arc::new(SpscSlotRing::new());
        let ready_slots = Arc::new(SpscSlotRing::new());

        // Setup occurs before either endpoint is moved to a thread. Main is
        // temporarily the sole FreeSlots producer here.
        for id in pool.slot_ids() {
            free_slots
                .try_push(id)
                .expect("the free ring must hold every pool SlotId at startup");
        }

        let counters = Arc::new(HandoffCounters {
            capture_dropped_no_free_slot: AtomicU64::new(0),
            sender_dropped_stale_ready: AtomicU64::new(0),
        });
        let capture_running = Arc::new(AtomicBool::new(true));

        Self {
            capture: CapturePort {
                pool: Arc::clone(&pool),
                free_slots: Arc::clone(&free_slots),
                ready_slots: Arc::clone(&ready_slots),
                counters: Arc::clone(&counters),
                capture_running: Arc::clone(&capture_running),
            },
            sender: SenderPort {
                pool,
                free_slots,
                ready_slots,
                counters,
                capture_running,
            },
        }
    }

    pub fn into_ports(self) -> (CapturePort, SenderPort) {
        (self.capture, self.sender)
    }
}

impl CapturePort {
    pub fn stop(&self) {
        self.capture_running.store(false, Ordering::Release);
    }

    /// Captures one source frame into a free pool slot, or dequeues and drops a
    /// source frame if sender currently owns every slot.
    pub fn capture_once(
        &self,
        source: &mut impl FrameSource,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(id) = self.free_slots.try_pop() else {
            source.discard_next_frame()?;
            self.counters
                .capture_dropped_no_free_slot
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        };

        let captured = unsafe {
            // SAFETY: popping from FreeSlots gives capture exclusive ownership
            // of `id` until it is either published or returned below.
            let slot = self.pool.capture_slot(&id);
            let info = source.next_frame(slot)?;
            slot.set_info(info)?;
            Ok::<(), Box<dyn std::error::Error>>(())
        };

        // A source failure terminates capture, so this slot is intentionally
        // not returned. Capture must never produce FreeSlots; sender is that
        // ring's sole producer.
        captured?;

        if let Err(id) = self.ready_slots.try_push(id) {
            // This cannot occur in a correct four-slot conservation cycle: a
            // slot just removed from FreeSlots guarantees ReadySlots cannot
            // contain all four IDs. Returning it here would make capture a
            // second FreeSlots producer and violate SPSC, so fail loudly.
            panic!("ReadySlots full after capture claimed {id:?}");
        }
        Ok(())
    }
}

impl SenderPort {
    pub fn capture_is_running(&self) -> bool {
        self.capture_running.load(Ordering::Acquire)
    }

    /// Removes all currently ready IDs, releases stale frames, and retains the
    /// newest frame for the sender. FIFO removal is preserved even though only
    /// the final frame is transmitted.
    pub fn take_newest(&self) -> Option<SlotId> {
        let newest = self.ready_slots.try_pop()?;
        let mut newest = newest;

        while let Some(stale) = self.ready_slots.try_pop() {
            self.return_to_free(newest);
            self.counters
                .sender_dropped_stale_ready
                .fetch_add(1, Ordering::Relaxed);
            newest = stale;
        }

        Some(newest)
    }

    /// Runs sender work while this endpoint exclusively owns `id`.
    ///
    /// The closure cannot retain a reference to the slot after it returns.
    pub fn with_frame<R>(&self, id: &SlotId, operation: impl FnOnce(FrameInfo, &[u8]) -> R) -> R {
        unsafe {
            // SAFETY: take_newest removed `id` from ReadySlots. Only this
            // sender accesses it until the caller returns it with
            // return_to_free.
            let slot = self.pool.sender_slot(id);
            let info = slot
                .info()
                .expect("a ReadySlots ID must refer to completed frame metadata");
            let bytes = slot
                .bytes(info.byte_len)
                .expect("a ReadySlots frame length must fit its pool slot");
            operation(info, bytes)
        }
    }

    pub fn return_to_free(&self, id: SlotId) {
        unsafe {
            // SAFETY: sender owns `id` after take_newest, including when the
            // frame is dropped as stale or an error ends send processing.
            self.pool.clear_claimed_slot(&id);
        }
        self.free_slots
            .try_push(id)
            .expect("returning a sender-owned slot must not fill FreeSlots");
    }

    /// Returns and resets handoff outcomes since the previous report.
    pub fn take_snapshot(&self) -> HandoffSnapshot {
        HandoffSnapshot {
            capture_dropped_no_free_slot: self
                .counters
                .capture_dropped_no_free_slot
                .swap(0, Ordering::Relaxed),
            sender_dropped_stale_ready: self
                .counters
                .sender_dropped_stale_ready
                .swap(0, Ordering::Relaxed),
        }
    }
}
