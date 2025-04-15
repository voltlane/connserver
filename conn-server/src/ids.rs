use std::sync::{atomic::AtomicU64, Arc, Mutex};

pub struct IdGenerator {
    next_id: AtomicU64,
    free_ids: Arc<Mutex<Vec<u64>>>,
}

impl IdGenerator {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(0),
            free_ids: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn next_id(&mut self) -> u64 {
        if let Some(id) = self.free_ids.lock().unwrap().pop() {
            id
        } else {
            let id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if id == u64::MAX {
                panic!("ID overflow");
            }
            id
        }
    }

    pub fn release_id(&mut self, id: u64) {
        self.free_ids.lock().unwrap().push(id);
    }
}
