pub struct IdGenerator {
    next_id: u64,
    free_ids: Vec<u64>,
}

impl IdGenerator {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            free_ids: Vec::new(),
        }
    }

    pub fn next_id(&mut self) -> u64 {
        if let Some(id) = self.free_ids.pop() {
            id
        } else {
            let id = self.next_id;
            self.next_id += 1;
            if id == u64::MAX {
                panic!("ID overflow");
            }
            id
        }
    }

    pub fn release_id(&mut self, id: u64) {
        self.free_ids.push(id);
    }
}
