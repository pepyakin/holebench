use std::{io, time::Instant};

pub mod io_uring;

pub enum OpTy {
    Read { buf: Vec<u8>, at: u64 },
    Write { buf: Vec<u8>, at: u64 },
}

pub struct Op {
    pub ty: OpTy,
    pub result: i32,
    created: Option<Instant>,
    submitted: Option<Instant>,
    retired: Option<Instant>,
}

impl Op {
    pub fn read(buf: Vec<u8>, at: u64) -> Self {
        Self {
            ty: OpTy::Read { buf, at },
            created: None,
            submitted: None,
            retired: None,
            result: 0,
        }
    }

    pub fn write(buf: Vec<u8>, at: u64) -> Self {
        Self {
            ty: OpTy::Write { buf, at },
            created: None,
            submitted: None,
            retired: None,
            result: 0,
        }
    }

    fn note_submitted(&mut self) {
        self.submitted = Some(Instant::now());
    }

    /// Note the time at which this op finished execution.
    fn note_retired(&mut self) {
        self.retired = Some(Instant::now());
    }
}

pub trait Backend {
    fn is_full(&self) -> bool {
        false
    }
    fn submit(&self, op: Op);
    fn wait(&self) -> Option<Op>;
}
