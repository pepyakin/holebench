use std::time::Instant;

pub mod io_uring;
pub mod mmap;
pub mod sync;

pub struct Read {
    pub buf: *mut u8,
    pub len: usize,
    pub at: u64,
}

pub struct Write {
    pub buf: *const u8,
    pub len: usize,
    pub at: u64,
}

pub enum OpTy {
    Read(Read),
    Write(Write),
}

impl OpTy {
    pub fn buf_ptr_and_len(&self) -> (*const u8, usize) {
        match self {
            OpTy::Read(r) => (r.buf as *const u8, r.len),
            OpTy::Write(w) => (w.buf, w.len),
        }
    }
}

unsafe impl Send for OpTy {}

pub struct Op {
    pub ty: OpTy,
    pub result: i32,
    pub created: Option<Instant>,
    pub submitted: Option<Instant>,
    pub retired: Option<Instant>,
    pub user_data: u64,
}

impl Op {
    pub fn read(buf: *mut u8, len: usize, at: u64) -> Self {
        Self {
            ty: OpTy::Read(Read { buf, len, at }),
            created: Some(Instant::now()),
            submitted: None,
            retired: None,
            result: 0,
            user_data: 0,
        }
    }

    pub fn write(buf: *const u8, len: usize, at: u64) -> Self {
        Self {
            ty: OpTy::Write(Write { buf, len, at }),
            created: Some(Instant::now()),
            submitted: None,
            retired: None,
            result: 0,
            user_data: 0,
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
