use std::{cell::RefCell, sync::{Arc, Weak}};

use crate::Opts;
use super::{Backend, Op, OpTy};
use std::{ptr, thread};
use crossbeam::channel;

struct Mmap {
    base: *mut u8,
    len: usize,
}

impl Mmap {
    fn mmap_fd(fd: i32, len: usize) -> Self {
        let base = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            panic!();
        }
        Self {
            base: base as *mut u8,
            len,
        }
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        unsafe {
            let _ = libc::munmap(
                self.base as *mut libc::c_void,
                self.len,
            );
        }
    }
}

unsafe impl Send for Mmap {   
}
unsafe impl Sync for Mmap {   
}

pub fn init(fd: i32, o: &Opts) -> Box<dyn Backend> {
    const NUMJOBS: usize = 2;

    let mmap = Arc::new(Mmap::mmap_fd(fd, o.size as usize));

    let (sq_tx, sq_rx) = channel::bounded(100);
    let (cq_tx, cq_rx) = channel::bounded(100);
    
    for i in 0..NUMJOBS {
        let sq_rx = sq_rx.clone();
        let cq_tx = cq_tx.clone();
        let mmap = Arc::downgrade(&mmap);
        let _ = thread::spawn(move || {
            worker(mmap, sq_rx, cq_tx);
        });
    }

    let me = MmapBackend {
        mmap,
        sq_tx,
        cq_rx,
        inflight: RefCell::new(0),
        cap: 4,
    };
    Box::new(me)
}

struct MmapBackend {
    mmap: Arc<Mmap>,
    sq_tx: channel::Sender<Op>,
    cq_rx: channel::Receiver<Op>,
    inflight: RefCell<usize>,
    cap: usize,
}

impl Backend for MmapBackend {
    fn is_full(&self) -> bool {
        *self.inflight.borrow() == self.cap
    }

    fn submit(&self, op: super::Op) {
        self.sq_tx.send(op).unwrap();
        *self.inflight.borrow_mut() += 1;
    }

    fn wait(&self) -> Option<super::Op> {
        let mut inflight = self.inflight.borrow_mut();
        if *inflight == 0 {
            return None;
        }
        let r = Some(self.cq_rx.recv().unwrap());
        *inflight -= 1;
        r
    }
}

fn worker(mmap: Weak<Mmap>, mut sq_rx: channel::Receiver<Op>, cq_tx: channel::Sender<Op>) {
    loop {
        let mut op = match sq_rx.recv() {
            Ok(op) => op,
            Err(_) => break,
        };
        {
            let Some(mmap) = mmap.upgrade() else { break };
            op.note_submitted();
            handle_op(mmap.base, &mut op);
            op.note_retired();
        }
        match cq_tx.send(op) {
            Ok(()) => (),
            Err(_) => break,
        }
    }
}

fn handle_op(base: *mut u8, op: &mut Op) {
    match op.ty {
        OpTy::Read { buf, len, at } => {
            unsafe {
                let src = base.offset(at as isize);
                std::ptr::copy_nonoverlapping(
                    src,
                    buf,
                    len,
                )
            }
        },
        OpTy::Write { buf, len, at } => {
            unsafe {
                let dst = base.offset(at as isize);
                std::ptr::copy_nonoverlapping(
                    buf,
                    dst,
                    len,
                )
            }
        },
    }
}
