use super::{Backend, Op, OpTy, Read, Write};
use crate::Opts;
use crossbeam::channel;
use std::cell::RefCell;
use std::thread;

pub fn init(fd: i32, o: &'static Opts) -> Box<dyn Backend> {
    let (sq_tx, sq_rx) = channel::bounded(o.backlog_cnt);
    let (cq_tx, cq_rx) = channel::bounded(o.backlog_cnt);

    for _i in 0..o.num_jobs {
        let sq_rx = sq_rx.clone();
        let cq_tx = cq_tx.clone();
        let _ = thread::spawn(move || {
            worker(o, fd, sq_rx, cq_tx);
        });
    }

    let me = SyncBackend {
        sq_tx,
        cq_rx,
        inflight: RefCell::new(0),
        cap: o.backlog_cnt,
    };
    Box::new(me)
}

struct SyncBackend {
    sq_tx: channel::Sender<Op>,
    cq_rx: channel::Receiver<Op>,
    inflight: RefCell<usize>,
    cap: usize,
}

impl Backend for SyncBackend {
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

fn worker(o: &'static Opts, fd: i32, sq_rx: channel::Receiver<Op>, cq_tx: channel::Sender<Op>) {
    loop {
        let mut op = match sq_rx.recv() {
            Ok(op) => op,
            Err(_) => break,
        };
        {
            op.note_submitted();
            handle_op(o, fd, &mut op);
            op.note_retired();
        }
        match cq_tx.send(op) {
            Ok(()) => (),
            Err(_) => break,
        }
    }
}

fn handle_op(_o: &'static Opts, fd: i32, op: &mut Op) {
    match op.ty {
        OpTy::Read(Read { buf, len, at }) => unsafe {
            libc::pread(fd, buf.cast(), len, at as i64);
        },
        OpTy::Write(Write { buf, len, at }) => unsafe {
            libc::pwrite(fd, buf.cast(), len, at as i64);
        },
    }
}
