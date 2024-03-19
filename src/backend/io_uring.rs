use crate::Opts;
use super::{Backend, Op, OpTy};
use io_uring::{opcode, types, IoUring};
use slab::Slab;
use std::cell::Cell;
use std::io;
use std::sync::mpsc;
use std::thread;

pub fn init(fd: i32, o: &Opts) -> Box<dyn Backend> {
    let (op_tx, op_rx) = mpsc::sync_channel(100);
    let (retired_tx, retired_rx) = mpsc::sync_channel(100);
    let params = WorkerParams {
        depth: 4,
        fd,
        op_rx,
        retired_tx,
    };
    let _ = thread::spawn(move || {
        worker(params);
    });
    let me = IoUringBackend {
        op_tx,
        retired_rx,
        inflight: Cell::new(0),
        cap: 4,
    };
    Box::new(me)
}

struct IoUringBackend {
    op_tx: mpsc::SyncSender<Op>,
    retired_rx: mpsc::Receiver<Op>,
    inflight: Cell<usize>,
    cap: usize,
}

impl Backend for IoUringBackend {
    fn is_full(&self) -> bool {
        self.inflight.get() == self.cap
    }
    fn submit(&self, op: Op) {
        // TODO:
        self.op_tx.send(op).unwrap();
        let new_inflight = self.inflight.get() + 1;
        self.inflight.set(new_inflight);
    }
    fn wait(&self) -> Option<Op> {
        if self.inflight.get() == 0 {
            return None;
        }
        // TODO: figure out what to do here
        let ret = Some(self.retired_rx.recv().unwrap());
        let new_inflight = self.inflight.get() - 1;
        self.inflight.set(new_inflight);
        ret
    }
}

struct WorkerParams {
    depth: usize,
    fd: i32,
    op_rx: mpsc::Receiver<Op>,
    retired_tx: mpsc::SyncSender<Op>,
}

fn worker(params: WorkerParams) {
    if let Err(err) = worker_inner(params) {}
}

fn worker_inner(
    WorkerParams {
        depth,
        fd,
        op_rx,
        retired_tx,
    }: WorkerParams,
) -> io::Result<()> {
    const PG_SZ: usize = 4096;
    let mut ring: IoUring = IoUring::builder()
        .setup_coop_taskrun()
        .setup_single_issuer()
        .setup_defer_taskrun()
        .build(depth as u32)?;
    let (submitter, mut sq, mut cq) = ring.split();
    let mut inflight: Slab<Op> = Slab::with_capacity(depth);
    loop {
        cq.sync();
        while let Some(cqe) = cq.next() {
            let mut op = inflight.remove(cqe.user_data() as usize);
            op.note_retired();
            if retired_tx.send(op).is_err() {
                return Ok(());
            }
        }

        sq.sync();
        let mut submitted = false;
        while !sq.is_full() {
            // The submission queue has free space. Check if there are any inbound ops pending.
            //
            // If there are none ops in flight, we use the blocking version since we don't need
            // to `enter`/wait for the io-uring.
            //
            // In case the other side of the channel hung up,
            let op = if inflight.is_empty() {
                op_rx.recv().ok()
            } else {
                op_rx.try_recv().ok()
            };
            let Some(mut op) = op else { return Ok(()) };
            op.note_submitted();
            let id = inflight.insert(op);
            let sqe = op_to_sqe(fd, &inflight[id]).user_data(id as u64);
            unsafe {
                // unwrap: we know the ring is not full
                sq.push(&sqe).unwrap();
                submitted = true;
            }
        }

        if submitted {
            sq.sync();
        }
        submitter.submit_and_wait(1)?;
    }
}

fn op_to_sqe(fd: i32, op: &Op) -> io_uring::squeue::Entry {
    let fd = types::Fd(fd);
    match &op.ty {
        OpTy::Read { buf, at } => opcode::Read::new(fd, buf.as_ptr() as *mut u8, buf.len() as u32)
            .offset(*at)
            .build(),
        OpTy::Write { buf, at } => opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
            .offset(*at)
            .build(),
    }
}
