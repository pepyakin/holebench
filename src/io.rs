use std::sync::mpsc;

use io_uring::IoUring;

enum Op {
    Read { offset: u64 },
    Write { offset: u64, buf_index: usize },
}

pub struct Handle {
    tx: mpsc::SyncSender<Op>,
    worker_join: std::thread::JoinHandle<()>,
}

impl Handle {
    pub fn read(&self, offset: u64) {
        self.tx.send(Op::Read { offset }).unwrap();
    }

    pub fn write(&self, offset: u64, data: &mut Vec<u8>) {
        self.tx.send(Op::Write { offset, buf_index: 0 }).unwrap();
    }

    pub fn wait(&self) {
        self.tx.
        todo!()
    }
}

fn worker(mut ring: IoUring, rx: mpsc::Receiver<Op>) {
    let (submitter, sq, cq) = ring.split();
    
}

pub fn init() -> anyhow::Result<Handle> {
    let ring = IoUring::new(256)?;
    let (tx, rx) = mpsc::sync_channel(256);
    let join_handle = std::thread::spawn(move || worker(ring, rx));
    Ok(Handle {
        tx,
        worker_join: join_handle,
    })
}
