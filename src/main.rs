use anyhow::{bail, Context, Result};
use clap::Parser;
use io_uring::{opcode, types, IoUring};
use rand::seq::SliceRandom;
use rand::{Rng, RngCore};
use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{FileExt, OpenOptionsExt},
    },
    path::PathBuf,
    sync::Arc,
};

use cli::Cli;
use junk::JunkBuf;

mod cli;
mod junk;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut rng = rng();

    println!("{:#?}", cli);
    cli.validate()?;

    let filename = PathBuf::from(&cli.filename);
    if filename.is_dir() {
        bail!("{} is a directory", filename.display());
    }

    // Attempt to open the file and create it if it doesn't exist. The file should be opened
    // with the O_DIRECT flag.
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        //.custom_flags(libc::O_DIRECT)
        .open(&filename)?;

    // Extend the file size to the requested.
    let sz = cli.size.to_bytes() as i64;
    file.set_len(sz as u64)?;

    let total_chunks = cli.size.to_bytes() / cli.bs.to_bytes();
    let n_chunks = (total_chunks as f64 * cli.ratio) as u64;

    println!("total chunks: {}", total_chunks);
    println!("n chunks: {}", n_chunks);

    let mut pos: Vec<_> = (0..total_chunks)
        .map(|chunk_no| chunk_no * cli.bs.to_bytes())
        .into_iter()
        .collect();
    pos.shuffle(&mut rng);
    pos.truncate(n_chunks as usize);

    let file = Arc::new(file);
    let gd = Arc::new(GlobalData {
        filename: filename.clone(),
        file: file.clone(),
        bs: cli.bs.to_bytes(),
        pos: pos,
        junk: JunkBuf::new(cli.bs.to_bytes() as usize, &mut rng),
    });

    const DEPTH: u32 = 256;
    let mut ring: IoUring = IoUring::builder()
        // .setup_coop_taskrun()
        // .setup_single_issuer()
        // .setup_defer_taskrun()
        .build(DEPTH)?;

    let mut pos = gd.pos.iter().copied();
    let mut inflight = 0;
    let mut remaining = gd.pos.len();
    loop {
        ring.completion().sync();

        while let Some(cqe) = ring.completion().next() {
            if cqe.result() < 0 {
                bail!("write failed: {}", cqe.result());
            }
            inflight -= 1;
            remaining -= 1;

            if remaining % 1000 == 0 {
                println!(
                    "remaining %: {:.0}",
                    (remaining as f64 / gd.pos.len() as f64) * 100.0
                );
            }
        }

        ring.submission().sync();
        while !ring.submission().is_full() {
            let Some(offset) = pos.next() else {
                break;
            };
            // let mut buf = junk::allocate_aligned_vec(4096, 4096);
            // rng.fill_bytes(&mut buf);
            let buf = gd.junk.rand(&mut rng);
            let wrt_e =
                opcode::Write::new(types::Fd(file.as_raw_fd()), buf.as_ptr(), buf.len() as _)
                    .offset(offset)
                    .build()
                    .user_data(offset as _);
            unsafe {
                // unwrap: we know the ring is not full
                ring.submission().push(&wrt_e).unwrap();
            }
            inflight += 1;
        }

        // Nothing was submitted this round. If there are no inflight operations, we are done.
        if inflight == 0 {
            break;
        }

        ring.submission().sync();
        ring.submit_and_wait(1)?;
    }

    Ok(())
}

#[derive(Debug)]
struct GlobalData {
    filename: PathBuf,
    file: Arc<File>,
    pos: Vec<u64>,
    bs: u64,
    junk: JunkBuf,
}

#[derive(Debug, Clone)]
struct ThreadData {
    job_id: usize,
    gd: Arc<GlobalData>,
    pos: Vec<u64>,
}

fn rng() -> rand_pcg::Pcg64 {
    rand_pcg::Pcg64::new(0xcafef00dd15ea5e5, 0xa02bdbf7bb3c0a7ac28fa16a64abf96)
}
