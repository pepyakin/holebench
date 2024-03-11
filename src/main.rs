use anyhow::{bail, Result};
use clap::Parser;
use hdrhistogram::Histogram;
use io_uring::{opcode, types, IoUring};
use libc::iovec;
use rand::seq::SliceRandom;
use rand::RngCore;
use slab::Slab;
use std::path::Path;
use std::time::Instant;
use std::{
    fs::{File, OpenOptions},
    os::{
        fd::AsRawFd,
        unix::fs::OpenOptionsExt,
    },
    path::PathBuf,
    sync::Arc,
};

use cli::Cli;
use junk::JunkBuf;

use crate::junk::allocate_aligned_vec;

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
    let junk = JunkBuf::new(cli.bs.to_bytes() as usize, &mut rng);

    create_and_layout_file(&filename, cli.size.to_bytes(), &mut rng, &pos, &junk)?;
    measure(filename, cli, pos)?;

    Ok(())
}

/// Perform a layout of the given file.
fn create_and_layout_file(
    filename: &Path,
    size: u64,
    rng: &mut impl RngCore,
    pos: &[u64],
    junk: &JunkBuf,
) -> anyhow::Result<()> {
    // We don't supply O_DIRECT here, since that seems to be faster for some reason.
    // TODO: this doesn't perform as best as possible with O_DIRECT. Why?
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&filename)?;

    // Extend the file size to the requested.
    file.set_len(size)?;

    const DEPTH: u32 = 256;
    let mut ring: IoUring = IoUring::builder()
        // .setup_coop_taskrun()
        // .setup_single_issuer()
        // .setup_defer_taskrun()
        .build(DEPTH)?;

    let mut pos_iter = pos.iter().copied();
    let mut inflight = 0;
    let mut remaining = pos.len();
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
                    (remaining as f64 / pos.len() as f64) * 100.0
                );
            }
        }

        ring.submission().sync();
        while !ring.submission().is_full() {
            let Some(offset) = pos_iter.next() else {
                break;
            };
            let buf = junk.rand(rng);
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

fn measure(filename: PathBuf, cli: Cli, pos: Vec<u64>) -> Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(&filename)?;
    const DEPTH: usize = 256;
    const PG_SZ: usize = 4096;
    let mut ring: IoUring = IoUring::builder()
        // .setup_coop_taskrun()
        // .setup_single_issuer()
        // .setup_defer_taskrun()
        .build(DEPTH as u32)?;
    let (submitter, mut sq, mut cq) = ring.split();
    let mut free_buf_pool = (0..DEPTH).collect::<Vec<_>>();
    let mut buffers = (0..DEPTH)
        .map(|_| allocate_aligned_vec(PG_SZ, 4096))
        .collect::<Vec<_>>();
    let iovecs = buffers
        .iter_mut()
        .map(|buf| iovec {
            iov_base: buf.as_mut_ptr() as _,
            iov_len: buf.len(),
        })
        .collect::<Vec<_>>();
    // SAFETY: trust me bro.
    unsafe {
        submitter.register_buffers(&iovecs)?;
    }
    let mut ioops: Slab<Ioop> = Slab::with_capacity(DEPTH as usize);
    struct Ioop {
        buf_index: usize,
        start: Instant,
    }
    let mut index = 0;
    let loop_start = Instant::now();
    let mut second_start = Instant::now();
    let mut iops = 0;
    let mut ramping_up = true;
    let mut latencies_h = Histogram::<u64>::new_with_bounds(1, 1000000000, 3).unwrap();
    loop {
        if iops > 1000 && second_start.elapsed().as_millis() > 1000 {
            second_start = Instant::now();

            let cur = iops;
            iops = 0;
            println!("cur: {}", cur);

            // Print out the latencies for various percentiles.
            for q in [0.001, 0.01, 0.25, 0.50, 0.75, 0.95, 0.99, 0.999] {
                let lat = latencies_h.value_at_quantile(q);
                println!("{}th: {} us", q * 100.0, lat / 1000);
            }
        }

        if ramping_up {
            if loop_start.elapsed().as_millis() >= cli.ramp_time as u128 * 1000 {
                ramping_up = false;
            }
        }

        cq.sync();
        while let Some(cqe) = cq.next() {
            if cqe.result() < 0 {
                bail!("write failed: {}", cqe.result());
            }
            let Ioop { buf_index, start } = ioops.remove(cqe.user_data() as usize);
            free_buf_pool.push(buf_index);

            if !ramping_up {
                // Do the bookkeeping, but only if we are not ramping up.
                latencies_h
                    .record(start.elapsed().as_nanos().try_into().unwrap())
                    .unwrap();
            }
            iops += 1;
        }

        sq.sync();
        let mut submitted = false;
        while !sq.is_full() && !free_buf_pool.is_empty() {
            // println!("inflight: {}/{}", ioops.len(), sq.capacity());

            let offset = pos[index];
            index = (index + 1) % pos.len();

            // check out a free buffer.
            let Some(buf_index) = free_buf_pool.pop() else {
                panic!("free buffer pool is exhausted")
            };
            let buf = &mut buffers[buf_index];
            let token = ioops.insert(Ioop {
                buf_index,
                start: Instant::now(),
            });
            let rd_e = opcode::ReadFixed::new(
                types::Fd(file.as_raw_fd()),
                buf.as_mut_ptr(),
                buf.len() as _,
                buf_index as u16,
            )
            .offset(offset)
            .build()
            .user_data(token as u64);
            unsafe {
                // unwrap: we know the ring is not full
                sq.push(&rd_e).unwrap();
                submitted = true;
            }
        }

        if submitted {
            sq.sync();
        }
        submitter.submit_and_wait(1)?;
    }
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
