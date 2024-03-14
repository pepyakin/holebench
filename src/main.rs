use anyhow::{bail, Result};
use clap::Parser;
use hdrhistogram::Histogram;
use io_uring::{opcode, types, IoUring};
use libc::iovec;
use rand::seq::SliceRandom;
use rand::RngCore;
use slab::Slab;
use std::io::Write;
use std::time::{Duration, Instant};
use std::{
    fs::OpenOptions,
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::PathBuf,
};

use cli::Cli;
use junk::JunkBuf;

use crate::junk::allocate_aligned_vec;

mod cli;
mod junk;

struct Opts {
    /// The name to the file under test.
    filename: PathBuf,
    /// The total size of the file.
    size: u64,
    /// The size of the IO operations performed in bytes.
    bs: u64,
    /// The number of blocks, including populated blocks and holes.
    ///
    /// This is essentially `size` but in blocks of the `bs` size.
    n_blocks: u64,
    /// The number of populated blocks we should populated in the file.
    ///
    /// Calculated using the passed ratio parameter. Each block is of `bs` size.
    n_populated_blocks: u64,
    /// true if we should zero file (as in contrast to leave holes)
    no_sparse: bool,
    /// true if `falloc` with `FALLOC_FL_KEEP_SIZE` should be applied to the file.
    falloc_keep_size: bool,
    /// true if `falloc` with `FALLOC_FL_ZERO_RANGE` should be applied to the file.
    falloc_zero_range: bool,
    ramp_time: Duration,
}

fn parse_cli(cli: Cli) -> Result<Opts> {
    let filename = PathBuf::from(&cli.filename);
    if filename.is_dir() {
        bail!("{} is a directory", filename.display());
    }
    let bs = cli.bs.to_bytes();
    if bs == 0 {
        bail!("bs can't be zero")
    }
    let size = cli.size.to_bytes();
    if i64::try_from(cli.size.to_bytes()).is_err() {
        bail!("the size should be equal or less than 2^63")
    }
    if size % bs != 0 {
        bail!("the size should be a multiple of block size");
    }
    let n_blocks = size / bs;
    if cli.ratio < 0.0 || cli.ratio > 1.0 {
        bail!("--ratio must be within 0..1");
    }
    let n_populated_blocks = (n_blocks as f64 * cli.ratio) as u64;
    let ramp_time = Duration::from_secs(cli.ramp_time);
    Ok(Opts {
        filename,
        size,
        bs,
        n_blocks,
        n_populated_blocks,
        no_sparse: cli.no_sparse,
        falloc_keep_size: cli.falloc_keep_size,
        falloc_zero_range: cli.falloc_zero_range,
        ramp_time,
    })
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut rng = rng();

    let o = parse_cli(cli)?;

    // Generate indicies of blocks that must be populated.
    let mut popix: Vec<_> = (0..o.n_blocks)
        .map(|chunk_no| chunk_no * o.bs)
        .into_iter()
        .collect();
    popix.shuffle(&mut rng);
    popix.truncate(o.n_populated_blocks as usize);
    let junk = JunkBuf::new(o.bs as usize, &mut rng);

    create_and_layout_file(&o, &mut rng, &popix, &junk)?;
    measure(&o, popix)?;

    Ok(())
}

/// Perform a layout of the given file.
fn create_and_layout_file(
    o: &Opts,
    rng: &mut impl RngCore,
    pos: &[u64],
    junk: &JunkBuf,
) -> anyhow::Result<()> {
    // We don't supply O_DIRECT here, since that seems to be faster for some reason.
    // TODO: this doesn't perform as best as possible with O_DIRECT. Why?
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&o.filename)?;

    // Extend the file size to the requested.
    file.set_len(o.size)?;

    if o.falloc_keep_size || o.falloc_zero_range {
        let mut flags = 0;
        if o.falloc_keep_size {
            flags |= libc::FALLOC_FL_KEEP_SIZE;
        }
        if o.falloc_zero_range {
            flags |= libc::FALLOC_FL_ZERO_RANGE;
        }
        unsafe {
            libc::fallocate(file.as_raw_fd(), flags, 0, o.size as i64);
        }
    }

    if o.no_sparse {
        // TODO: optimize this
        let zeros = vec![0; o.bs as usize];
        let blocks = o.size / o.bs;
        for i in 0..blocks {
            if i % 1000 == 0 {
                println!("zeroing: {}/{}", i, blocks);
            }
            file.write_all(&zeros)?;
        }
    }

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

fn measure(o: &Opts, pos: Vec<u64>) -> Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(&o.filename)?;
    const DEPTH: usize = 4;
    const PG_SZ: usize = 4096;
    let mut ring: IoUring = IoUring::builder()
        .setup_coop_taskrun()
        .setup_single_issuer()
        .setup_defer_taskrun()
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
            println!("mean={} us", latencies_h.mean() / 1000.0);
        }

        if ramping_up {
            if loop_start.elapsed() >= o.ramp_time {
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

fn rng() -> rand_pcg::Pcg64 {
    rand_pcg::Pcg64::new(0xcafef00dd15ea5e5, 0xa02bdbf7bb3c0a7ac28fa16a64abf96)
}
