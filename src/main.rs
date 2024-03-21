use anyhow::{bail, Result};
use clap::Parser;
use hdrhistogram::Histogram;
use rand::seq::SliceRandom;
use rand::RngCore;
use slab::Slab;
use std::fs::File;
use std::io::Write;
use std::time::{Duration, Instant};
use std::{
    fs::OpenOptions,
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::PathBuf,
};

use cli::Cli;
use junk::JunkBuf;

use crate::backend::Op;
use crate::junk::allocate_aligned_vec;

mod backend;
mod cli;
mod junk;

struct Opts {
    /// The name to the file under test.
    filename: PathBuf,
    /// The total size of the file in bytes.
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
    /// Skip layout phase. Assume file exists.
    skip_layout: bool,
    /// The number of items to keep in the backlog.
    backlog_cnt: usize,
    ramp_time: Duration,
    backend: cli::Backend,
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

    if cli.skip_layout {
        if !filename.exists() {
            bail!("--skip-layout passed and file does not exist!");
        }
    }

    Ok(Opts {
        filename,
        size,
        bs,
        n_blocks,
        n_populated_blocks,
        no_sparse: cli.no_sparse,
        falloc_keep_size: cli.falloc_keep_size,
        falloc_zero_range: cli.falloc_zero_range,
        skip_layout: cli.skip_layout,
        backlog_cnt: cli.backlog,
        ramp_time,
        backend: cli.backend,
    })
}

fn backend(file: &File, o: &Opts) -> Box<dyn crate::backend::Backend> {
    match o.backend {
        cli::Backend::IoUring => crate::backend::io_uring::init(file.as_raw_fd(), o),
        cli::Backend::Mmap => crate::backend::mmap::init(file.as_raw_fd(), o),
    }
}

fn rng() -> rand_pcg::Pcg64 {
    rand_pcg::Pcg64::new(0xcafef00dd15ea5e5, 0xa02bdbf7bb3c0a7ac28fa16a64abf96)
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

    if !o.skip_layout {
        create_and_layout_file(&o, &mut rng, &popix, &junk)?;
    }
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
        .read(true)
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

    let backend = backend(&file, o);
    let mut pos_iter = pos.iter().copied();
    let mut remaining = pos.len();
    loop {
        while !backend.is_full() {
            let Some(offset) = pos_iter.next() else {
                break;
            };
            let buf = junk.rand(rng);
            backend.submit(Op::write(buf.as_ptr(), buf.len(), offset));
        }

        match backend.wait() {
            Some(op) => {
                if op.result < 0 {
                    bail!("write error: {}", op.result);
                }
                remaining -= 1;
                if remaining % 1000 == 0 {
                    println!(
                        "remaining %: {:.0}",
                        (remaining as f64 / pos.len() as f64) * 100.0
                    );
                }
            }
            None => {
                if remaining == 0 {
                    break;
                }
            }
        }
    }

    file.flush()?;

    Ok(())
}

fn measure(o: &Opts, pos: Vec<u64>) -> Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(&o.filename)?;

    let backend = backend(&file, o);
    let mut index = 0;
    let loop_start = Instant::now();
    let mut second_start = Instant::now();
    let mut iops = 0;
    let mut ramping_up = true;
    let mut latencies_h = Histogram::<u64>::new_with_bounds(1, 1000000000, 3).unwrap();

    let mut buf_pool = BufPool::new();
    loop {
        if iops > 1000 && second_start.elapsed().as_millis() > 1000 {
            second_start = Instant::now();

            let cur = iops;
            iops = 0;
            println!("cur: {}", cur);

            if !ramping_up {
                // Print out the latencies for various percentiles.
                for q in [0.001, 0.01, 0.25, 0.50, 0.75, 0.95, 0.99, 0.999] {
                    let lat = latencies_h.value_at_quantile(q);
                    println!("{}th: {} ns", q * 100.0, lat);
                }
                println!("mean={} ns", latencies_h.mean());
            }
        }

        if ramping_up {
            if loop_start.elapsed() >= o.ramp_time {
                ramping_up = false;
            }
        }

        while !backend.is_full() {
            let offset = pos[index];
            index = (index + 1) % pos.len();

            let (buf_index, ptr, len) = buf_pool.checkout();
            let mut op = Op::read(ptr, len, offset);
            op.user_data = buf_index as u64;
            backend.submit(op)
        }

        match backend.wait() {
            Some(op) => {
                if op.result < 0 {
                    bail!("write failed: {}", op.result);
                }

                let buf_index = op.user_data as usize;
                buf_pool.release(buf_index);

                if !ramping_up {
                    // Do the bookkeeping, but only if we are not ramping up.
                    let dur = op.retired.unwrap() - op.submitted.unwrap();
                    let dur_nanos = dur.as_nanos().try_into().unwrap();
                    latencies_h.record(dur_nanos).unwrap();
                }
                iops += 1;
            }
            None => {
                panic!()
            }
        };
    }
}

struct BufPool {
    pool: Slab<Box<[u8]>>,
    free: Vec<usize>,
}

impl BufPool {
    pub fn new() -> Self {
        Self {
            pool: Slab::new(),
            free: Vec::new(),
        }
    }

    pub fn checkout(&mut self) -> (usize, *mut u8, usize) {
        let index = match self.free.pop() {
            Some(index) => index,
            _ => {
                let iovec = allocate_aligned_vec(4096, 4096);
                self.pool.insert(iovec.into_boxed_slice())
            }
        };
        let (ptr, len) = self.get_ptr_and_len(index);
        (index, ptr, len)
    }

    pub fn release(&mut self, index: usize) {
        self.free.push(index);
    }

    fn get_ptr_and_len(&self, index: usize) -> (*mut u8, usize) {
        let buf = &self.pool[index];
        (buf.as_ptr() as *mut u8, buf.len())
    }
}
