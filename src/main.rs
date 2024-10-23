use anyhow::{bail, Result};
use clap::Parser;
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use rand::seq::SliceRandom;
use rand::RngCore;
use slab::Slab;
use std::fs::File;
use std::io::Write;
use std::time::{Duration, Instant};
use std::{fs::OpenOptions, os::fd::AsRawFd, path::PathBuf};

use cli::Cli;
use junk::JunkBuf;

use crate::backend::Op;

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
    #[allow(unused)]
    falloc_keep_size: bool,
    /// true if `falloc` with `FALLOC_FL_ZERO_RANGE` should be applied to the file.
    #[allow(unused)]
    falloc_zero_range: bool,
    /// Skip layout phase. Assume file exists.
    skip_layout: bool,
    /// The number of items to keep in the backlog.
    backlog_cnt: usize,
    ramp_time: Duration,
    backend: cli::Backend,
    direct: bool,
    num_jobs: usize,
}

fn parse_cli(cli: Cli) -> Result<&'static Opts> {
    let filename = PathBuf::from(&cli.filename);
    if filename.is_dir() {
        bail!("{} is a directory", filename.display());
    }
    let bs = cli.bs.to_bytes();
    if bs == 0 {
        bail!("bs can't be zero")
    }
    if !bs.is_power_of_two() {
        bail!("bs should be a power of two");
    }
    if bs < 512 {
        bail!("bs can't be less than 512");
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
        if cli.no_sparse {
            eprintln!("warning: --skip-layout prevents --no-sparse from being used");
        }
    }

    if cli.direct && matches!(cli.backend, cli::Backend::Mmap) {
        eprintln!("warning: direct I/O is not supported with mmap backend");
    }

    let o = Box::new(Opts {
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
        direct: cli.direct,
        num_jobs: cli.num_jobs,
    });
    Ok(Box::leak(o))
}

fn backend(file: &File, o: &'static Opts) -> Box<dyn crate::backend::Backend> {
    match o.backend {
        #[cfg(target_os = "linux")]
        cli::Backend::IoUring => crate::backend::io_uring::init(file.as_raw_fd(), o),
        #[cfg(not(target_os = "linux"))]
        cli::Backend::IoUring => {
            // Should be checked elsewhere.
            unreachable!()
        }
        cli::Backend::Mmap => crate::backend::mmap::init(file.as_raw_fd(), o),
        cli::Backend::Sync => crate::backend::sync::init(file.as_raw_fd(), o),
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
    o: &'static Opts,
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

    #[cfg(target_os = "linux")]
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

    let total_sz = o.bs * remaining as u64;
    let pb = ProgressBar::new(total_sz);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40}] {bytes}/{total_bytes} ({eta})")
            .unwrap(),
    );
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
                pb.inc(o.bs);
            }
            None => {
                if remaining == 0 {
                    break;
                }
            }
        }
    }

    pb.finish_with_message("flushing...");
    file.flush()?;
    println!("flushed");

    Ok(())
}

fn measure(o: &'static Opts, pos: Vec<u64>) -> Result<()> {
    let file = {
        let mut oo = OpenOptions::new();
        #[cfg(target_os = "linux")]
        if o.direct {
            use std::os::unix::fs::OpenOptionsExt as _;
            oo.custom_flags(libc::O_DIRECT);
        }
        oo.read(true);
        oo.write(true);
        oo
    }
    .open(&o.filename)?;

    let backend = backend(&file, o);
    let mut index = 0;
    let loop_start = Instant::now();
    let mut ramping_up = true;
    let mut m = Metrics::new();

    let mut buf_pool = BufPool::new(o.bs);
    loop {
        m.on_tick();

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
                    m.on_op_complete(op);
                }
            }
            None => {
                panic!()
            }
        };
    }
}

struct BufPool {
    pool: Slab<*mut u8>,
    free: Vec<usize>,
    bs: usize,
}

impl BufPool {
    pub fn new(bs: u64) -> Self {
        Self {
            pool: Slab::new(),
            free: Vec::new(),
            bs: bs.try_into().unwrap(),
        }
    }

    pub fn checkout(&mut self) -> (usize, *mut u8, usize) {
        let index = match self.free.pop() {
            Some(index) => index,
            _ => unsafe {
                let layout = std::alloc::Layout::from_size_align(self.bs, self.bs).unwrap();
                let ptr = std::alloc::alloc_zeroed(layout);
                self.pool.insert(ptr)
            },
        };
        let (ptr, len) = self.get_ptr_and_len(index);
        (index, ptr, len)
    }

    pub fn release(&mut self, index: usize) {
        self.free.push(index);
    }

    fn get_ptr_and_len(&self, index: usize) -> (*mut u8, usize) {
        let buf = self.pool[index];
        (buf, self.bs)
    }
}

struct Metrics {
    second_start: Instant,
    running_iops: usize,
    last_iops: usize,
    histogram_total: Histogram<u64>,
    histogram_completion: Histogram<u64>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            second_start: Instant::now(),
            running_iops: 0,
            last_iops: 0,
            histogram_total: Histogram::new(5).unwrap(),
            histogram_completion: Histogram::new(5).unwrap(),
        }
    }

    /// Called every now and then. Displays data if needed.
    pub fn on_tick(&mut self) {
        // Avoid checking the time too often.
        if self.running_iops < 1000 {
            return;
        }

        if self.second_start.elapsed().as_millis() < 1000 {
            return;
        }

        self.second_start = Instant::now();

        self.last_iops = self.running_iops;
        self.running_iops = 0;
        self.display();
    }

    pub fn on_op_complete(&mut self, op: Op) {
        let now = Instant::now();
        let total = now - op.created.unwrap();
        let completion = op.retired.unwrap() - op.submitted.unwrap();

        self.histogram_total
            .record(total.as_nanos() as u64)
            .unwrap();
        self.histogram_completion
            .record(completion.as_nanos() as u64)
            .unwrap();

        self.running_iops += 1;
    }

    fn display(&self) {
        println!("iops: {}", self.last_iops);
        println!(
            "total lat ns: {} (50th: {}, 99th: {})",
            self.histogram_total.mean(),
            self.histogram_total.value_at_quantile(0.50),
            self.histogram_total.value_at_quantile(0.99),
        );
        println!(
            "completion lat ns: {} (50th: {}, 99th: {})",
            self.histogram_completion.mean(),
            self.histogram_completion.value_at_quantile(0.50),
            self.histogram_completion.value_at_quantile(0.99),
        );
    }
}
