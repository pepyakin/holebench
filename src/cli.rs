//! Definition of the command-line interface arguments.

use anyhow::{bail, Result};
use clap::Parser;
pub use bytes_cnt::BytesCnt;

mod bytes_cnt;

#[derive(Parser, Debug)]
pub struct Cli {
    #[clap(long)]
    pub filename: String,

    /// The block size to use for the test.
    /// 
    /// can't be zero.
    #[clap(long, default_value = "4096")]
    pub bs: BytesCnt,

    /// The size of the file to create.
    #[clap(long)]
    pub size: BytesCnt,

    /// The sparsiness ratio of the file. 1 means that the file is not sparse at all, 0 means that
    /// the file is completely sparse.
    #[clap(long)]
    pub ratio: f64,

    #[clap(long, default_value = "1")]
    pub numjobs: u64,

    /// The number of seconds we should run the test before performing the measurements.
    #[clap(long, default_value = "2")]
    pub ramp_time: u64,

    /// The number of seconds to run the test.
    #[clap(long, default_value = "60")]
    pub run_time: u64,
}

impl Cli {
    pub fn validate(&self) -> Result<()> {
        if self.ratio < 0.0 || self.ratio > 1.0 {
            bail!("--ratio must be within 0..1");
        }
        if self.bs.to_bytes() == 0 {
            bail!("bs can't be zero")
        }
        if self.size.to_bytes() % self.bs.to_bytes() != 0 {
            bail!("the size should be a multiple of block size");
        }
        Ok(())
    }
}
