//! Definition of the command-line interface arguments.

pub use bytes_cnt::BytesCnt;
use clap::Parser;

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
    ///
    /// The size must be equal or less than 2^63.
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

    /// By default the files are sparse.
    #[clap(long, default_value = "false")]
    pub no_sparse: bool,

    #[clap(long, default_value = "false")]
    pub falloc_keep_size: bool,

    #[clap(long, default_value = "false")]
    pub falloc_zero_range: bool,
}
