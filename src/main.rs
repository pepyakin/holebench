use anyhow::{bail, Context, Result};
use clap::Parser;
use rand::seq::SliceRandom;
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
        .custom_flags(libc::O_DIRECT | libc::O_DSYNC)
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

    let layout_thread_num = 4;
    let chunk_sz = total_chunks / layout_thread_num;
    let threads: Vec<_> = gd
        .pos
        .chunks(chunk_sz as usize)
        .enumerate()
        .map(|(job_id, chunk)| {
            let td = ThreadData {
                job_id,
                gd: gd.clone(),
                pos: chunk.to_vec(),
            };
            std::thread::spawn(move || write_chunks(&td))
        })
        .collect();
    threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Result<_, _>>()?;

    Ok(())
}

fn write_chunks(td: &ThreadData) -> Result<(), anyhow::Error> {
    let mut rng = rng();
    for (index, &ofs) in td.pos.iter().enumerate() {
        let buf = td.gd.junk.rand(&mut rng);
        td.gd.file.write_all_at(&buf, ofs).with_context(|| {
            format!(
                "failed to write to {}, job_id={}, index={index}, offset={ofs}",
                td.gd.filename.display(),
                td.job_id,
            )
        })?;
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
