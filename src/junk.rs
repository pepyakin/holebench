use std::alloc::{alloc, Layout};
use std::ptr::NonNull;
use std::fmt;
use rand::{Rng, RngCore};

const K_SZ: usize = 4096;
const ALIGNMENT: usize = 4096;

/// The buffer that provides random data to be written to disk. The buffer is guaranteed to be
/// aligned to 4096 bytes.
pub struct JunkBuf {
    buf: Vec<u8>,
    bs: usize,
}

impl JunkBuf {
    pub fn new(bs: usize, rng: &mut impl RngCore) -> Self {
        let sz = bs * K_SZ;
        let mut buf = allocate_aligned_vec::<u8>(sz, ALIGNMENT);
        rng.fill_bytes(&mut buf);
        Self { buf, bs }
    }

    /// Returns a buffer ready to be written to disk.
    pub fn rand(&self, rng: &mut impl RngCore) -> &[u8] {
        let start = rng.gen_range(0..K_SZ) * self.bs;
        &self.buf[start..]
    }
}

fn allocate_aligned_vec<T>(len: usize, alignment: usize) -> Vec<T> {
    let layout = Layout::array::<T>(len).unwrap();
    let layout = Layout::from_size_align(layout.size(), alignment).unwrap();
    unsafe {
        let ptr = NonNull::new(alloc(layout)).unwrap().cast::<T>();
        Vec::from_raw_parts(ptr.as_ptr(), len, len)
    }
}

impl fmt::Debug for JunkBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JunkBuf {{ bs: {} }}", self.bs)
    }
}