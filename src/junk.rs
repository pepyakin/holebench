use rand::distributions::{Distribution, Uniform};
use rand::RngCore;
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::fmt;

// The number of pages to generate.
const K_SZ: usize = 8192;
const ALIGNMENT: usize = 4096;

/// The buffer that provides random data to be written to disk. The buffer is guaranteed to be
/// aligned to be aligned to the specified block size.
pub struct JunkBuf {
    /// A memory allocation containing `K_SZ` of `bs`-sized blocks totalling `n_bytes` of bytes.
    buf: *mut u8,
    /// A block size. Is not equal to 0.
    bs: usize,
    n_bytes: usize,
    dist: Uniform<usize>,
}

impl JunkBuf {
    pub fn new(bs: usize, rng: &mut impl RngCore) -> Self {
        // Ensure that the `bs` is a power of two and is not zero.
        assert!(bs.count_ones() == 1);
        assert!(bs >= 512);
        let n_bytes = bs * K_SZ;
        let layout = Layout::from_size_align(n_bytes, bs).unwrap();
        unsafe {
            // Why alloc_zeroed if we are going to initialize the memory region just a little
            // further down the line? Well, the subtlety lies in the fact that `RngCore` is a trait
            // that takes a `&mut [u8]` which doesn't only allow to write stuff, but also to read
            // stuff, which in this case would be UB. This is not performance sensitive code so
            // we just err on the safer side and get an initialized allocation.
            //
            // SAFETY: the request size is not zero.
            let buf = alloc_zeroed(layout);
            let mut bytes = std::slice::from_raw_parts_mut(buf, n_bytes);
            rng.fill_bytes(&mut bytes);
            Self {
                buf,
                bs,
                n_bytes,
                dist: Uniform::new(0, K_SZ),
            }
        }
    }

    /// Returns a buffer ready to be written to disk.
    pub fn rand(&self, rng: &mut impl RngCore) -> &[u8] {
        // Sample a random block index from 0 to K_SZ.
        let rnd_blk_idx = self.dist.sample(rng);
        let start_ofs = rnd_blk_idx * self.bs;
        assert!(start_ofs < self.n_bytes);
        let start_ofs: isize = start_ofs.try_into().unwrap();
        unsafe {
            // SAFETY: we don't do a rigorious proof here, but practically the offset should not
            //         overflow isize. As shown above, the new pointer should be within the same
            //         allocation.
            let ptr = self.buf.offset(start_ofs);
            assert!((ptr as usize) % self.bs == 0);
            // SAFETY: - ptr is a pointer that is at least `self.bs` bytes short of the end of the
            //           allocation and thus the last byte of the slice should be within the
            //           allocation boundaries.
            //
            //         - the data at ptr is initialized, first by zeroing and then by writing
            //           a bunch of random data over.
            //
            //         - the underlying memory is written only once before this function can be
            //           called.
            //
            //         - the lifetime of the returned slice is tied to self, which is the owner
            //           of the backing store.
            std::slice::from_raw_parts(ptr, self.bs)
        }
    }
}

impl Drop for JunkBuf {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.n_bytes, ALIGNMENT).unwrap();
        unsafe {
            // SAFETY: self.buf was allocated by GlobalAlloc::alloc and layout is the same as was
            //         used for allocation.
            dealloc(self.buf, layout);
        }
    }
}

impl fmt::Debug for JunkBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JunkBuf {{ bs: {} }}", self.bs)
    }
}
