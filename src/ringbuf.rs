use std::{
    mem,
    sync::{
        Mutex,
        Arc,
    },
};

// pub fn scsc<T: Send>(cap: usize) -> (Producer<T>, Consumer<T>) {
//     let rb = RingBuf::new(cap);
//     rb.split()
// }

pub struct RingBuf<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T: Send> RingBuf<T> {
    pub fn new(cap: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::new(cap))),
        }
    }

    pub fn producer(&self) -> Producer<T> {
        Producer {
            inner: self.inner.clone(),
        }
    } 

    pub fn consumer(&self) -> Consumer<T> {
        Consumer {
            inner: self.inner.clone()
        }
    }
}


struct Inner<T> {
    buf: *mut T,
    cap: usize,
    head: usize,
    tail: usize,
}

impl<T> Inner<T> {
    fn new(cap: usize) -> Self {
        // we don't want to deal with complexities of allocating a buffer, so just punt on vec.
        let mut vec = Vec::with_capacity(cap);
        let buf = vec.as_mut_ptr();
        mem::forget(vec);
        Self {
            buf,
            cap,
            head: 0,
            tail: 0,
        }
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let mut head = self.head;
        let tail = self.tail;
        while head != tail {
            unsafe {
                let ptr = self.buf.offset(head as isize);
                let v = std::ptr::read(ptr);
                drop(v);
            }
            head = (head + 1) % self.cap;
        }
        unsafe {
            // SAFETY: the buf and capacity are the same that was
            // created the vector. The lenght is 0 and all items
            // should be cleared already.
            let vec = Vec::from_raw_parts(self.buf, 0, self.cap);
            drop(vec);
        }
    }
}

pub struct Producer<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T: Send> Producer<T> {
    pub fn push(&mut self, v: T) -> Result<(), T> {
        let mut inner = self.inner.lock().unwrap();
        if inner.head.wrapping_sub(inner.tail) == inner.cap {
            return Err(v);
        }
        unsafe {
            let ptr = inner.buf.offset(inner.tail as isize);
            std::ptr::write(ptr, v);
        }
        inner.tail = (inner.tail + 1) % self.cap();
        Ok(())
    }

    pub fn len(&self) -> usize {
        let mut inner = self.inner.lock().unwrap();
        inner.head.wrapping_sub(inner.tail)
    }

    pub fn cap(&self) -> usize {
        self.inner.lock().unwrap().cap
    }
}

pub struct Consumer<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T: Send> Consumer<T> {
    pub fn pop(&mut self) -> Option<T> {
        let mut inner = self.inner.lock().unwrap();
        if inner.head == inner.tail {
            return None;
        }
        let v = unsafe {
            let ptr = inner.buf.offset(inner.head as isize);
            std::ptr::read(ptr)
        };
        inner.head = (inner.head + 1) % inner.cap;
        Some(v)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        let mut inner = self.inner.lock().unwrap();
        inner.head.wrapping_sub(inner.tail)
    }
}
