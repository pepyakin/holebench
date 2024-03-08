use anyhow::{bail, Result};
use std::fmt;

#[derive(Clone, Debug)]
pub struct BytesCnt {
    /// The number of
    num: u64,
    /// Originally supplied suffix. Assumed bytes if none.
    suffix: Option<char>,
}

impl fmt::Display for BytesCnt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.num)?;
        if let Some(suffix) = self.suffix {
            write!(f, "{suffix}")?;
        }
        Ok(())
    }
}

impl std::str::FromStr for BytesCnt {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut input = input.chars();
        let mut num_buf = String::with_capacity(32);
        let mut suffix = None::<char>;
        while let Some(ch) = input.next() {
            if ch.is_ascii_digit() {
                num_buf.push(ch);
                continue;
            }
            if num_buf.is_empty() {
                bail!("the size is supposed to start with a digit");
            }
            assert!(suffix.is_none());
            match ch {
                'k' | 'K' => {
                    suffix = Some('k');
                    break;
                }
                'm' | 'M' => {
                    suffix = Some('m');
                    break;
                }
                'g' | 'G' => {
                    suffix = Some('g');
                    break;
                }
                '_' => {
                    // separator, discard it.
                    continue;
                }
                ch => {
                    bail!("unexpected character {ch}")
                }
            }
        }
        if num_buf.is_empty() {
            bail!("empty number")
        }
        if input.next().is_some() {
            bail!("trailing input after suffix")
        }
        let num = num_buf.parse::<u64>()?;
        Ok(BytesCnt::new(num, suffix)?)
    }
}

const KB_K: u64 = 1024;
const MB_K: u64 = 1024 * KB_K;
const GB_K: u64 = 1024 * MB_K;

impl BytesCnt {
    pub fn new(num: u64, suffix: Option<char>) -> Result<Self> {
        let me = Self { num, suffix };
        let _ = me
            .to_bytes_safe()
            .ok_or_else(|| anyhow::anyhow!("{me} is too big to fit into u64"))?;
        Ok(me)
    }

    pub fn to_bytes(&self) -> u64 {
        // unwrap here is safe since should be checked by the constructor.
        self.to_bytes_safe().unwrap()
    }

    fn to_bytes_safe(&self) -> Option<u64> {
        Some(match self.suffix {
            None => self.num,
            Some('k') => self.num.checked_mul(KB_K)?,
            Some('m') => self.num.checked_mul(MB_K)?,
            Some('g') => self.num.checked_mul(GB_K)?,
            _ => unreachable!(),
        })
    }
}
