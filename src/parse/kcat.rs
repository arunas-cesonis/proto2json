use crate::parse::error::*;
use crate::parse::msg::{Msg, ParsedKey};
use byteorder::ReadBytesExt;
use std::fmt::Debug;
use std::io::{BufRead, BufReader, Read};
use tracing::{debug, trace};

pub struct Parser<R> {
    r: BufReader<R>,
}

impl<R> Iterator for Parser<R>
where
    R: Read + Debug,
{
    type Item = Msg;
    fn next(&mut self) -> Option<Self::Item> {
        match self.parse_next() {
            Ok(msg) => Some(msg),
            Err(Error::Eof) => None,
            Err(other) => panic!("{:?}", other),
        }
    }
}

impl<R> Parser<R>
where
    R: Read + Debug,
{
    #[tracing::instrument]
    pub fn new(r: R) -> Self {
        trace!("new");
        Parser {
            r: BufReader::new(r),
        }
    }
    #[tracing::instrument(skip(self))]
    fn token(&mut self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(16 * 4096);
        self.r.read_until(b'\n', &mut buf)?;
        if buf.is_empty() {
            debug!("eof");
            return Err(Error::Eof);
        }
        if buf.last().copied() == Some(b'\n') {
            buf.pop();
        } else {
            return Err(Error::EolNotFound);
        }
        trace!(buf = format!("{:?}", buf));
        Ok(buf)
    }

    #[tracing::instrument(skip(self))]
    fn string(&mut self) -> Result<String> {
        let a = self.token()?;
        let s = String::from_utf8(a)?;
        trace!(s = format!("{:?}", s));
        Ok(s)
    }

    #[tracing::instrument(skip(self))]
    fn string_int(&mut self) -> Result<i64> {
        let s = self.string()?;
        let i = s.parse::<i64>()?;
        trace!(i = format!("{:?}", i));
        Ok(i)
    }

    #[tracing::instrument(skip(self))]
    fn exact(&mut self, n: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; n];
        self.r.read_exact(&mut buf)?;
        self.consume_eol().unwrap();
        trace!(buf = format!("{:?}", buf));
        Ok(buf)
    }

    #[tracing::instrument(skip(self))]
    fn consume_eol(&mut self) -> Result<()> {
        trace!("");
        assert_eq!(self.r.read_u8()?, b'\n');
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn exact_string(&mut self, n: usize) -> Result<String> {
        Ok(String::from_utf8(self.exact(n)?)?)
    }

    #[tracing::instrument(skip(self))]
    fn parse_next(&mut self) -> Result<Msg> {
        let topic = self.string()?;
        debug!(topic = topic.as_str());
        let partition = self.string_int()?;
        debug!(partition = partition);
        let offset = self.string_int()?;
        debug!(offset = offset);
        let ts = self.string_int()?;
        debug!(ts = ts);
        let key_len = self.string_int()?;
        debug!(key_len = key_len);
        let key = ParsedKey::new(self.exact(key_len.try_into()?)?.as_slice());
        debug!(key = format!("{:?}", key).as_str());
        let msg_len = self.string_int()?;
        debug!(msg_len = msg_len);
        let msg = self.exact(msg_len.try_into()?)?;
        debug!(msg = msg.len());
        Ok(Msg {
            topic,
            partition,
            offset,
            ts,
            key_len: key_len.try_into()?,
            key,
            msg,
            msg_len: msg_len.try_into()?,
        })
    }
}
