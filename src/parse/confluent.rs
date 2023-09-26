use crate::parse::error::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Cursor};
use std::ops::Deref;
use tracing::trace;

pub struct ConfluentMsg {
    pub schema_id: i32,
    pub value: Vec<u8>,
}

pub fn parse_confluent(value: &[u8]) -> Result<ConfluentMsg> {
    let mut rdr: Cursor<&[u8]> = Cursor::new(value);

    let magic_byte = rdr.read_u8()?;
    trace!("magic_byte={:?}", magic_byte);
    assert_eq!(magic_byte, 0);

    let schema_id = rdr.read_i32::<BigEndian>()?;
    trace!("schema_id={:?}", schema_id);

    // confluent java parser throws these away
    let message_idx = parse_message_indexes(&mut rdr)?;
    trace!("message_idx={:?}", message_idx);

    let value = rdr.clone().into_inner()[rdr.position() as usize..].to_vec();
    Ok(ConfluentMsg { schema_id, value })
}

fn parse_message_indexes(rdr: &mut Cursor<&[u8]>) -> Result<Vec<i32>> {
    let size = read_varint(rdr)?;
    Result::from_iter((0..size).into_iter().map(|_| read_varint(rdr)))
}

fn unsigned_right_shift(x: i32, n: usize) -> u32 {
    u32::from_be_bytes(x.to_be_bytes()) >> n
}

fn unsigned_right_shift_i32(x: i32, n: usize) -> Result<i32> {
    let v = unsigned_right_shift(x, n);
    Ok(v.try_into()?)
}

#[cfg(test)]
mod test {
    use std::io::Cursor;
    use crate::parse::confluent::{read_unsigned_varint, read_varint, write_unsigned_varint, write_varint};

    // https://github.com/a0x8o/kafka/blob/54eff6af115ee647f60129f2ce6a044cb17215d0/clients/src/test/java/org/apache/kafka/common/utils/ByteUtilsTest.java#L142

    fn assert_varint_serde(value: i32, expected: &[u8]) {
        let buf = write_varint(value).unwrap();
        println!("assert_varint_serde: {:?} {:?}", buf, expected);
        assert_eq!(buf, expected);
        let read_val: i32 = read_varint(&mut Cursor::new(&buf)).unwrap();
        println!("assert_varint_serde: {} {}", value, read_val);
        assert_eq!(value, read_val);
    }

    fn assert_unsigned_varint_serde(value: i32, expected: &[u8]) {
        let buf = write_unsigned_varint(value).unwrap();
        println!("assert_unsigned_varint_serde: {:?} {:?}", buf, expected);
        assert_eq!(buf, expected);
        let read_val: i32 = read_unsigned_varint(&mut Cursor::new(&buf)).unwrap();
        println!("assert_unsigned_varint_serde: {} {}", value, read_val);
        assert_eq!(value, read_val);
    }

    #[test]
    fn test_invalid_varint() {
        let r = read_varint(&mut Cursor::new(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]));
        println!("test_invalid_varint: {:?}", r);
        assert!(r.is_err());
    }

    #[test]
    fn test_unsigned_varint() {
        assert_unsigned_varint_serde(0, &[0x00]);
        assert_unsigned_varint_serde(-1, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_unsigned_varint_serde(1, &[0x01]);
        assert_unsigned_varint_serde(63, &[0x3F]);
        assert_unsigned_varint_serde(-64, &[0xC0, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_unsigned_varint_serde(64, &[0x40]);
        assert_unsigned_varint_serde(8191, &[0xFF, 0x3F]);
        assert_unsigned_varint_serde(-8192, &[0x80, 0xC0, 0xFF, 0xFF, 0x0F]);
        assert_unsigned_varint_serde(8192, &[0x80, 0x40]);
        assert_unsigned_varint_serde(-8193, &[0xFF, 0xBF, 0xFF, 0xFF, 0x0F]);
        assert_unsigned_varint_serde(1048575, &[0xFF, 0xFF, 0x3F]);
        assert_unsigned_varint_serde(1048576, &[0x80, 0x80, 0x40]);
        assert_unsigned_varint_serde(i32::MAX, &[0xFF, 0xFF, 0xFF, 0xFF, 0x07]);
        assert_unsigned_varint_serde(i32::MIN, &[0x80, 0x80, 0x80, 0x80, 0x08]);
    }

    #[test]
    fn test_varint() {
        assert_varint_serde(0, &[0x00]);
        assert_varint_serde(-1, &[0x01]);
        assert_varint_serde(1, &[0x02]);
        assert_varint_serde(63, &[0x7E]);
        assert_varint_serde(-64, &[0x7F]);
        assert_varint_serde(64, &[0x80, 0x01]);
        assert_varint_serde(-65, &[0x81, 0x01]);
        assert_varint_serde(8191, &[0xFE, 0x7F]);
        assert_varint_serde(-8192, &[0xFF, 0x7F]);
        assert_varint_serde(8192, &[0x80, 0x80, 0x01]);
        assert_varint_serde(-8193, &[0x81, 0x80, 0x01]);
        assert_varint_serde(1048575, &[0xFE, 0xFF, 0x7F]);
        assert_varint_serde(-1048576, &[0xFF, 0xFF, 0x7F]);
        assert_varint_serde(1048576, &[0x80, 0x80, 0x80, 0x01]);
        assert_varint_serde(-1048577, &[0x81, 0x80, 0x80, 0x01]);
        assert_varint_serde(134217727, &[0xFE, 0xFF, 0xFF, 0x7F]);
        assert_varint_serde(-134217728, &[0xFF, 0xFF, 0xFF, 0x7F]);
        assert_varint_serde(134217728, &[0x80, 0x80, 0x80, 0x80, 0x01]);
        assert_varint_serde(-134217729, &[0x81, 0x80, 0x80, 0x80, 0x01]);
        assert_varint_serde(i32::MAX, &[0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
        assert_varint_serde(i32::MIN, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    }
}

fn read_varint(rdr: &mut Cursor<&[u8]>) -> Result<i32> {
    let value1 = read_unsigned_varint(rdr)?;
    Ok(unsigned_right_shift_i32(value1, 1)? ^ -(value1 & 1))
}

#[allow(overflowing_literals)]
fn write_unsigned_varint(mut value: i32) -> Result<Vec<u8>> {
    let mut out = vec![];
    while (value & 0xffffff80) != 0 {
        let b = ((value & 0x7f) | 0x80) as u8;
        out.push(b);
        value = unsigned_right_shift_i32(value, 7)?;
    }
    out.push(value as u8);
    Ok(out)
}

#[allow(overflowing_literals)]
#[allow(unused)]
fn write_varint(value: i32) -> Result<Vec<u8>> {
    write_unsigned_varint((value << 1) ^ (value >> 31))
}

#[derive(Debug)]
pub enum VarintError {
    InvalidVarint,
}

fn read_unsigned_varint(rdr: &mut Cursor<&[u8]>) -> std::result::Result<i32, VarintError> {
    let mut value: i32 = 0;
    let mut i: i32 = 0;
    while let Ok(byte) = rdr.read_u8() {
        let b: i32 = byte.into();
        if byte & 0x80 == 0 {
            value |= b << i;
            break;
        }
        value |= (b & 0x7f) << i;
        i += 7;
        if i > 28 {
            return Err(VarintError::InvalidVarint);
        }
    }
    Ok(value)
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Schema {
    pub id: i32,
    pub version: usize,
    pub schema_type: String,
    pub subject: String,
    pub references: Option<Vec<Reference>>,
    pub schema: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Reference {
    pub name: String,
    pub subject: String,
    pub version: usize,
}

pub fn parse_schemas(sl: &[u8]) -> Result<Vec<Schema>> {
    Ok(serde_json::from_slice(sl)?)
}

pub fn get_schemas_http(url: String) -> Result<Vec<Schema>> {
    //info!("getting schemas from {url}");
    let resp = reqwest::blocking::get(url)?;
    parse_schemas(resp.bytes()?.deref())
}

pub fn get_schemas_fs(path: &str) -> Result<Vec<Schema>> {
    //info!("reading schemas from {path}");
    parse_schemas(std::fs::read(path)?.as_slice())
}
