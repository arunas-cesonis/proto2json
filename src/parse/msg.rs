

#[derive(Debug)]
pub struct Msg {
    pub topic: String,
    pub partition: i64,
    pub offset: i64,
    pub ts: i64,
    pub key: ParsedKey,
    pub key_len: usize,
    pub msg: Vec<u8>,
    pub msg_len: usize,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub enum ParsedKey {
    None,
    Utf8(String),
    NotUtf8(Vec<u8>),
}

impl ParsedKey {
    pub fn new(v: &[u8]) -> Self {
        if let Ok(s) = String::from_utf8(v.to_vec()) {
            ParsedKey::Utf8(s)
        } else {
            ParsedKey::NotUtf8(v.to_vec())
        }
    }
    pub fn len(&self) -> usize {
        match self {
            ParsedKey::None => 0,
            ParsedKey::Utf8(s) => s.len(),
            ParsedKey::NotUtf8(s) => s.len(),
        }
    }
}
