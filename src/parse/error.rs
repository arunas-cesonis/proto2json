use crate::parse::confluent::VarintError;
use crate::parse::protobuf::ProtobufError;
use derive_more::From;
use std::num::{ParseIntError, TryFromIntError};
use std::string::FromUtf8Error;

#[derive(Debug, From)]
pub enum Error {
    IoError(std::io::Error),
    FromUtf8Error(FromUtf8Error),
    ParseIntError(ParseIntError),
    TryFromIntError(TryFromIntError),
    Infallible(std::convert::Infallible),
    Eof,
    EolNotFound,
    NeedAtLeastOneBrokerHostname,
    NeedAtLeastOneTopic,
    Varint(VarintError),
    Protobuf(ProtobufError),
    SerdeJson(serde_json::Error),
    Reqwest(reqwest::Error),
    JsonPrint(protobuf_json_mapping::PrintError),
}

pub type Result<A> = std::result::Result<A, Error>;
