use crate::parse::confluent::{get_schemas_fs, get_schemas_http, parse_confluent, Schema};
use crate::parse::error::*;
use crate::parse::msg::Msg;
use crate::parse::protobuf::{to_json_strings, ProtobufFileDescriptors};
use tracing::{debug, span, warn, Level};

pub struct Proto2Json {
    schemas_proto_path: Option<String>,
    schemas: Vec<Schema>,
    includes: Vec<String>,
    pfd: ProtobufFileDescriptors,
}

impl Proto2Json {
    pub fn load(
        schemas_proto_path: Option<String>,
        includes: Option<Vec<String>>,
        schemas_url: Option<String>,
        schemas_path: Option<String>,
    ) -> Result<Proto2Json> {
        let schemas: Vec<Schema> = match schemas_url {
            Some(url) => match get_schemas_http(url) {
                Ok(schemas) => schemas,
                Err(_) => {
                    warn!("could not get schemas via HTTP, continuing without");
                    vec![]
                }
            },
            None => vec![],
        };
        let mut includes = includes.unwrap_or_default();
        includes.push(".".to_string());

        let pfd = ProtobufFileDescriptors::default();
        Ok(Proto2Json {
            schemas_proto_path,
            schemas,
            includes,
            pfd,
        })
    }
    pub fn proto2json(&mut self, msg: &Msg) -> Result<Vec<String>> {
        let key = format!("{:?}", msg.key);
        let sp = span!(
            Level::INFO,
            "message",
            topic = msg.topic.as_str(),
            partition = msg.partition,
            offset = msg.offset,
            key = key.as_str()
        );
        let _enter = sp.enter();
        debug!(
            topic = msg.topic.as_str(),
            partition = msg.partition,
            offset = msg.offset,
            key = format!("{:?}", msg.key).as_str(),
            len = msg.msg.len(),
            "message"
        );
        // Try parsing as JSON first
        let json: serde_json::Result<serde_json::Value> = serde_json::from_slice(&msg.msg);
        if let Ok(json) = json {
            return Ok(vec![json.to_string()]);
        }
        let msg = parse_confluent(&msg.msg)?;
        debug!(
            schema_id = msg.schema_id,
            len = msg.value.len(),
            "confluent"
        );
        let msg = self.pfd.parse(
            &self.schemas_proto_path.as_deref().unwrap_or("."),
            &self.includes,
            &self.schemas,
            msg.schema_id,
            msg.value,
        )?;
        to_json_strings(msg)
    }
}
