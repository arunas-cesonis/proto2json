use crate::parse::confluent::*;
use crate::parse::error::*;
use itertools::Itertools;
use protobuf::reflect::FileDescriptor;
use protobuf::MessageDyn;
use protobuf_json_mapping::PrintOptions;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tracing::{debug, info, warn};

#[derive(Debug)]
pub enum ProtobufError {
    SchemaNotFound(i32),
    CouldNotFindFileDescriptorForSchema(i32),
}

#[derive(Default)]
pub struct ProtobufFileDescriptors {
    map: BTreeMap<i32, FileDescriptor>,
}
fn same_path(a: &str, b: &str) -> bool {
    let a = a.strip_prefix("./").unwrap_or(a);
    let b = b.strip_prefix("./").unwrap_or(b);
    a == b
}

impl ProtobufFileDescriptors {
    fn get_file_descriptor(
        schemas_proto_path: &str,
        includes: &[String],
        schemas: &[Schema],
        schema_id: i32,
    ) -> Result<FileDescriptor> {
        let schema = schemas
            .iter()
            .find(|s| s.id == schema_id)
            .ok_or_else(|| ProtobufError::SchemaNotFound(schema_id))?;
        let dir = PathBuf::from(schemas_proto_path);
        //info!("schema={:?}", schema);
        let path = dir.join(format!("temp-schema-{}.proto", schema.id));
        debug!("writing {}", path.display().to_string());
        std::fs::write(&path, &schema.schema)?;

        //info!("include {:?}", includes);
        let file_descriptor_protos = protobuf_parse::Parser::new()
            .pure()
            .includes(includes)
            .input(&path)
            .parse_and_typecheck()
            .unwrap()
            .file_descriptors;

        let temp_path = path.to_str().map(|s| s.to_string()).unwrap();

        let mut deps = vec![];
        for fdp in file_descriptor_protos {
            let name = fdp.clone().name;
            let file_descriptor: protobuf::Result<FileDescriptor> =
                FileDescriptor::new_dynamic(fdp, &deps);
            let name = name.unwrap();
            if let Ok(fd) = file_descriptor {
                if same_path(&name, &temp_path) {
                    return Ok(fd);
                } else {
                    deps.push(fd);
                }
            }
        }
        Err(Error::Protobuf(
            ProtobufError::CouldNotFindFileDescriptorForSchema(schema_id),
        ))
    }

    #[tracing::instrument(skip(fd, data))]
    fn parse_message(fd: &FileDescriptor, data: Vec<u8>) -> Result<Vec<Box<dyn MessageDyn>>> {
        let mut out = vec![];
        debug!(
            "names: {:?}",
            fd.messages().map(|x| x.name().to_string()).collect_vec()
        );
        for msg in fd.messages() {
            let rr: protobuf::Result<Box<dyn MessageDyn>> = msg.parse_from_bytes(data.as_slice());
            match rr {
                Ok(parsed) => {
                    //info!(message_name = msg.name());
                    debug!("ok {}", msg.name());
                    out.push(parsed);
                }
                Err(e) => {
                    debug!("fail {}", msg.name());
                    warn!(
                        message_name = msg.name(),
                        e = e.to_string().as_str(),
                        "failed"
                    )
                }
            }
        }
        Ok(out)
    }
    pub fn parse(
        &mut self,
        schemas_proto_path: &str,
        include: &[String], // yuck
        schemas: &[Schema],
        schema_id: i32,
        data: Vec<u8>,
    ) -> Result<Vec<Box<dyn MessageDyn>>> {
        match self.map.get(&schema_id) {
            Some(fd) => Self::parse_message(fd, data),
            None => {
                let fd =
                    Self::get_file_descriptor(schemas_proto_path, include, schemas, schema_id)?;
                self.map.insert(schema_id, fd);
                Self::parse_message(self.map.get(&schema_id).unwrap(), data)
            }
        }
    }
}

pub fn to_json_strings(msg: Vec<Box<dyn MessageDyn>>) -> Result<Vec<String>> {
    let options = PrintOptions {
        enum_values_int: false,
        proto_field_name: false,
        always_output_default_values: true,
        _future_options: (),
    };
    let mut out = vec![];
    for msg in msg {
        let json_str = protobuf_json_mapping::print_to_string_with_options(&*msg, &options)
            .map_err(Error::JsonPrint)?;
        out.push(json_str);
    }
    Ok(out)
}
