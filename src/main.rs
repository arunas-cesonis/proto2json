extern crate core;

use std::fmt::Debug;
use std::process::CommandArgs;

use clap::Parser;
use tracing;
use tracing::warn;

use crate::parse::error::*;
use crate::parse::kafka;
use crate::parse::proto2json::Proto2Json;

mod parse;

fn setup_tracing(level: tracing_subscriber::filter::LevelFilter) {
    let t = tracing_subscriber::fmt::time::Uptime::default();
    let fmt = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_timer(t)
        .with_thread_ids(false)
        .compact()
        .finish();
    tracing::subscriber::set_global_default(fmt).unwrap();
}

#[derive(Debug, Clone, clap::Args)]
struct Verbosity {
    /// verbosity level, can be specified up to 3 times
    #[arg(long, short = 'v', action = clap::ArgAction::Count, global = true)]
    verbose: u8,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum Source {
    Kafka,
    Kcat,
}

#[derive(Debug, Clone, clap::Args)]
struct BrokerArgs {
    /// broker csv list (required when in kafka mode)
    #[arg(short, long, required = true)]
    brokers: String,
}

#[derive(Debug, Clone, clap::Args)]
struct DumpJsonArgs {
    /// include paths for .proto files
    #[arg(short = 'I', long = "include")]
    include: Option<Vec<String>>,

    /// url to confluent's schemas endpoint, e.g. http://127.0.0.1:8081/schemas
    ///
    /// if schema_path is specified the results of both is concatenated with one from url taking precedence
    #[arg(long, required = false)]
    schemas_url: Option<String>,

    /// selects whether messages should be from kafka or kcat
    #[arg(short = 'm', long, required = true)]
    source: Source,

    #[command(flatten)]
    brokers_args: BrokerArgs,

    /// topic csv list
    #[arg(short, long, required = false)]
    topics: String,

    /// path to store .proto files  (the protobuf library needs them to be on disk)
    ///
    /// if unset, current directory will be used
    #[arg(long, required = false)]
    schemas_proto_path: Option<String>,

    /// file path to confluent's schemas JSON in format it is served on /schemas endpoint
    ///
    /// if schema_path is specified the results of both is concatenated with one from url taking precedence
    #[arg(long, required = false)]
    schemas_path: Option<String>,

    #[command(flatten)]
    verbosity: Verbosity,
}

#[derive(Debug, Clone, clap::Parser)]
enum Command {
    // #[clap(name = "list-topics")]
    ListTopics(BrokerArgs),
    ProtoToJson(DumpJsonArgs),
}

#[derive(Debug, Clone, clap::Parser)]
struct Arguments2 {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Debug, Clone, clap::Parser)]
struct Arguments {
    #[command(flatten)]
    parser: DumpJsonArgs,
}

fn main() {
    let args = Arguments2::parse();
    match args.cmd {
        Command::ListTopics(args) => kafka::list_topics(args.brokers.as_str())
            .for_each(|t| t.1.into_iter().for_each(|id| println!("{} {}", t.0, id))),
        Command::ProtoToJson(args) => dump_json(args).unwrap(),
    }
}

fn dump_json(args: DumpJsonArgs) -> Result<()> {
    let verbosity_level = match args.verbosity.verbose {
        0 => tracing_subscriber::filter::LevelFilter::WARN,
        1 => tracing_subscriber::filter::LevelFilter::INFO,
        _ => tracing_subscriber::filter::LevelFilter::TRACE,
    };
    setup_tracing(verbosity_level);
    let mut p = Proto2Json::load(
        args.schemas_proto_path,
        args.include,
        args.schemas_url,
        args.schemas_path,
    )?;
    match args.source {
        Source::Kafka => {
            let brokers = args.brokers_args.brokers;
            let topics = args.topics;
            let rd = kafka::read_kafka(
                brokers.as_str(),
                &topics.split(",").into_iter().collect::<Vec<_>>(),
            );
            for msg in rd {
                match p.proto2json(&msg) {
                    Ok(out) => out.into_iter().for_each(|s| println!("{}", s)),
                    Err(e) => {
                        warn!(
                            topic = msg.topic,
                            partition = msg.partition,
                            offset = msg.offset,
                            "failed to parse message: {:?}",
                            e
                        )
                    }
                }
            }
        }
        Source::Kcat => {
            for msg in parse::kcat::Parser::new(std::io::stdin()) {
                p.proto2json(&msg)?
                    .into_iter()
                    .for_each(|s| println!("{}", s));
            }
        }
    };
    Ok(())
}
