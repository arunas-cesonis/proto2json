use crate::parse::msg::{Msg, ParsedKey};
use fnv::{FnvHashMap, FnvHashSet};
use itertools::Itertools;
use rdkafka::admin::AdminClient;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::Offset;
use rdkafka::{Message, TopicPartitionList};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use tracing::info;
use uuid::Uuid;

const MAX_RETRIES: usize = 10;

#[derive(Debug, Clone)]
struct Watermark {
    lo: i64,
    hi: i64,
    received: i64,
}

struct Iter {
    consumer: BaseConsumer<DefaultConsumerContext>,
    positions: HashMap<(String, i32), Vec<Offset>>,
    watermarks: fnv::FnvHashMap<(String, i32), Watermark>,
    attempts: usize,
    messages_received: usize,
    retries: usize,
}

fn parse(m: BorrowedMessage) -> Msg {
    let key = m
        .key()
        .map(|bytes| ParsedKey::new(bytes))
        .unwrap_or(ParsedKey::None);
    let msg = m.payload().map(|bytes| bytes.to_vec()).unwrap_or(vec![]);
    let key_len = key.len();
    let msg_len = msg.len();
    Msg {
        topic: m.topic().to_string(),
        partition: m.partition() as i64,
        offset: m.offset() as i64,
        ts: m.timestamp().to_millis().unwrap(),
        key,
        key_len,
        msg,
        msg_len,
    }
}

impl Iter {
    fn count_missing(&self, tpl: TopicPartitionList) -> usize {
        let mut missing = self.watermarks.len();
        for tp in tpl.elements() {
            if let Offset::Offset(offset) = tp.offset() {
                let wm = self
                    .watermarks
                    .get(&(tp.topic().to_owned(), tp.partition()))
                    .unwrap();
                if (offset >= wm.hi) {
                    missing -= 1;
                }
            }
        }
        missing
    }
}

impl Iterator for Iter {
    type Item = Msg;
    fn next(&mut self) -> Option<Self::Item> {
        if self.retries == 0 {
            return None;
        }
        loop {
            self.attempts += 1;
            let missing = self.count_missing(self.consumer.position().unwrap());
            if (missing == 0) {
                return None;
            }
            let r = self.consumer.poll(Duration::from_secs(1));
            match r {
                Some(Ok(msg)) => {
                    self.retries = MAX_RETRIES;
                    self.messages_received += 1;
                    let key = (msg.topic().to_owned(), msg.partition());
                    if let Some(mut wm) = self.watermarks.get_mut(&key) {
                        wm.received += 1;
                    } else {
                        panic!("Received message on unknown topic partition: {:?}", key);
                    }
                    return Some(parse(msg));
                }
                Some(Err(e)) => panic!("rdkafka error: {:?}", e),
                None if self.retries > 0 => {
                    info!(missing = missing, retries = self.retries);
                    self.retries -= 1;
                }
                None => {
                    info!(missing = missing, retries = self.retries);
                    info!("exiting kafka poll loop");
                    break;
                }
            };
        }
        None
    }
}

pub fn list_topics(servers_csv: &str) -> impl Iterator<Item = (String, Vec<i32>)> {
    let mut config = rdkafka::config::ClientConfig::new();
    config.set("bootstrap.servers", servers_csv);
    config.set("enable.auto.commit", "false");
    config.set("isolation.level", "read_committed");
    config.set("group.id", Uuid::new_v4().to_string().as_str());
    config.set("auto.offset.reset", "beginning");
    config.set("debug", "all");
    let client: BaseConsumer = config.create().unwrap();
    let md = client.fetch_metadata(None, Timeout::Never).unwrap();
    let mut out = vec![];
    for x in md.topics() {
        let p = x.partitions().into_iter().map(|p| p.id()).collect_vec();
        out.push((x.name().to_string(), p))
    }
    out.into_iter()
}

pub struct Topic {
    name: String,
    partition: i32,
    end_offset: Offset,
}

pub fn read_kafka(servers_csv: &str, user_topics: &[&str]) -> impl Iterator<Item = Msg> {
    let mut config = rdkafka::config::ClientConfig::new();
    config.set("bootstrap.servers", servers_csv);
    config.set("enable.auto.commit", "false");
    config.set("isolation.level", "read_committed");
    config.set("group.id", Uuid::new_v4().to_string().as_str());
    config.set("auto.offset.reset", "beginning");
    config.set("debug", "all");
    let consumer: BaseConsumer = config.create().unwrap();
    let mut partitions = vec![];
    for topic in user_topics {
        let md = consumer
            .fetch_metadata(Some(topic), Timeout::Never)
            .unwrap();
        md.topics()
            .iter()
            .flat_map(|t| {
                t.partitions()
                    .into_iter()
                    .map(|p| (t.name().to_owned(), p.id()))
            })
            .for_each(|el| {
                partitions.push(el);
            });
    }

    drop(consumer);

    let consumer: BaseConsumer = config.create().unwrap();
    let mut watermarks = FnvHashMap::default();
    for (t, p) in partitions {
        let (lo, hi) = consumer.fetch_watermarks(&t, p, Timeout::Never).unwrap();
        info!(topic = t, partition = p, lo = lo, hi = hi, d = hi - lo);
        watermarks.insert(
            (t.to_string(), p),
            Watermark {
                lo,
                hi,
                received: 0,
            },
        );
        //eprintln!("{:?}", consumer.position());
    }
    let topics = watermarks
        .keys()
        .into_iter()
        .map(|(t, _)| t.as_str())
        .collect::<FnvHashSet<_>>()
        .into_iter()
        .collect_vec();
    let consumer: BaseConsumer = config.create().unwrap();
    info!("subscribing to {:?}", topics);
    consumer.subscribe(&topics).unwrap();
    info!("subscribed to {:?}", topics);
    let it = Iter {
        consumer,
        positions: HashMap::new(),
        watermarks,
        retries: MAX_RETRIES,
        messages_received: 0,
        attempts: 0,
    };
    it
}
