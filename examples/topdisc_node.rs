//! A topic discovery node for interoperability runs: joins a network through bootnode
//! ENRs, keeps an advertisement for one topic alive and/or searches it, and serves its
//! state as JSON on a status port.
//!
//! Every `TopicRegistered` and `TopicNodesFound` event, and the end of every search, is
//! one JSON line on stdout. Logs go to stderr (`RUST_LOG`).
//!
//! ```sh
//! cargo run --features topic-discovery --example topdisc_node -- \
//!     --port 9300 --status-port 19300 --topic <64 hex chars> \
//!     --bootnode enr:... --register --search 100
//! ```

use clap::Parser;
use discv5::{ConfigBuilder, Discv5, Enr, Event, ListenConfig, topic::TopicConfig};
use enr::{CombinedKey, NodeId, k256};
use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tracing::{info, warn};

#[derive(Parser)]
struct Args {
    /// Address to bind the UDP socket to.
    #[clap(long, default_value = "127.0.0.1")]
    listen_ip: Ipv4Addr,
    /// UDP port to listen on and to advertise.
    #[clap(long)]
    port: u16,
    /// IPv4 address advertised in the ENR.
    #[clap(long, default_value = "127.0.0.1")]
    enr_ip: Ipv4Addr,
    /// Bootnode ENR (base64, with or without the `enr:` prefix). Repeatable.
    #[clap(long)]
    bootnode: Vec<String>,
    /// A file with one bootnode ENR per line, read once it exists.
    #[clap(long)]
    bootnode_file: Option<String>,
    /// Seconds between the FINDNODE lookups that fill the routing table.
    #[clap(long, default_value_t = 30)]
    lookup_interval: u64,
    /// Topic id, 32 bytes in hex.
    #[clap(long)]
    topic: String,
    /// Other topic ids whose ads the status reports. Repeatable.
    #[clap(long)]
    report_topic: Vec<String>,
    /// Keep an advertisement for the topic alive.
    #[clap(long)]
    register: bool,
    /// Seconds to wait after start before registering.
    #[clap(long, default_value_t = 0)]
    register_after: u64,
    /// Search the topic for this many advertisers, again and again.
    #[clap(long)]
    search: Option<usize>,
    /// Adaptive search: ads per reply below which the search moves closer. 0 turns it off.
    #[clap(long, default_value_t = 8)]
    yield_floor: usize,
    /// Adaptive search: buckets around the active one to refill. -1 asks for every bucket.
    #[clap(long, default_value_t = 1)]
    aux_radius: i64,
    /// Cap on the gap between search passes, in seconds. -1 keeps 2 s.
    #[clap(long, default_value_t = 60)]
    pass_backoff: i64,
    /// Ad lifetime in seconds.
    #[clap(long, default_value_t = 900)]
    ad_lifetime: u64,
    /// How long one search runs at most, in seconds.
    #[clap(long, default_value_t = 60)]
    search_timeout: u64,
    /// TCP port of the status server.
    #[clap(long)]
    status_port: u16,
    /// Private key in hex. A random one when absent.
    #[clap(long)]
    key: Option<String>,
}

/// What the status server reports, kept up to date by the event loop.
#[derive(Default)]
struct State {
    registrars: BTreeMap<[u8; 32], u64>,
    advertisers: BTreeMap<[u8; 32], u64>,
    searches_done: usize,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn hex_id(id: &NodeId) -> String {
    hex::encode(id.raw())
}

fn json_ids<'a>(ids: impl Iterator<Item = &'a [u8; 32]>) -> String {
    let list: Vec<String> = ids.map(|id| format!("\"{}\"", hex::encode(id))).collect();
    format!("[{}]", list.join(","))
}

fn parse_enr(s: &str) -> Result<Enr, String> {
    let s = s.trim();
    let s = s.strip_prefix("enr:").unwrap_or(s);
    s.parse::<Enr>().map_err(|e| format!("{s}: {e}"))
}

#[tokio::main]
async fn main() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("info"))
        .unwrap();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
    let args = Args::parse();

    let parse_topic = |t: &str| -> [u8; 32] {
        hex::decode(t.trim_start_matches("0x"))
            .ok()
            .and_then(|b| b.try_into().ok())
            .expect("a topic must be 32 bytes in hex")
    };
    let topic = parse_topic(&args.topic);
    let mut report_topics = vec![topic];
    for t in &args.report_topic {
        let t = parse_topic(t);
        if !report_topics.contains(&t) {
            report_topics.push(t);
        }
    }
    let key = match &args.key {
        Some(k) => {
            let raw = hex::decode(k).expect("--key must be hex");
            CombinedKey::from(k256::ecdsa::SigningKey::from_slice(&raw).expect("bad key"))
        }
        None => CombinedKey::generate_secp256k1(),
    };
    let enr = Enr::builder()
        .ip4(args.enr_ip)
        .udp4(args.port)
        .build(&key)
        .unwrap();

    let mut topic_config = TopicConfig {
        ad_lifetime: Duration::from_secs(args.ad_lifetime),
        search_yield_floor: args.yield_floor,
        search_aux_radius: usize::try_from(args.aux_radius).ok(),
        search_pass_backoff: u64::try_from(args.pass_backoff)
            .ok()
            .map(Duration::from_secs),
        ..Default::default()
    };
    topic_config.reg_attempt_timeout = topic_config.ad_lifetime + topic_config.ad_lifetime / 2;
    let listen = ListenConfig::from_ip(IpAddr::V4(args.listen_ip), args.port);
    let config = ConfigBuilder::new(listen)
        .query_timeout(Duration::from_secs(args.search_timeout))
        .topic_config(topic_config)
        .build();

    let mut discv5 = Discv5::new(enr, key, config).unwrap();
    let local = discv5.local_enr();
    info!(node_id = %hex_id(&local.node_id()), enr = %local.to_base64(), "Local node");
    println!(
        "{{\"t\":{},\"event\":\"start\",\"id\":\"{}\",\"enr\":\"{}\"}}",
        unix_ms(),
        hex_id(&local.node_id()),
        local.to_base64()
    );
    for b in &args.bootnode {
        match parse_enr(b) {
            Ok(enr) => {
                if let Err(e) = discv5.add_enr(enr) {
                    warn!(error = e, "Bootnode rejected");
                }
            }
            Err(e) => warn!(error = %e, "Bad bootnode ENR"),
        }
    }
    discv5.start().await.expect("discv5 start");
    let discv5 = Arc::new(discv5);
    let state = Arc::new(Mutex::new(State::default()));
    let started = Instant::now();

    // Late bootnodes, and lookups to fill the routing table.
    {
        let discv5 = discv5.clone();
        let file = args.bootnode_file.clone();
        let interval = Duration::from_secs(args.lookup_interval.max(1));
        tokio::spawn(async move {
            let mut pending = file;
            loop {
                if let Some(path) = &pending
                    && let Ok(text) = std::fs::read_to_string(path)
                {
                    for line in text.lines().filter(|l| !l.trim().is_empty()) {
                        match parse_enr(line) {
                            Ok(enr) => {
                                info!(node_id = %hex_id(&enr.node_id()), "Adding bootnode from file");
                                if let Err(e) = discv5.add_enr(enr) {
                                    warn!(error = e, "Bootnode rejected");
                                }
                            }
                            Err(e) => warn!(error = %e, "Bad bootnode ENR"),
                        }
                    }
                    pending = None;
                }
                if discv5.table_entries_id().is_empty() && pending.is_some() {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                match discv5.find_node(NodeId::random()).await {
                    Ok(found) => println!(
                        "{{\"t\":{},\"event\":\"lookup\",\"found\":{},\"table\":{}}}",
                        unix_ms(),
                        found.len(),
                        discv5.table_entries_id().len()
                    ),
                    Err(e) => warn!(error = ?e, "Lookup failed"),
                }
                tokio::time::sleep(interval).await;
            }
        });
    }

    // Status server.
    {
        let discv5 = discv5.clone();
        let state = state.clone();
        let report_topics = report_topics.clone();
        let addr = SocketAddr::new(IpAddr::V4(args.listen_ip), args.status_port);
        let listener = TcpListener::bind(addr).await.expect("status port");
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    continue;
                };
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                let body = status_json(&discv5, &state, topic, &report_topics, started);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
    }

    // Event log.
    let mut events = discv5.event_stream().await.expect("event stream");
    {
        let state = state.clone();
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                match event {
                    Event::TopicRegistered { topic, registrar } => {
                        let t = unix_ms();
                        state.lock().unwrap().registrars.insert(registrar.raw(), t);
                        println!(
                            "{{\"t\":{t},\"event\":\"registered\",\"topic\":\"{}\",\"registrar\":\"{}\"}}",
                            hex::encode(topic),
                            hex_id(&registrar)
                        );
                    }
                    Event::TopicNodesFound { topic, nodes } => {
                        let t = unix_ms();
                        let mut st = state.lock().unwrap();
                        let ids: Vec<[u8; 32]> = nodes.iter().map(|n| n.node_id().raw()).collect();
                        let mut new = Vec::new();
                        for id in &ids {
                            if st.advertisers.insert(*id, t).is_none() {
                                new.push(*id);
                            }
                        }
                        println!(
                            "{{\"t\":{t},\"event\":\"found\",\"topic\":\"{}\",\"nodes\":{},\"new\":{}}}",
                            hex::encode(topic),
                            json_ids(ids.iter()),
                            json_ids(new.iter())
                        );
                    }
                    _ => {}
                }
            }
        });
    }

    if args.register {
        let discv5 = discv5.clone();
        let delay = Duration::from_secs(args.register_after);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            discv5.register_topic(topic).expect("register_topic");
            info!("Registering topic {}", hex::encode(topic));
            println!(
                "{{\"t\":{},\"event\":\"register\",\"topic\":\"{}\"}}",
                unix_ms(),
                hex::encode(topic)
            );
        });
    }
    if let Some(count) = args.search {
        let discv5 = discv5.clone();
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                let t0 = Instant::now();
                let result = discv5.topic_search(topic, count).await;
                let t = unix_ms();
                let n = result.as_ref().map(Vec::len).unwrap_or(0);
                let mut st = state.lock().unwrap();
                st.searches_done += 1;
                let total = st.advertisers.len();
                drop(st);
                println!(
                    "{{\"t\":{t},\"event\":\"search_end\",\"topic\":\"{}\",\"results\":{n},\"ms\":{},\"distinct_total\":{total},\"err\":{}}}",
                    hex::encode(topic),
                    t0.elapsed().as_millis(),
                    match &result {
                        Ok(_) => "null".to_string(),
                        Err(e) => format!("\"{e:?}\""),
                    }
                );
            }
        });
    }

    tokio::signal::ctrl_c().await.ok();
    info!("Shutting down");
}

fn status_json(
    discv5: &Discv5,
    state: &Mutex<State>,
    topic: [u8; 32],
    report_topics: &[[u8; 32]],
    started: Instant,
) -> String {
    let local = discv5.local_enr();
    let mut ads_held = Vec::new();
    let mut ads_held_ids = Vec::new();
    for t in report_topics {
        let ads: Vec<[u8; 32]> = discv5
            .local_topic_nodes(*t)
            .iter()
            .map(|n| n.node_id().raw())
            .collect();
        ads_held.push(format!("\"{}\":{}", hex::encode(t), ads.len()));
        ads_held_ids.push(format!("\"{}\":{}", hex::encode(t), json_ids(ads.iter())));
    }
    let metrics = Discv5::metrics();
    let st = state.lock().unwrap();
    let registered = !st.registrars.is_empty();
    format!(
        concat!(
            "{{\"id\":\"{}\",\"enr\":\"{}\",\"uptime_s\":{},\"table\":{},\"connected\":{},",
            "\"topic\":\"{}\",\"registered_at\":{},\"registrars\":{},",
            "\"ads_held\":{{{}}},\"ads_held_ids\":{{{}}},",
            "\"advertisers_found\":{},\"advertiser_ids\":{},\"searches_done\":{},",
            "\"regtopic_sent\":{},\"regtopic_recv\":{},\"topicquery_sent\":{},\"topicquery_recv\":{},",
            "\"regconfirmation_recv\":{},\"topicnodes_recv\":{},\"sessions\":{}}}"
        ),
        hex_id(&local.node_id()),
        local.to_base64(),
        started.elapsed().as_secs(),
        discv5.table_entries_id().len(),
        discv5.connected_peers(),
        hex::encode(topic),
        registered,
        json_ids(st.registrars.keys()),
        ads_held.join(","),
        ads_held_ids.join(","),
        st.advertisers.len(),
        json_ids(st.advertisers.keys()),
        st.searches_done,
        metrics.topic.regtopic_sent,
        metrics.topic.regtopic_recv,
        metrics.topic.topicquery_sent,
        metrics.topic.topicquery_recv,
        metrics.topic.regconfirmation_recv,
        metrics.topic.topicnodes_recv,
        metrics.active_sessions,
    )
}
