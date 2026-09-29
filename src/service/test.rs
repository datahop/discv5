#![cfg(test)]

use super::*;

use crate::{
    ConfigBuilder, Enr,
    discv5::test::generate_deterministic_keypair,
    handler::Handler,
    kbucket,
    kbucket::{BucketInsertResult, KBucketsTable, NodeStatus},
    metrics::METRICS,
    node_info::NodeContact,
    query_pool::{QueryId, QueryPool},
    rpc::RequestId,
    service::{ActiveRequest, Service},
    socket::ListenConfig,
};
use enr::CombinedKey;
use parking_lot::RwLock;
use rand;
use std::{
    collections::HashMap,
    net::{Ipv4Addr, Ipv6Addr},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::sync::{
    mpsc,
    mpsc::{Sender, UnboundedReceiver},
    oneshot,
};

/// Default UDP port number to use for tests requiring UDP exposure
pub const DEFAULT_UDP_PORT: u16 = 0;

fn _connected_state() -> NodeStatus {
    NodeStatus {
        state: ConnectionState::Connected,
        direction: ConnectionDirection::Outgoing,
    }
}

fn disconnected_state() -> NodeStatus {
    NodeStatus {
        state: ConnectionState::Disconnected,
        direction: ConnectionDirection::Outgoing,
    }
}

fn init() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

async fn build_service_with(
    local_enr: Arc<RwLock<Enr>>,
    enr_key: Arc<RwLock<CombinedKey>>,
    filters: bool,
    listen_config: ListenConfig,
    ip_mode: IpMode,
) -> Service {
    let config = ConfigBuilder::new(listen_config)
        .executor(Box::<crate::executor::TokioExecutor>::default())
        .build();
    // build the session service
    let (_handler_exit, handler_send, handler_recv) =
        Handler::spawn(local_enr.clone(), enr_key.clone(), config.clone())
            .await
            .unwrap();

    let (table_filter, bucket_filter) = if filters {
        (
            Some(Box::new(kbucket::filter::IpTableFilter) as Box<dyn kbucket::filter::Filter<Enr>>),
            Some(Box::new(kbucket::filter::IpBucketFilter) as Box<dyn kbucket::filter::Filter<Enr>>),
        )
    } else {
        (None, None)
    };

    let kbuckets = Arc::new(RwLock::new(KBucketsTable::new(
        local_enr.read().node_id().into(),
        Duration::from_secs(60),
        config.incoming_bucket_limit,
        table_filter,
        bucket_filter,
    )));

    // create the required channels
    let (_discv5_send, discv5_recv) = mpsc::channel(30);
    let (_exit_send, exit) = oneshot::channel();

    let connectivity_state = ConnectivityState::new(config.auto_nat_listen_duration);

    Service {
        local_enr,
        enr_key,
        kbuckets,
        queries: QueryPool::new(config.query_timeout),
        active_requests: Default::default(),
        active_nodes_responses: HashMap::new(),
        ip_votes: None,
        handler_send,
        handler_recv,
        handler_exit: Some(_handler_exit),
        peers_to_ping: HashSetDelay::new(config.ping_interval),
        discv5_recv,
        event_stream: None,
        exit,
        #[cfg(feature = "topic-discovery")]
        topic_table: crate::topic::TopicTable::new(config.topic.clone()),
        #[cfg(feature = "topic-discovery")]
        ticket_sealer: crate::topic::TicketSealer::new(std::time::Instant::now()),
        config,
        ip_mode,
        connectivity_state,
    }
}

async fn build_service(
    local_enr: Arc<RwLock<Enr>>,
    enr_key: Arc<RwLock<CombinedKey>>,
    filters: bool,
) -> Service {
    let listen_config = ListenConfig::Ipv4 {
        ip: local_enr.read().ip4().unwrap(),
        port: local_enr.read().udp4().unwrap(),
    };
    build_service_with(local_enr, enr_key, filters, listen_config, IpMode::Ip4).await
}

async fn build_service_ipv6(
    local_enr: Arc<RwLock<Enr>>,
    enr_key: Arc<RwLock<CombinedKey>>,
    filters: bool,
) -> Service {
    let listen_config = ListenConfig::Ipv6 {
        ip: local_enr.read().ip6().unwrap(),
        port: local_enr.read().udp6().unwrap(),
    };
    build_service_with(local_enr, enr_key, filters, listen_config, IpMode::Ip6).await
}

fn build_non_handler_service(
    local_enr: Arc<RwLock<Enr>>,
    enr_key: Arc<RwLock<CombinedKey>>,
    filters: bool,
) -> (Service, UnboundedReceiver<HandlerIn>, Sender<HandlerOut>) {
    let listen_config = ListenConfig::Ipv4 {
        ip: local_enr.read().ip4().unwrap(),
        port: local_enr.read().udp4().unwrap(),
    };
    let config = ConfigBuilder::new(listen_config).build();

    // Fake's the handler with empty channels.
    let (handler_send, handler_recv_fake) = mpsc::unbounded_channel();
    let (handler_send_fake, handler_recv) = mpsc::channel(1000);

    let (table_filter, bucket_filter) = if filters {
        (
            Some(Box::new(kbucket::filter::IpTableFilter) as Box<dyn kbucket::filter::Filter<Enr>>),
            Some(Box::new(kbucket::filter::IpBucketFilter) as Box<dyn kbucket::filter::Filter<Enr>>),
        )
    } else {
        (None, None)
    };

    let kbuckets = Arc::new(RwLock::new(KBucketsTable::new(
        local_enr.read().node_id().into(),
        Duration::from_secs(60),
        config.incoming_bucket_limit,
        table_filter,
        bucket_filter,
    )));

    let ip_vote = IpVote::new(10, Duration::from_secs(10000));

    // create the required channels.
    let (_discv5_send, discv5_recv) = mpsc::channel(30);
    let (_exit_send, exit) = oneshot::channel();

    let connectivity_state = ConnectivityState::new(config.auto_nat_listen_duration);

    let service = Service {
        local_enr,
        enr_key,
        kbuckets,
        queries: QueryPool::new(config.query_timeout),
        active_requests: Default::default(),
        active_nodes_responses: HashMap::new(),
        ip_votes: Some(ip_vote),
        handler_send,
        handler_recv,
        handler_exit: None,
        peers_to_ping: HashSetDelay::new(config.ping_interval),
        discv5_recv,
        event_stream: None,
        exit,
        #[cfg(feature = "topic-discovery")]
        topic_table: crate::topic::TopicTable::new(config.topic.clone()),
        #[cfg(feature = "topic-discovery")]
        ticket_sealer: crate::topic::TicketSealer::new(std::time::Instant::now()),
        config,
        ip_mode: IpMode::DualStack,
        connectivity_state,
    };
    (service, handler_recv_fake, handler_send_fake)
}

#[tokio::test]
async fn test_updating_connection_on_ping() {
    init();
    let enr_key1 = CombinedKey::generate_secp256k1();
    let ip = "127.0.0.1".parse().unwrap();
    let enr = Enr::builder()
        .ip4(ip)
        .udp4(DEFAULT_UDP_PORT)
        .build(&enr_key1)
        .unwrap();
    let ip2 = "127.0.0.1".parse().unwrap();
    let enr_key2 = CombinedKey::generate_secp256k1();
    let enr2 = Enr::builder()
        .ip4(ip2)
        .udp4(DEFAULT_UDP_PORT)
        .build(&enr_key2)
        .unwrap();

    let mut service = build_service(
        Arc::new(RwLock::new(enr)),
        Arc::new(RwLock::new(enr_key1)),
        false,
    )
    .await;
    // Set up service with one disconnected node
    let key = kbucket::Key::from(enr2.node_id());
    if let kbucket::Entry::Absent(entry) = service.kbuckets.write().entry(&key) {
        match entry.insert(enr2.clone(), disconnected_state()) {
            BucketInsertResult::Inserted => {}
            BucketInsertResult::Full => {
                panic!("Can't be full");
            }
            BucketInsertResult::Pending { .. } => {}
            _ => panic!("Could not be inserted"),
        }
    }

    // Add a fake request
    let response = Response {
        id: RequestId(vec![1]),
        body: ResponseBody::Pong {
            enr_seq: 2,
            ip: ip2.into(),
            port: 9000.try_into().unwrap(),
        },
    };

    let node_contact: NodeContact = enr2.into();
    let expected_return_addr = node_contact.node_address();

    service.active_requests.insert(
        RequestId(vec![1]),
        ActiveRequest {
            contact: node_contact,
            request_body: RequestBody::Ping { enr_seq: 2 },
            query_id: Some(QueryId(1)),
            callback: None,
        },
    );

    // Handle the ping and expect the disconnected Node to become connected
    service.handle_rpc_response(expected_return_addr, response);
    let buckets = service.kbuckets.read();
    let node = buckets.iter_ref().next().unwrap();
    assert!(node.status.is_connected())
}

#[tokio::test]
async fn test_connection_direction_on_inject_session_established() {
    init();

    let enr_key1 = CombinedKey::generate_secp256k1();
    let ip = std::net::Ipv4Addr::LOCALHOST;
    let enr = Enr::builder()
        .ip4(ip)
        .udp4(DEFAULT_UDP_PORT)
        .build(&enr_key1)
        .unwrap();

    let enr_key2 = CombinedKey::generate_secp256k1();
    let ip2 = std::net::Ipv4Addr::LOCALHOST;
    let enr2 = Enr::builder()
        .ip4(ip2)
        .udp4(DEFAULT_UDP_PORT)
        .build(&enr_key2)
        .unwrap();

    let mut service = build_service(
        Arc::new(RwLock::new(enr)),
        Arc::new(RwLock::new(enr_key1)),
        false,
    )
    .await;

    let key = &kbucket::Key::from(enr2.node_id());

    let dummy_socket = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 80);
    // Test that the existing connection direction is not updated.
    // Incoming
    service.inject_session_established(enr2.clone(), &dummy_socket, ConnectionDirection::Incoming);
    let status = service.kbuckets.read().iter_ref().next().unwrap().status;
    assert!(status.is_connected());
    assert_eq!(ConnectionDirection::Incoming, status.direction);

    service.inject_session_established(enr2.clone(), &dummy_socket, ConnectionDirection::Outgoing);
    let status = service.kbuckets.read().iter_ref().next().unwrap().status;
    assert!(status.is_connected());
    assert_eq!(ConnectionDirection::Incoming, status.direction);

    // (disconnected) Outgoing
    let result = service.kbuckets.write().update_node_status(
        key,
        ConnectionState::Disconnected,
        Some(ConnectionDirection::Outgoing),
    );
    assert!(matches!(result, UpdateResult::Updated));
    service.inject_session_established(enr2.clone(), &dummy_socket, ConnectionDirection::Incoming);
    let status = service.kbuckets.read().iter_ref().next().unwrap().status;
    assert!(status.is_connected());
    assert_eq!(ConnectionDirection::Outgoing, status.direction);
}

#[tokio::test]
async fn test_handling_concurrent_responses() {
    init();

    // Seed is chosen such that all nodes are in the 256th distance of the first node.
    let seed = 1652;
    let mut keypairs = generate_deterministic_keypair(5, seed);

    let mut service = {
        let enr_key = keypairs.pop().unwrap();
        let enr = Enr::builder()
            .ip4(Ipv4Addr::LOCALHOST)
            .udp4(10005)
            .build(&enr_key)
            .unwrap();
        build_service(
            Arc::new(RwLock::new(enr)),
            Arc::new(RwLock::new(enr_key)),
            false,
        )
        .await
    };

    let node_contact: NodeContact = Enr::builder()
        .ip4(Ipv4Addr::LOCALHOST)
        .udp4(10006)
        .build(&keypairs.remove(0))
        .unwrap()
        .into();
    let node_address = node_contact.node_address();

    // Add fake requests
    // Request1
    service.active_requests.insert(
        RequestId(vec![1]),
        ActiveRequest {
            contact: node_contact.clone(),
            request_body: RequestBody::FindNode {
                distances: vec![254, 255, 256],
            },
            query_id: Some(QueryId(1)),
            callback: None,
        },
    );
    // Request2
    service.active_requests.insert(
        RequestId(vec![2]),
        ActiveRequest {
            contact: node_contact,
            request_body: RequestBody::FindNode {
                distances: vec![254, 255, 256],
            },
            query_id: Some(QueryId(2)),
            callback: None,
        },
    );

    assert_eq!(3, keypairs.len());
    let mut enrs_for_response = keypairs
        .iter()
        .enumerate()
        .map(|(i, key)| {
            Enr::builder()
                .ip4(Ipv4Addr::LOCALHOST)
                .udp4(10007 + i as u16)
                .build(key)
                .unwrap()
        })
        .collect::<Vec<_>>();

    // Response to `Request1` is sent as two separate messages in total. Handle the first one of the
    // messages here.
    service.handle_rpc_response(
        node_address.clone(),
        Response {
            id: RequestId(vec![1]),
            body: ResponseBody::Nodes {
                total: 2,
                nodes: vec![enrs_for_response.pop().unwrap()],
            },
        },
    );
    // Service has still two active requests since we are waiting for the second NODE response to
    // `Request1`.
    assert_eq!(2, service.active_requests.len());
    // Service stores the first response to `Request1` into `active_nodes_responses`.
    assert!(!service.active_nodes_responses.is_empty());

    // Second, handle a response to *`Request2`* before the second response to `Request1`.
    service.handle_rpc_response(
        node_address.clone(),
        Response {
            id: RequestId(vec![2]),
            body: ResponseBody::Nodes {
                total: 1,
                nodes: vec![enrs_for_response.pop().unwrap()],
            },
        },
    );
    // `Request2` is completed so now the number of active requests should be one.
    assert_eq!(1, service.active_requests.len());
    // Service still keeps the first response in `active_nodes_responses`.
    assert!(!service.active_nodes_responses.is_empty());

    // Finally, handle the second response to `Request1`.
    service.handle_rpc_response(
        node_address,
        Response {
            id: RequestId(vec![1]),
            body: ResponseBody::Nodes {
                total: 2,
                nodes: vec![enrs_for_response.pop().unwrap()],
            },
        },
    );
    assert!(service.active_requests.is_empty());
    assert!(service.active_nodes_responses.is_empty());
}

fn generate_rand_ipv4() -> Ipv4Addr {
    let a: u8 = rand::random();
    let b: u8 = rand::random();
    let c: u8 = rand::random();
    let d: u8 = rand::random();
    Ipv4Addr::new(a, b, c, d)
}

fn generate_rand_ipv6() -> Ipv6Addr {
    let a: u16 = rand::random();
    let b: u16 = rand::random();
    let c: u16 = rand::random();
    let d: u16 = rand::random();
    let e: u16 = rand::random();
    let f: u16 = rand::random();
    let g: u16 = rand::random();
    let h: u16 = rand::random();
    Ipv6Addr::new(a, b, c, d, e, f, g, h)
}

fn random_connection_direction() -> ConnectionDirection {
    let outgoing: bool = rand::random();
    if outgoing {
        ConnectionDirection::Outgoing
    } else {
        ConnectionDirection::Incoming
    }
}

#[tokio::test]
async fn test_ipv6_update_amongst_ipv4_dominated_network() {
    init();

    let enr_key = CombinedKey::generate_secp256k1();
    let ip = std::net::Ipv4Addr::LOCALHOST;
    let local_enr = Enr::builder()
        .ip4(ip)
        .udp4(DEFAULT_UDP_PORT)
        .build(&enr_key)
        .unwrap();

    let (mut service, mut handler_recv, _handler_send) = build_non_handler_service(
        Arc::new(RwLock::new(local_enr)),
        Arc::new(RwLock::new(enr_key)),
        false,
    );

    // Load up the routing table with 100 random ENRs.

    let dummy_socket = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 80);

    for _ in 0..100 {
        let key = CombinedKey::generate_secp256k1();
        let ip = generate_rand_ipv4();
        let enr = Enr::builder()
            .ip4(ip)
            .udp4(DEFAULT_UDP_PORT)
            .build(&key)
            .unwrap();

        let direction = random_connection_direction();
        service.inject_session_established(enr.clone(), &dummy_socket, direction);
    }

    // Attempt to add 10 IPv6 nodes and expect that we attempt to send 10 PING's to IPv6 nodes.
    for _ in 0..10 {
        let key = CombinedKey::generate_secp256k1();
        let ip = generate_rand_ipv6();
        let enr = Enr::builder()
            .ip6(ip)
            .udp6(DEFAULT_UDP_PORT)
            .build(&key)
            .unwrap();

        let direction = ConnectionDirection::Outgoing;
        service.inject_session_established(enr.clone(), &dummy_socket, direction);
    }

    // Collect all the messages to the handler and count the PING requests for ENR v6 addresses.
    let mut v6_pings = 0;
    while let Ok(event) = handler_recv.try_recv() {
        if let HandlerIn::Request(contact, request) = event
            && contact.node_address().socket_addr.is_ipv6()
            && matches!(request.body, RequestBody::Ping { .. })
        {
            v6_pings += 1
        }
    }

    // Should be 10 ipv6 pings
    assert_eq!(v6_pings, 10)
}

// Tests that METRICS.ipv4_contactable is set to true once enough incoming connections are received
// during an active auto-NAT listen window.
#[tokio::test]
async fn test_connectivity_metrics_updated_on_incoming_connections() {
    init();

    // Ensure a clean state for the global METRICS.
    METRICS.ipv4_contactable.store(false, Ordering::Relaxed);

    let enr_key = CombinedKey::generate_secp256k1();
    let ip = Ipv4Addr::LOCALHOST;
    let enr = Enr::builder()
        .ip4(ip)
        .udp4(DEFAULT_UDP_PORT)
        .build(&enr_key)
        .unwrap();

    let mut service = build_service(
        Arc::new(RwLock::new(enr)),
        Arc::new(RwLock::new(enr_key)),
        false,
    )
    .await;

    let ipv4_socket: SocketAddr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 9000);

    // Simulate an ENR socket update to start the auto-NAT timer. Without this,
    // received_incoming_connection() returns early because the wait timer is None.
    service.connectivity_state.enr_socket_update(&ipv4_socket);

    // Inject incoming connections. Two are required to reach
    // NUMBER_OF_INCOMING_CONNECTIONS_REQUIRED_TO_BE_VALID.
    let peer_key = CombinedKey::generate_secp256k1();
    let peer_enr = Enr::builder()
        .ip4(ip)
        .udp4(DEFAULT_UDP_PORT)
        .build(&peer_key)
        .unwrap();

    service.inject_session_established(
        peer_enr.clone(),
        &ipv4_socket,
        ConnectionDirection::Incoming,
    );
    service.inject_session_established(
        peer_enr.clone(),
        &ipv4_socket,
        ConnectionDirection::Incoming,
    );

    // ipv4_contactable should now be true.
    assert!(
        METRICS.ipv4_contactable.load(Ordering::Relaxed),
        "ipv4_contactable should be true after receiving enough incoming connections"
    );

    // Clean up to avoid affecting other tests.
    METRICS.ipv4_contactable.store(false, Ordering::Relaxed);
}

// Tests that METRICS.ipv6_contactable is set to true once enough incoming connections are received
// during an active auto-NAT listen window.
#[tokio::test]
async fn test_connectivity_metrics_updated_on_incoming_connections_ipv6() {
    init();

    // Ensure a clean state for the global METRICS.
    METRICS.ipv6_contactable.store(false, Ordering::Relaxed);

    let enr_key = CombinedKey::generate_secp256k1();
    let ip6 = Ipv6Addr::LOCALHOST;
    let enr = Enr::builder()
        .ip6(ip6)
        .udp6(DEFAULT_UDP_PORT)
        .build(&enr_key)
        .unwrap();

    let mut service = build_service_ipv6(
        Arc::new(RwLock::new(enr)),
        Arc::new(RwLock::new(enr_key)),
        false,
    )
    .await;

    let ipv6_socket: SocketAddr = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 9000);

    // Simulate an ENR socket update to start the auto-NAT timer for IPv6.
    service.connectivity_state.enr_socket_update(&ipv6_socket);

    // Inject incoming connections. Two are required to reach
    // NUMBER_OF_INCOMING_CONNECTIONS_REQUIRED_TO_BE_VALID.
    let peer_key = CombinedKey::generate_secp256k1();
    let peer_enr = Enr::builder()
        .ip6(ip6)
        .udp6(DEFAULT_UDP_PORT)
        .build(&peer_key)
        .unwrap();

    service.inject_session_established(
        peer_enr.clone(),
        &ipv6_socket,
        ConnectionDirection::Incoming,
    );
    service.inject_session_established(
        peer_enr.clone(),
        &ipv6_socket,
        ConnectionDirection::Incoming,
    );

    // ipv6_contactable should now be true.
    assert!(
        METRICS.ipv6_contactable.load(Ordering::Relaxed),
        "ipv6_contactable should be true after receiving enough incoming connections"
    );

    // Clean up to avoid affecting other tests.
    METRICS.ipv6_contactable.store(false, Ordering::Relaxed);
}

#[cfg(feature = "topic-discovery")]
mod topic {
    use super::*;
    use crate::{
        rpc::{Request, RequestBody},
        topic::{TOPIC_DISCOVERY_ENR_KEY, TOPIC_DISCOVERY_VERSION, supports_topic_discovery},
    };
    use enr::NodeId;

    fn capable_enr(port: u16) -> (Enr, CombinedKey) {
        let key = CombinedKey::generate_secp256k1();
        let mut enr = Enr::builder()
            .ip4(Ipv4Addr::LOCALHOST)
            .udp4(port)
            .build(&key)
            .unwrap();
        enr.insert(TOPIC_DISCOVERY_ENR_KEY, &TOPIC_DISCOVERY_VERSION, &key)
            .unwrap();
        (enr, key)
    }

    async fn service_with_table(port: u16, table: &[Enr]) -> Service {
        let (enr, enr_key) = capable_enr(port);
        let service = build_service(
            Arc::new(RwLock::new(enr)),
            Arc::new(RwLock::new(enr_key)),
            false,
        )
        .await;
        for enr in table {
            let key = kbucket::Key::from(enr.node_id());
            let _ = service.kbuckets.write().insert_or_update(
                &key,
                enr.clone(),
                NodeStatus {
                    state: ConnectionState::Connected,
                    direction: ConnectionDirection::Incoming,
                },
            );
        }
        service
    }

    #[tokio::test]
    async fn aux_nodes_one_per_distance_capable_only() {
        init();
        let topic = [7u8; 32];
        let target = kbucket::Key::new_raw(NodeId::new(&topic), topic);

        // Half of the table advertises the capability.
        let mut table = Vec::new();
        for i in 0..60u16 {
            let key = CombinedKey::generate_secp256k1();
            let mut enr = Enr::builder()
                .ip4(Ipv4Addr::LOCALHOST)
                .udp4(20000 + i)
                .build(&key)
                .unwrap();
            if i % 2 == 0 {
                enr.insert(TOPIC_DISCOVERY_ENR_KEY, &TOPIC_DISCOVERY_VERSION, &key)
                    .unwrap();
            }
            table.push(enr);
        }
        let service = service_with_table(10030, &table).await;
        let held: Vec<Enr> = service
            .kbuckets
            .read()
            .iter_ref()
            .map(|e| e.node.value.clone())
            .collect();
        assert!(held.len() > 16, "table too small: {}", held.len());

        // Every distance, farthest first: the reply is capped at the first eight and
        // carries one capable node per requested distance, in request order.
        let requested: Vec<u64> = (1..=256).rev().collect();
        let aux = service.collect_topic_aux_nodes(&topic, &requested, Ipv4Addr::LOCALHOST.into());
        assert!(!aux.is_empty());
        assert!(aux.len() <= service.config.topic.aux_nodes_limit);
        let mut distances: Vec<u64> = aux
            .iter()
            .map(|enr| {
                target
                    .log2_distance(&kbucket::Key::from(enr.node_id()))
                    .unwrap()
            })
            .collect();
        for enr in &aux {
            assert!(supports_topic_discovery(enr));
            assert!(held.iter().any(|h| h.node_id() == enr.node_id()));
        }
        assert!(distances.iter().all(|d| *d > 248));
        assert!(
            distances.windows(2).all(|w| w[0] > w[1]),
            "not in request order: {distances:?}"
        );
        distances.dedup();
        assert_eq!(distances.len(), aux.len());
        let capable_at: std::collections::HashSet<u64> = held
            .iter()
            .filter(|e| supports_topic_discovery(e))
            .filter_map(|e| target.log2_distance(&kbucket::Key::from(e.node_id())))
            .filter(|d| *d > 248)
            .collect();
        assert_eq!(distances.len(), capable_at.len());

        // Unusable distances and an empty request yield nothing.
        assert!(
            service
                .collect_topic_aux_nodes(&topic, &[0, 257], Ipv4Addr::LOCALHOST.into())
                .is_empty()
        );
        assert!(
            service
                .collect_topic_aux_nodes(&topic, &[], Ipv4Addr::LOCALHOST.into())
                .is_empty()
        );
        // A remote WAN address may not be told about loopback records.
        assert!(
            service
                .collect_topic_aux_nodes(&topic, &requested, Ipv4Addr::new(8, 8, 8, 8).into())
                .is_empty()
        );
    }

    #[tokio::test]
    async fn regtopic_admits_and_topicquery_serves() {
        init();
        let topic = [9u8; 32];
        let mut service = service_with_table(10031, &[]).await;

        let (adv, _) = capable_enr(10032);
        let contact: NodeContact = adv.clone().into();
        let node_address = contact.node_address();
        let regtopic = |enr: Enr| Request {
            id: RequestId(vec![1]),
            body: RequestBody::RegTopic {
                topic,
                ticket: vec![],
                enr,
                buckets: vec![256, 255],
            },
        };

        // A record that does not belong to the sender is dropped.
        let (other, _) = capable_enr(10033);
        service.handle_rpc_request(node_address.clone(), regtopic(other.clone()));
        assert!(!service.topic_table.is_registered(&other.node_id(), &topic));
        assert_eq!(service.topic_table.occupancy().0, 0);

        // A record whose address differs from the packet source is dropped.
        let key = CombinedKey::generate_secp256k1();
        let elsewhere = Enr::builder()
            .ip4(Ipv4Addr::new(1, 2, 3, 4))
            .udp4(10034)
            .build(&key)
            .unwrap();
        let bad_contact = NodeContact::new(
            elsewhere.public_key(),
            (Ipv4Addr::new(1, 2, 3, 5), 10034).into(),
            Some(elsewhere.clone()),
        );
        service.handle_rpc_request(bad_contact.node_address(), regtopic(elsewhere.clone()));
        assert!(
            !service
                .topic_table
                .is_registered(&elsewhere.node_id(), &topic)
        );

        // An empty cache admits at once.
        service.handle_rpc_request(node_address.clone(), regtopic(adv.clone()));
        assert!(service.topic_table.is_registered(&adv.node_id(), &topic));
        assert_eq!(service.topic_table.occupancy().0, 1);

        // A garbage ticket gets no reply and changes nothing.
        service.handle_rpc_request(
            node_address.clone(),
            Request {
                id: RequestId(vec![2]),
                body: RequestBody::RegTopic {
                    topic: [1u8; 32],
                    ticket: vec![0xaa; 90],
                    enr: adv.clone(),
                    buckets: vec![],
                },
            },
        );
        assert_eq!(service.topic_table.occupancy().0, 1);

        service.handle_rpc_request(
            node_address.clone(),
            Request {
                id: RequestId(vec![3]),
                body: RequestBody::TopicQuery {
                    topic,
                    buckets: vec![256],
                },
            },
        );
        let found = service.topic_table.random_nodes(&topic, 16, |_| true);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].node_id(), adv.node_id());
    }
}
