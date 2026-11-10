//! Topic discovery in the service: the registrar-side handling of REGTOPIC and
//! TOPICQUERY, the requests of the advertiser and the searcher with the collection of
//! their responses, and the registration and search loops.

use super::{ActiveRequest, CallbackResponse, ConnectionStatus, Service};
use crate::{
    Enr, Event, RequestError,
    handler::HandlerIn,
    kbucket::{self, TableChange},
    metrics::METRICS,
    node_info::{NodeAddress, NodeContact},
    rpc::{RequestBody, RequestId, Response, ResponseBody},
    topic::{
        RegConfirmation, Registration, Search, SearchFilter, Ticket, TopicId, TopicQueryResult,
        netutil::{
            check_regtopic_record_ip, check_relay_addr, preferred_ip, preferred_udp_port, unmap,
        },
        params::{
            AUX_ENR_LIMIT, ENRS_PER_PACKET, F_RETURN, QUERY_RESP_LIMIT, REG_RESP_LIMIT,
            SEARCH_PASS_BACKOFF, SEARCH_PASS_GAP,
        },
        supports_topic_discovery,
    },
};
use enr::NodeId;
use rand::{RngExt, seq::SliceRandom};
use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;
use tracing::{debug, trace, warn};

/// The least time between two seedings of a registration or search table from the
/// routing table, so the loops do not run hot on an empty table.
const LOOP_MIN_TIME: Duration = Duration::from_secs(2);

/// Records with a port up to this one are not accepted from a response.
const LOW_PORT: u16 = 1024;

/// The registrations and searches of the local node, and the responses in progress.
#[derive(Default)]
pub(super) struct TopicSystem {
    registrations: HashMap<TopicId, TopicReg>,
    searches: HashMap<u64, TopicSearch>,
    /// Identifies registrations and searches in the callbacks of their requests.
    next_id: u64,
    /// The messages received so far for the topic requests in flight.
    responses: HashMap<RequestId, TopicResponse>,
}

/// The messages that answer one REGTOPIC or TOPICQUERY.
#[derive(Default)]
struct TopicResponse {
    received: u64,
    /// The number of messages the first message announced.
    announced: Option<u64>,
    /// Ticket and wait time in ms of the REGCONFIRMATION.
    confirmation: Option<(Vec<u8>, u64)>,
    aux: Vec<Enr>,
    advertisers: Vec<Enr>,
    aux_seen: HashSet<NodeId>,
    advertisers_seen: HashSet<NodeId>,
}

/// A REGTOPIC to send: the attempt as it was when the registration chose it.
struct RegJob {
    node: Enr,
    ticket: Vec<u8>,
    buckets: Vec<u64>,
}

/// The registration loop of one topic.
struct TopicReg {
    id: u64,
    table: Registration,
    /// When the table was last seeded from the routing table.
    seeded_at: Option<Instant>,
    /// The table is seeded and holds nodes.
    running: bool,
    /// Chosen for the next request. No other attempt is chosen while it waits.
    next: Option<RegJob>,
    /// The registrar of the request in flight. One request is sent at a time.
    in_flight: Option<NodeId>,
}

impl TopicReg {
    fn wakeup(&self) -> Option<Instant> {
        if !self.running {
            return self.seeded_at.map(|t| t + LOOP_MIN_TIME);
        }
        match self.next {
            Some(_) => None,
            None => self.table.next_update_time(),
        }
    }
}

/// One search: passes over the topic until enough advertisers are found.
struct TopicSearch {
    topic: TopicId,
    count: usize,
    deadline: Instant,
    callback: oneshot::Sender<Vec<Enr>>,
    found: Vec<Enr>,
    /// The advertisers returned by this search.
    result_filter: SearchFilter,
    /// The registrars asked by the adaptive passes. Kept here between passes.
    asked_filter: Option<SearchFilter>,
    /// Where the last adaptive pass settled.
    active_bucket: usize,
    pass: Option<Search>,
    seeded_at: Option<Instant>,
    /// The minimum length of the current pass.
    gap: Duration,
    /// Passes in a row that returned nothing new.
    idle_passes: usize,
    /// Results of the current pass.
    pass_results: usize,
    /// The registrar of the request in flight. One request is sent at a time, and a
    /// pass does not end while it is out.
    in_flight: Option<Enr>,
}

impl TopicSearch {
    fn wakeup(&self) -> Option<Instant> {
        let seed = match self.pass {
            Some(_) => None,
            None => self.seeded_at.map(|t| t + self.gap),
        };
        Some(seed.map_or(self.deadline, |t| t.min(self.deadline)))
    }

    /// The minimum length of the next pass: `SEARCH_PASS_GAP`, doubling after every
    /// pass that returned nothing new, up to `SEARCH_PASS_BACKOFF`.
    fn pass_gap(&mut self, nresults: usize) -> Duration {
        if nresults > 0 {
            self.idle_passes = 0;
            return SEARCH_PASS_GAP;
        }
        self.idle_passes += 1;
        if self.idle_passes >= 30 {
            return SEARCH_PASS_BACKOFF;
        }
        SEARCH_PASS_BACKOFF.min(SEARCH_PASS_GAP * (1 << self.idle_passes))
    }

    /// Feeds the answer of a registrar into the pass.
    fn apply_response(
        pass: &mut Search,
        from: &Enr,
        response: TopicResponse,
        failed: bool,
        now: Instant,
    ) {
        if failed && response.aux.is_empty() && response.advertisers.is_empty() {
            pass.handle_error_response(&from.node_id());
            return;
        }
        // Nodes that arrived before an error count: the registrar is kept.
        let from_id = from.node_id();
        pass.add_nodes(Some(&from_id), capable(response.aux), now);
        pass.add_query_results(from, capable(response.advertisers), now);
    }
}

fn capable(nodes: Vec<Enr>) -> impl Iterator<Item = Enr> {
    nodes.into_iter().filter(supports_topic_discovery)
}

impl Service {
    /// Serves REGTOPIC: admits the advertiser or issues a ticket, and attaches auxiliary
    /// routing nodes near the topic. Invalid tickets and records get no reply.
    pub(super) fn handle_regtopic(
        &mut self,
        node_address: NodeAddress,
        id: RequestId,
        topic: TopicId,
        ticket: Vec<u8>,
        enr: Enr,
        buckets: Vec<u64>,
    ) {
        let Some(sealer) = &mut self.ticket_sealer else {
            return;
        };
        let now = Instant::now();
        let ticket = match sealer.unpack(&topic, &ticket, now) {
            Ok(ticket) => ticket,
            Err(error) => {
                debug!(%node_address, %error, "Invalid ticket in REGTOPIC");
                return;
            }
        };
        if !enr.verify() {
            debug!(%node_address, "Node record in REGTOPIC has an invalid signature");
            return;
        }
        if enr.node_id() != node_address.node_id {
            debug!(%node_address, "Node record in REGTOPIC does not match sender");
            return;
        }
        if let Err(error) = check_regtopic_record_ip(&enr, node_address.socket_addr.ip()) {
            debug!(%node_address, error, "Bad address in REGTOPIC record");
            return;
        }

        // Only a returning advertiser has accumulated waiting time.
        let waited = ticket.as_ref().map_or(Duration::ZERO, |t| {
            now.saturating_duration_since(t.first_issued)
        });

        let aux = self.collect_topic_aux_nodes(&topic, &buckets, node_address.socket_addr.ip());
        let nodes_responses = pack_node_records(aux);
        let total = 1 + nodes_responses.len() as u64;

        let (Some(table), Some(sealer)) = (&self.topic_table, &mut self.ticket_sealer) else {
            return;
        };
        let new_wait = table.write().register(&enr, topic, waited, now);
        let (ticket, wait_time) = if new_wait > Duration::ZERO {
            let first_issued = ticket.map_or(now, |t| t.first_issued);
            let mut ticket = Ticket {
                key_id: 0,
                topic,
                wait_time_issued: new_wait,
                last_used: now,
                first_issued,
            };
            (sealer.pack(&mut ticket, now), wait_time_ms(new_wait))
        } else {
            (Vec::new(), wait_time_ms(table.read().ad_lifetime()))
        };
        trace!(
            %node_address,
            topic = hex::encode(topic),
            admitted = ticket.is_empty(),
            wait_time,
            waited_ms = waited.as_millis(),
            "Sending REGCONFIRMATION",
        );

        self.send_topic_response(
            node_address.clone(),
            Response {
                id: id.clone(),
                body: ResponseBody::RegConfirmation {
                    total,
                    ticket,
                    wait_time,
                },
            },
        );
        for nodes in nodes_responses {
            self.send_topic_response(
                node_address.clone(),
                Response {
                    id: id.clone(),
                    body: ResponseBody::Nodes { total, nodes },
                },
            );
        }
    }

    /// Serves TOPICQUERY: auxiliary routing nodes in NODES messages, then registered
    /// advertisers in TOPICNODES messages. At least one message is always sent.
    pub(super) fn handle_topic_query(
        &mut self,
        node_address: NodeAddress,
        id: RequestId,
        topic: TopicId,
        buckets: Vec<u64>,
    ) {
        let Some(table) = &self.topic_table else {
            return;
        };
        let src_ip = node_address.socket_addr.ip();
        let aux_responses =
            pack_node_records(self.collect_topic_aux_nodes(&topic, &buckets, src_ip));

        let topic_nodes = table.read().random_nodes(&topic, F_RETURN, |enr| {
            preferred_ip(enr).is_some_and(|ip| check_relay_addr(src_ip, ip).is_ok())
        });
        let topic_responses = pack_node_records(topic_nodes);

        let total = (aux_responses.len() + topic_responses.len()) as u64;
        trace!(
            %node_address,
            topic = hex::encode(topic),
            total,
            "Sending TOPICQUERY responses",
        );
        if total == 0 {
            self.send_topic_response(
                node_address,
                Response {
                    id,
                    body: ResponseBody::TopicNodes {
                        total: 1,
                        nodes: Vec::new(),
                    },
                },
            );
            return;
        }
        for nodes in aux_responses {
            self.send_topic_response(
                node_address.clone(),
                Response {
                    id: id.clone(),
                    body: ResponseBody::Nodes { total, nodes },
                },
            );
        }
        for nodes in topic_responses {
            self.send_topic_response(
                node_address.clone(),
                Response {
                    id: id.clone(),
                    body: ResponseBody::TopicNodes { total, nodes },
                },
            );
        }
    }

    /// Picks one random topic-discovery-capable routing table node per requested
    /// distance from the topic hash, in the requested order, capped at the configured
    /// aux node limit. Only nodes the requester may be told about are considered.
    pub(super) fn collect_topic_aux_nodes(
        &self,
        topic: &TopicId,
        requested: &[u64],
        remote_ip: IpAddr,
    ) -> Vec<Enr> {
        let remote_ip = unmap(remote_ip);
        let check = |enr: &Enr| {
            supports_topic_discovery(enr)
                && preferred_ip(enr).is_some_and(|ip| check_relay_addr(remote_ip, ip).is_ok())
        };

        // One node per distance, so the response is bounded by the number of distances.
        let limit = AUX_ENR_LIMIT.min(requested.len());
        let requested = &requested[..limit];
        let wanted: HashSet<u64> = requested
            .iter()
            .copied()
            .filter(|d| (1..=256).contains(d))
            .collect();
        if wanted.is_empty() {
            return Vec::new();
        }

        // A single pass over the table, reservoir-sampling one node per distance so
        // repeated requests learn different nodes.
        let target = kbucket::Key::new_raw(NodeId::new(topic), *topic);
        let mut picked: HashMap<u64, Enr> = HashMap::new();
        let mut seen: HashMap<u64, u32> = HashMap::new();
        let mut rng = rand::rng();
        for entry in self.kbuckets.read().iter_ref() {
            let Some(distance) = target.log2_distance(entry.node.key) else {
                continue;
            };
            if !wanted.contains(&distance) || !check(entry.node.value) {
                continue;
            }
            let count = seen.entry(distance).or_insert(0);
            *count += 1;
            if rng.random_range(0..*count) == 0 {
                picked.insert(distance, entry.node.value.clone());
            }
        }

        requested.iter().filter_map(|d| picked.remove(d)).collect()
    }

    fn send_topic_response(&self, node_address: NodeAddress, response: Response) {
        if let Err(e) = self
            .handler_send
            .send(HandlerIn::Response(node_address, Box::new(response)))
        {
            warn!(error = %e, "Failed to send topic response");
        }
    }

    // Requests //

    /// Sends a REGTOPIC for the user.
    pub(super) fn request_regtopic(
        &mut self,
        contact: NodeContact,
        topic: TopicId,
        ticket: Vec<u8>,
        buckets: Vec<u64>,
        callback: oneshot::Sender<Result<RegConfirmation, RequestError>>,
    ) {
        let request_body = RequestBody::RegTopic {
            topic,
            ticket,
            enr: self.local_enr.read().clone(),
            buckets,
        };
        self.send_rpc_request(ActiveRequest {
            contact,
            request_body,
            query_id: None,
            callback: Some(CallbackResponse::RegConfirmation(callback)),
        });
    }

    /// Sends a TOPICQUERY for the user.
    pub(super) fn request_topic_query(
        &mut self,
        contact: NodeContact,
        topic: TopicId,
        buckets: Vec<u64>,
        callback: oneshot::Sender<Result<TopicQueryResult, RequestError>>,
    ) {
        self.send_rpc_request(ActiveRequest {
            contact,
            request_body: RequestBody::TopicQuery { topic, buckets },
            query_id: None,
            callback: Some(CallbackResponse::TopicNodes(callback)),
        });
    }

    /// Sends a request of a registration or a search. Returns false when the record
    /// has no address the local node can send to.
    fn send_topic_request(
        &mut self,
        node: Enr,
        request_body: RequestBody,
        callback: CallbackResponse,
    ) -> bool {
        let Ok(contact) = NodeContact::try_from_enr(node, self.ip_mode) else {
            return false;
        };
        self.send_rpc_request(ActiveRequest {
            contact,
            request_body,
            query_id: None,
            callback: Some(callback),
        });
        true
    }

    /// Collects one message of the response to a topic request. The request is over
    /// when the announced number of messages, capped, has arrived and, for REGTOPIC, the
    /// REGCONFIRMATION is among them.
    pub(super) fn handle_topic_response(
        &mut self,
        node_address: NodeAddress,
        id: RequestId,
        active_request: ActiveRequest,
        body: ResponseBody,
    ) {
        let is_regtopic = matches!(active_request.request_body, RequestBody::RegTopic { .. });
        // The cap on the messages and on the records of each kind.
        let limit = if is_regtopic {
            REG_RESP_LIMIT
        } else {
            QUERY_RESP_LIMIT
        };
        let sender = node_address.socket_addr.ip();
        let mut response = self.topics.responses.remove(&id).unwrap_or_default();

        let total = body.total().unwrap_or(1);
        let announced = *response.announced.get_or_insert(total);
        response.received += 1;
        // The handler forwards no further message after the announced number, or after
        // a message that announces a single one.
        let last_message = total <= 1 || response.received >= announced;
        match body {
            ResponseBody::RegConfirmation {
                ticket, wait_time, ..
            } => {
                METRICS
                    .topic
                    .regconfirmation_recv
                    .fetch_add(1, Ordering::Relaxed);
                response.confirmation = Some((ticket, wait_time));
            }
            ResponseBody::Nodes { nodes, .. } => {
                for enr in nodes {
                    if response.aux.len() >= limit {
                        break;
                    }
                    if verify_response_node(sender, &enr, &mut response.aux_seen) {
                        response.aux.push(enr);
                    }
                }
            }
            ResponseBody::TopicNodes { nodes, .. } => {
                METRICS
                    .topic
                    .topicnodes_recv
                    .fetch_add(1, Ordering::Relaxed);
                for enr in nodes {
                    if response.advertisers.len() >= limit {
                        break;
                    }
                    if verify_response_node(sender, &enr, &mut response.advertisers_seen) {
                        response.advertisers.push(enr);
                    }
                }
            }
            _ => {}
        }

        let confirmed = !is_regtopic || response.confirmation.is_some();
        if confirmed && response.received >= announced.min(limit as u64) {
            self.topic_request_done(active_request, response, None);
        } else if last_message {
            let error = RequestError::InvalidRemotePacket;
            self.topic_request_done(active_request, response, Some(error));
        } else {
            self.topics.responses.insert(id.clone(), response);
            self.active_requests.insert(id, active_request);
        }
    }

    /// A topic request failed. The messages that arrived before go to the requester
    /// with the error.
    pub(super) fn topic_request_failed(
        &mut self,
        id: RequestId,
        active_request: ActiveRequest,
        error: RequestError,
    ) {
        let response = self.topics.responses.remove(&id).unwrap_or_default();
        self.topic_request_done(active_request, response, Some(error));
    }

    fn topic_request_done(
        &mut self,
        active_request: ActiveRequest,
        response: TopicResponse,
        error: Option<RequestError>,
    ) {
        let from = active_request.contact.node_id();
        match active_request.callback {
            Some(CallbackResponse::RegConfirmation(callback)) => {
                let result = match (error, response.confirmation) {
                    (Some(error), _) => Err(error),
                    (None, Some((ticket, wait_time))) => Ok(RegConfirmation {
                        admitted: ticket.is_empty(),
                        ticket,
                        wait_time: Duration::from_millis(wait_time),
                        aux: response.aux,
                    }),
                    (None, None) => Err(RequestError::InvalidRemotePacket),
                };
                if callback.send(result).is_err() {
                    debug!("Couldn't send the REGTOPIC response to the user");
                }
            }
            Some(CallbackResponse::TopicNodes(callback)) => {
                let result = match error {
                    Some(error) => Err(error),
                    None => Ok(TopicQueryResult {
                        advertisers: response.advertisers,
                        aux: response.aux,
                    }),
                };
                if callback.send(result).is_err() {
                    debug!("Couldn't send the TOPICQUERY response to the user");
                }
            }
            Some(CallbackResponse::TopicRegistration(topic, id)) => {
                if error.is_some() {
                    self.connection_updated(from, ConnectionStatus::Disconnected);
                }
                self.registration_response(topic, id, from, response, error);
            }
            Some(CallbackResponse::TopicSearch(id)) => {
                // A registrar that sent nodes before the error counts as responsive.
                let silent = response.aux.is_empty() && response.advertisers.is_empty();
                if error.is_some() && silent {
                    self.connection_updated(from, ConnectionStatus::Disconnected);
                }
                self.search_response(id, from, response, error);
            }
            _ => {}
        }
    }

    // The loops //

    /// The nodes of the routing table that can act as registrars, in random order.
    fn topic_capable_nodes(&self) -> Vec<Enr> {
        let mut nodes: Vec<Enr> = self
            .kbuckets
            .read()
            .iter_ref()
            .filter(|entry| supports_topic_discovery(entry.node.value))
            .map(|entry| entry.node.value.clone())
            .collect();
        nodes.shuffle(&mut rand::rng());
        nodes
    }

    /// Follows the routing table: a node that leaves it loses its ads and its parked
    /// registration attempts, a node that enters it becomes a registrar candidate.
    pub(super) fn topic_table_changes(&mut self) {
        let changes = self.kbuckets.write().take_changes();
        if changes.is_empty() {
            return;
        }
        let now = Instant::now();
        for change in changes {
            match change {
                TableChange::Removed(key) => {
                    let id = key.preimage();
                    if let Some(table) = &self.topic_table {
                        table.write().remove_node(id);
                    }
                    for reg in self.topics.registrations.values_mut() {
                        reg.table.remove_node(id, now);
                        // The attempt chosen for the next request is gone with it.
                        if reg
                            .next
                            .as_ref()
                            .is_some_and(|job| job.node.node_id() == *id)
                        {
                            reg.next = None;
                        }
                    }
                }
                TableChange::Inserted(key) => {
                    if !self.topics.registrations.values().any(|reg| reg.running) {
                        continue;
                    }
                    let node = match self.kbuckets.write().entry(&key) {
                        kbucket::Entry::Present(entry, _) => entry.value().clone(),
                        _ => continue,
                    };
                    if !supports_topic_discovery(&node) {
                        continue;
                    }
                    for reg in self.topics.registrations.values_mut() {
                        if reg.running {
                            reg.table.add_nodes(None, [node.clone()], now);
                        }
                    }
                }
            }
        }
    }

    /// When the topic system has to run next.
    pub(super) fn topic_next_wakeup(&self) -> Option<Instant> {
        let expiry = self.topic_table.as_ref()?.read().next_expiry();
        let registrations = self.topics.registrations.values().map(TopicReg::wakeup);
        let searches = self.topics.searches.values().map(TopicSearch::wakeup);
        registrations
            .chain(searches)
            .chain([expiry])
            .flatten()
            .min()
    }

    /// Runs what is due: ad expiry, registration attempts, search passes.
    pub(super) fn topic_tick(&mut self) {
        let now = Instant::now();
        if let Some(table) = &self.topic_table {
            table.write().expire(now);
        }
        let topics: Vec<TopicId> = self.topics.registrations.keys().copied().collect();
        for topic in topics {
            self.registration_step(topic, now);
        }
        let searches: Vec<u64> = self.topics.searches.keys().copied().collect();
        for id in searches {
            self.search_step(id, now);
        }
    }

    /// Starts to advertise the local node for a topic. No-op when it does already.
    pub(super) fn register_topic(&mut self, topic: TopicId) {
        if self.topics.registrations.contains_key(&topic) {
            return;
        }
        let id = self.topics.next_id;
        self.topics.next_id += 1;
        let local_id = self.local_enr.read().node_id();
        self.topics.registrations.insert(
            topic,
            TopicReg {
                id,
                table: Registration::new(topic, local_id, self.config.topic.clone()),
                seeded_at: None,
                running: false,
                next: None,
                in_flight: None,
            },
        );
        self.registration_step(topic, Instant::now());
    }

    /// Stops the registration loop of a topic. The ads placed so far run out on the
    /// registrars.
    pub(super) fn stop_topic_registration(&mut self, topic: TopicId) {
        self.topics.registrations.remove(&topic);
    }

    /// Runs the registration of a topic as far as it goes without waiting.
    fn registration_step(&mut self, topic: TopicId, now: Instant) {
        let Some(mut reg) = self.topics.registrations.remove(&topic) else {
            return;
        };
        loop {
            if !reg.running {
                if reg.seeded_at.is_some_and(|t| now < t + LOOP_MIN_TIME) {
                    break;
                }
                reg.seeded_at = Some(now);
                let nodes = self.topic_capable_nodes();
                if nodes.is_empty() {
                    break;
                }
                reg.table.add_nodes(None, nodes, now);
                reg.running = true;
            }
            if reg.table.node_count() == 0 {
                reg.running = false;
                continue;
            }

            if reg.next.is_none() && reg.table.next_update_time().is_some_and(|t| t <= now) {
                let Some(id) = reg.table.update(now) else {
                    continue;
                };
                reg.next = reg.table.attempt(&id).map(|attempt| RegJob {
                    node: attempt.node.clone(),
                    ticket: attempt.ticket.clone(),
                    buckets: reg.table.buckets_with_free_space(),
                });
            }

            if reg.in_flight.is_none()
                && let Some(job) = reg.next.take()
            {
                let id = job.node.node_id();
                if !reg.table.start_request(&id) {
                    continue;
                }
                let request_body = RequestBody::RegTopic {
                    topic,
                    ticket: job.ticket,
                    enr: self.local_enr.read().clone(),
                    buckets: job.buckets,
                };
                let callback = CallbackResponse::TopicRegistration(topic, reg.id);
                if self.send_topic_request(job.node, request_body, callback) {
                    reg.in_flight = Some(id);
                } else {
                    reg.table.handle_error_response(&id, now);
                }
                continue;
            }
            break;
        }
        self.topics.registrations.insert(topic, reg);
    }

    /// The outcome of the REGTOPIC in flight of a registration.
    fn registration_response(
        &mut self,
        topic: TopicId,
        id: u64,
        from: NodeId,
        response: TopicResponse,
        error: Option<RequestError>,
    ) {
        let now = Instant::now();
        let Some(reg) = self.topics.registrations.get_mut(&topic) else {
            return;
        };
        if reg.id != id || reg.in_flight != Some(from) {
            return;
        }
        reg.in_flight = None;

        let aux: Vec<Enr> = capable(response.aux).collect();
        if !aux.is_empty() {
            reg.table.add_nodes(Some(&from), aux, now);
        }
        let mut registered = false;
        match (error, response.confirmation) {
            (None, Some((ticket, wait_time))) => {
                let wait_time = Duration::from_millis(wait_time);
                if ticket.is_empty() {
                    debug!(%from, topic = %hex::encode(topic), lifetime_ms = wait_time.as_millis(), "Topic registration admitted");
                    reg.table.handle_registered(&from, wait_time, now);
                    registered = true;
                } else {
                    debug!(%from, topic = %hex::encode(topic), wait_ms = wait_time.as_millis(), "Topic registration ticket");
                    reg.table
                        .handle_ticket_response(&from, ticket, wait_time, now);
                }
            }
            (error, _) => {
                debug!(%from, ?error, "Topic registration failed");
                reg.table.handle_error_response(&from, now);
            }
        }
        if registered {
            self.send_event(Event::TopicRegistered {
                topic,
                registrar: from,
            });
        }
        self.registration_step(topic, now);
    }

    /// Starts a search for `count` advertisers of a topic. It ends after the query
    /// timeout at the latest, with the advertisers found until then.
    pub(super) fn start_topic_search(
        &mut self,
        topic: TopicId,
        count: usize,
        callback: oneshot::Sender<Vec<Enr>>,
    ) {
        let now = Instant::now();
        let id = self.topics.next_id;
        self.topics.next_id += 1;
        self.topics.searches.insert(
            id,
            TopicSearch {
                topic,
                count,
                deadline: now + self.config.query_timeout,
                callback,
                found: Vec::new(),
                result_filter: SearchFilter::new(&self.config.topic),
                asked_filter: None,
                active_bucket: 0,
                pass: None,
                seeded_at: None,
                gap: LOOP_MIN_TIME,
                idle_passes: 0,
                pass_results: 0,
                in_flight: None,
            },
        );
        self.search_step(id, now);
    }

    /// Runs a search as far as it goes without waiting.
    fn search_step(&mut self, id: u64, now: Instant) {
        let Some(mut search) = self.topics.searches.remove(&id) else {
            return;
        };
        let mut new_results = Vec::new();
        let finished = loop {
            if search.callback.is_closed()
                || search.found.len() >= search.count
                || now >= search.deadline
            {
                break true;
            }

            let pass = match &mut search.pass {
                Some(pass) => pass,
                None => {
                    if search.seeded_at.is_some_and(|t| now < t + search.gap) {
                        break false;
                    }
                    search.seeded_at = Some(now);
                    search.pass_results = 0;
                    let nodes = self.topic_capable_nodes();
                    if nodes.is_empty() {
                        break false;
                    }
                    let local_id = self.local_enr.read().node_id();
                    let mut pass = Search::new(search.topic, local_id, self.config.topic.clone());
                    pass.set_active_bucket(search.active_bucket);
                    if let Some(filter) = search.asked_filter.take() {
                        pass.set_asked_filter(filter);
                    }
                    pass.add_nodes(None, nodes, now);
                    search.pass.insert(pass)
                }
            };

            // The results are taken at once, so none is left when the pass is checked.
            while search.found.len() < search.count
                && let Some(node) = pass.pop_result()
            {
                if search.result_filter.seen(&node, now) {
                    continue;
                }
                search.result_filter.add(&node, now);
                search.found.push(node.clone());
                search.pass_results += 1;
                new_results.push(node);
            }
            if search.found.len() >= search.count {
                continue;
            }

            if pass.is_done() {
                debug!(
                    topic = %hex::encode(search.topic),
                    found = search.found.len(),
                    "Topic search rollover",
                );
                search.active_bucket = pass.active_bucket();
                search.asked_filter = search.pass.take().map(Search::into_asked_filter);
                search.gap = search.pass_gap(search.pass_results);
                continue;
            }

            if search.in_flight.is_none()
                && let Some(target) = pass.query_target()
            {
                let request_body = RequestBody::TopicQuery {
                    topic: search.topic,
                    buckets: pass.buckets_with_free_space(),
                };
                let callback = CallbackResponse::TopicSearch(id);
                if self.send_topic_request(target.clone(), request_body, callback) {
                    search.in_flight = Some(target);
                } else {
                    pass.handle_error_response(&target.node_id());
                    continue;
                }
            }
            break false;
        };

        if !new_results.is_empty() {
            self.send_event(Event::TopicNodesFound {
                topic: search.topic,
                nodes: new_results,
            });
        }
        if finished {
            if search.callback.send(search.found).is_err() {
                debug!("The topic search was dropped before it ended");
            }
        } else {
            self.topics.searches.insert(id, search);
        }
    }

    /// The outcome of the TOPICQUERY in flight of a search.
    fn search_response(
        &mut self,
        id: u64,
        from: NodeId,
        response: TopicResponse,
        error: Option<RequestError>,
    ) {
        let now = Instant::now();
        let Some(search) = self.topics.searches.get_mut(&id) else {
            return;
        };
        let Some(from) = search.in_flight.take_if(|node| node.node_id() == from) else {
            return;
        };
        if let Some(error) = &error {
            debug!(from = %from.node_id(), ?error, "TOPICQUERY failed");
        }
        if let Some(pass) = &mut search.pass {
            TopicSearch::apply_response(pass, &from, response, error.is_some(), now);
        }
        self.search_step(id, now);
    }
}

/// Checks a record of a response to a topic request: it needs an address that the
/// sender may relay, a port above the low ports, and an id not seen in this response.
fn verify_response_node(sender: IpAddr, enr: &Enr, seen: &mut HashSet<NodeId>) -> bool {
    let relay_ok = preferred_ip(enr).is_some_and(|ip| check_relay_addr(unmap(sender), ip).is_ok());
    if !relay_ok || preferred_udp_port(enr).is_none_or(|port| port <= LOW_PORT) {
        debug!(node_id = %enr.node_id(), "Invalid record in topic response");
        return false;
    }
    seen.insert(enr.node_id())
}

/// Wait time in whole milliseconds, rounded up.
fn wait_time_ms(d: Duration) -> u64 {
    (d.as_secs_f64() * 1000.0).ceil() as u64
}

fn pack_node_records(nodes: Vec<Enr>) -> Vec<Vec<Enr>> {
    nodes.chunks(ENRS_PER_PACKET).map(<[Enr]>::to_vec).collect()
}

#[cfg(test)]
mod tests {
    use super::{
        super::test::{build_non_handler_service, set_topic_config},
        *,
    };
    use crate::{
        kbucket::{ConnectionDirection, ConnectionState, NodeStatus},
        rpc::Request,
        topic::{RegAttemptState, TOPIC_DISCOVERY_ENR_KEY, TOPIC_DISCOVERY_VERSION, TopicConfig},
    };
    use enr::CombinedKey;
    use parking_lot::RwLock;
    use std::{net::Ipv4Addr, sync::Arc};
    use tokio::sync::mpsc::{self, UnboundedReceiver};

    const TOPIC: TopicId = [3u8; 32];

    fn record(ip: Ipv4Addr, port: u16, capable: bool) -> Enr {
        let key = CombinedKey::generate_secp256k1();
        let mut enr = Enr::builder().ip4(ip).udp4(port).build(&key).unwrap();
        if capable {
            enr.insert(TOPIC_DISCOVERY_ENR_KEY, &TOPIC_DISCOVERY_VERSION, &key)
                .unwrap();
        }
        enr
    }

    fn registrar(port: u16) -> Enr {
        record(Ipv4Addr::LOCALHOST, port, true)
    }

    /// A registrar in the farthest bucket from `TOPIC`.
    fn far_registrar(port: u16) -> Enr {
        loop {
            let enr = registrar(port);
            if crate::topic::log_dist(&TOPIC, &enr.node_id()) == 256 {
                return enr;
            }
        }
    }

    fn add_to_table(service: &Service, enr: &Enr) {
        let result = service.kbuckets.write().insert_or_update(
            &enr.node_id().into(),
            enr.clone(),
            NodeStatus {
                state: ConnectionState::Connected,
                direction: ConnectionDirection::Outgoing,
            },
        );
        assert!(matches!(result, kbucket::InsertResult::Inserted));
    }

    /// A service without a handler, with topic discovery enabled and the given nodes in
    /// the routing table.
    fn service(config: TopicConfig, table: &[Enr]) -> (Service, UnboundedReceiver<HandlerIn>) {
        let key = CombinedKey::generate_secp256k1();
        let enr = record(Ipv4Addr::LOCALHOST, 12100, true);
        let (mut service, handler, _) = build_non_handler_service(
            Arc::new(RwLock::new(enr)),
            Arc::new(RwLock::new(key)),
            false,
        );
        service.ip_mode = crate::IpMode::Ip4;
        let config = TopicConfig {
            enabled: true,
            ..config
        };
        set_topic_config(&mut service, config);
        for enr in table {
            add_to_table(&service, enr);
        }
        (service, handler)
    }

    /// The next topic request the service handed to the handler.
    fn next_request(handler: &mut UnboundedReceiver<HandlerIn>) -> Option<(NodeContact, Request)> {
        while let Ok(message) = handler.try_recv() {
            if let HandlerIn::Request(contact, request) = message
                && request.body.is_topic_request()
            {
                return Some((contact, *request));
            }
        }
        None
    }

    fn respond(service: &mut Service, contact: &NodeContact, id: &RequestId, body: ResponseBody) {
        service.handle_rpc_response(
            contact.node_address(),
            Response {
                id: id.clone(),
                body,
            },
        );
    }

    fn topic_nodes(nodes: Vec<Enr>) -> ResponseBody {
        ResponseBody::TopicNodes { total: 1, nodes }
    }

    fn ids(nodes: &[Enr]) -> Vec<NodeId> {
        nodes.iter().map(|enr| enr.node_id()).collect()
    }

    // REGCONFIRMATION and NODES are collected in any order until the announced number
    // of messages arrived; bad records and duplicates are dropped.
    #[tokio::test]
    async fn regtopic_response_collection() {
        let target = registrar(12101);
        let (mut service, mut handler) = service(TopicConfig::default(), &[]);
        let (tx, mut rx) = oneshot::channel();
        service.request_regtopic(target.clone().into(), TOPIC, vec![9], vec![256, 255], tx);

        let (contact, request) = next_request(&mut handler).unwrap();
        assert_eq!(contact.node_id(), target.node_id());
        assert_eq!(
            request.body,
            RequestBody::RegTopic {
                topic: TOPIC,
                ticket: vec![9],
                enr: service.local_enr.read().clone(),
                buckets: vec![256, 255],
            }
        );

        let (a, b, c) = (registrar(12102), registrar(12103), registrar(12104));
        let wan = record(Ipv4Addr::new(8, 8, 8, 8), 30303, false);
        let lan = record(Ipv4Addr::new(10, 0, 0, 1), 30303, true);
        let bad = vec![
            record(Ipv4Addr::LOCALHOST, 1024, true),
            record(Ipv4Addr::new(203, 0, 113, 1), 30303, true),
            Enr::builder()
                .build(&CombinedKey::generate_secp256k1())
                .unwrap(),
        ];

        let nodes = |nodes: Vec<Enr>| ResponseBody::Nodes { total: 3, nodes };
        respond(
            &mut service,
            &contact,
            &request.id,
            nodes(vec![a.clone(), b.clone()]),
        );
        assert!(rx.try_recv().is_err());
        respond(
            &mut service,
            &contact,
            &request.id,
            ResponseBody::RegConfirmation {
                total: 3,
                ticket: vec![1, 2],
                wait_time: 1500,
            },
        );
        assert!(rx.try_recv().is_err());
        let mut last = vec![b.clone(), wan.clone()];
        last.extend(bad);
        last.extend([lan.clone(), c.clone()]);
        respond(&mut service, &contact, &request.id, nodes(last));

        let confirmation = rx.try_recv().unwrap().unwrap();
        assert!(!confirmation.admitted);
        assert_eq!(confirmation.ticket, vec![1, 2]);
        assert_eq!(confirmation.wait_time, Duration::from_millis(1500));
        assert_eq!(ids(&confirmation.aux), ids(&[a, b, wan, lan, c]));
        assert!(service.active_requests.is_empty());
        assert!(service.topics.responses.is_empty());
    }

    // The number of messages and of records is capped.
    #[tokio::test]
    async fn topic_response_caps() {
        let target = registrar(12101);
        let (mut service, mut handler) = service(TopicConfig::default(), &[]);

        // REGTOPIC: 8 messages, 8 records.
        let (tx, mut rx) = oneshot::channel();
        service.request_regtopic(target.clone().into(), TOPIC, vec![], vec![], tx);
        let (contact, request) = next_request(&mut handler).unwrap();
        respond(
            &mut service,
            &contact,
            &request.id,
            ResponseBody::RegConfirmation {
                total: 200,
                ticket: vec![],
                wait_time: 5,
            },
        );
        for i in 0..7u16 {
            assert!(rx.try_recv().is_err());
            let nodes = vec![registrar(13000 + 2 * i), registrar(13001 + 2 * i)];
            respond(
                &mut service,
                &contact,
                &request.id,
                ResponseBody::Nodes { total: 200, nodes },
            );
        }
        let confirmation = rx.try_recv().unwrap().unwrap();
        assert!(confirmation.admitted);
        assert_eq!(confirmation.aux.len(), 8);

        // TOPICQUERY: 16 messages, 16 records of each kind.
        let (tx, mut rx) = oneshot::channel();
        service.request_topic_query(target.into(), TOPIC, vec![256], tx);
        let (contact, request) = next_request(&mut handler).unwrap();
        for i in 0..16u16 {
            assert!(rx.try_recv().is_err());
            let nodes = (0..3).map(|j| registrar(14000 + 3 * i + j)).collect();
            let body = if i % 2 == 0 {
                ResponseBody::Nodes { total: 40, nodes }
            } else {
                ResponseBody::TopicNodes { total: 40, nodes }
            };
            respond(&mut service, &contact, &request.id, body);
        }
        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.aux.len(), 16);
        assert_eq!(result.advertisers.len(), 16);
    }

    // A single empty TOPICNODES ends a TOPICQUERY; a failure goes to the user.
    #[tokio::test]
    async fn topic_query_empty_and_failed() {
        let target = registrar(12101);
        let (mut service, mut handler) = service(TopicConfig::default(), &[]);

        let (tx, mut rx) = oneshot::channel();
        service.request_topic_query(target.clone().into(), TOPIC, vec![], tx);
        let (contact, request) = next_request(&mut handler).unwrap();
        respond(&mut service, &contact, &request.id, topic_nodes(vec![]));
        let result = rx.try_recv().unwrap().unwrap();
        assert!(result.advertisers.is_empty() && result.aux.is_empty());

        let (tx, mut rx) = oneshot::channel();
        service.request_topic_query(target.into(), TOPIC, vec![], tx);
        let (contact, request) = next_request(&mut handler).unwrap();
        let nodes = vec![registrar(12102)];
        respond(
            &mut service,
            &contact,
            &request.id,
            ResponseBody::TopicNodes { total: 2, nodes },
        );
        service.rpc_failure(request.id, RequestError::Timeout);
        assert_eq!(rx.try_recv().unwrap().unwrap_err(), RequestError::Timeout);
        assert!(service.topics.responses.is_empty());
    }

    // A response that the handler has finished and that is not complete, or of the
    // wrong kind, fails the request.
    #[tokio::test]
    async fn incomplete_response_fails() {
        let target = registrar(12101);
        let (mut service, mut handler) = service(TopicConfig::default(), &[]);
        let mut request = |service: &mut Service| {
            let (tx, rx) = oneshot::channel();
            service.request_regtopic(target.clone().into(), TOPIC, vec![], vec![], tx);
            let (contact, request) = next_request(&mut handler).unwrap();
            (contact, request.id, rx)
        };
        let invalid = Err(RequestError::InvalidRemotePacket);

        // The only message is not the REGCONFIRMATION.
        let (contact, id, mut rx) = request(&mut service);
        let nodes = vec![registrar(12102)];
        respond(
            &mut service,
            &contact,
            &id,
            ResponseBody::Nodes { total: 1, nodes },
        );
        assert_eq!(rx.try_recv().unwrap().map(|_| ()), invalid);

        // All announced messages arrived, without the REGCONFIRMATION.
        let (contact, id, mut rx) = request(&mut service);
        for _ in 0..2 {
            assert!(rx.try_recv().is_err());
            let nodes = vec![registrar(12102)];
            respond(
                &mut service,
                &contact,
                &id,
                ResponseBody::Nodes { total: 2, nodes },
            );
        }
        assert_eq!(rx.try_recv().unwrap().map(|_| ()), invalid);

        // A later message announces a single one.
        let (contact, id, mut rx) = request(&mut service);
        let nodes = vec![registrar(12102)];
        respond(
            &mut service,
            &contact,
            &id,
            ResponseBody::Nodes { total: 3, nodes },
        );
        let confirmation = ResponseBody::RegConfirmation {
            total: 1,
            ticket: vec![],
            wait_time: 1,
        };
        respond(&mut service, &contact, &id, confirmation);
        assert_eq!(rx.try_recv().unwrap().map(|_| ()), invalid);

        // The wrong kind of message.
        let (contact, id, mut rx) = request(&mut service);
        respond(&mut service, &contact, &id, topic_nodes(vec![]));
        assert_eq!(rx.try_recv().unwrap().map(|_| ()), invalid);

        assert!(service.active_requests.is_empty());
        assert!(service.topics.responses.is_empty());
    }

    // The registration walk: ticket, wait, second request with the ticket, admission.
    // A node that enters the routing table becomes a registrar.
    #[tokio::test]
    async fn registration_walk() {
        let first = registrar(12101);
        let config = TopicConfig::default();
        let (mut service, mut handler) = service(config.clone(), std::slice::from_ref(&first));
        // The changes that built the table are not news.
        service.topic_table_changes();
        let (events_tx, mut events) = mpsc::channel(10);
        service.event_stream = Some(events_tx);

        service.register_topic(TOPIC);
        let (contact, request) = next_request(&mut handler).unwrap();
        assert_eq!(contact.node_id(), first.node_id());
        let RequestBody::RegTopic {
            topic,
            ticket,
            enr,
            buckets,
        } = request.body
        else {
            panic!("not a REGTOPIC");
        };
        assert_eq!(topic, TOPIC);
        assert!(ticket.is_empty());
        assert_eq!(enr, service.local_enr.read().clone());
        // The labels of the registration buckets, as the Go table lists them.
        assert_eq!(buckets, (239..=256).rev().collect::<Vec<u64>>());
        // One request at a time.
        assert!(next_request(&mut handler).is_none());
        assert!(service.topic_next_wakeup().is_none());

        let aux = registrar(12102);
        let not_capable = record(Ipv4Addr::LOCALHOST, 12103, false);
        respond(
            &mut service,
            &contact,
            &request.id,
            ResponseBody::RegConfirmation {
                total: 2,
                ticket: vec![7; 90],
                wait_time: 100,
            },
        );
        respond(
            &mut service,
            &contact,
            &request.id,
            ResponseBody::Nodes {
                total: 2,
                nodes: vec![aux.clone(), not_capable.clone()],
            },
        );

        // The auxiliary node is asked at once, the first registrar after the wait.
        let (aux_contact, aux_request) = next_request(&mut handler).unwrap();
        assert_eq!(aux_contact.node_id(), aux.node_id());
        let reg = &service.topics.registrations[&TOPIC];
        assert!(reg.table.attempt(&not_capable.node_id()).is_none());
        assert_eq!(reg.table.node_count(), 2);
        respond(
            &mut service,
            &aux_contact,
            &aux_request.id,
            ResponseBody::RegConfirmation {
                total: 1,
                ticket: vec![],
                // More than the local lifetime: cut to it.
                wait_time: 10 * config.ad_lifetime.as_millis() as u64,
            },
        );
        let Some(Event::TopicRegistered {
            topic,
            registrar: by,
        }) = events.try_recv().ok()
        else {
            panic!("no registration event");
        };
        assert_eq!((topic, by), (TOPIC, aux.node_id()));
        let reg = &service.topics.registrations[&TOPIC];
        let attempt = reg.table.attempt(&aux.node_id()).unwrap();
        assert_eq!(attempt.state, RegAttemptState::Registered);
        assert!(attempt.next_time <= Instant::now() + config.ad_lifetime);

        assert!(next_request(&mut handler).is_none());
        let wakeup = service.topic_next_wakeup().unwrap();
        assert!(wakeup <= Instant::now() + Duration::from_millis(100));
        tokio::time::sleep_until(wakeup.into()).await;
        service.topic_tick();
        let (contact, request) = next_request(&mut handler).unwrap();
        assert_eq!(contact.node_id(), first.node_id());
        assert!(
            matches!(request.body, RequestBody::RegTopic { ref ticket, .. } if ticket == &vec![7; 90])
        );

        // A node enters the routing table while the request is in flight.
        let late = registrar(12104);
        add_to_table(&service, &late);
        add_to_table(&service, &record(Ipv4Addr::LOCALHOST, 12105, false));
        service.topic_table_changes();
        assert_eq!(service.topics.registrations[&TOPIC].table.node_count(), 3);
        service.topic_tick();
        assert!(next_request(&mut handler).is_none());

        // The first registrar does not answer: its attempt ends, the next one starts.
        service.rpc_failure(request.id, RequestError::Timeout);
        let reg = &service.topics.registrations[&TOPIC];
        assert!(reg.table.attempt(&first.node_id()).is_none());
        let (contact, request) = next_request(&mut handler).unwrap();
        assert_eq!(contact.node_id(), late.node_id());

        // A stopped registration ignores the answer.
        service.stop_topic_registration(TOPIC);
        respond(
            &mut service,
            &contact,
            &request.id,
            ResponseBody::RegConfirmation {
                total: 1,
                ticket: vec![],
                wait_time: 1000,
            },
        );
        assert!(events.try_recv().is_err());
        assert!(service.topics.registrations.is_empty());
        assert!(service.topic_next_wakeup().is_none());
    }

    // Without registrars in the routing table, the registration looks again after the
    // pause.
    #[tokio::test]
    async fn registration_waits_for_registrars() {
        let not_capable = record(Ipv4Addr::LOCALHOST, 12101, false);
        let (mut service, mut handler) = service(TopicConfig::default(), &[not_capable]);
        let start = Instant::now();
        service.register_topic(TOPIC);
        assert!(next_request(&mut handler).is_none());
        let wakeup = service.topic_next_wakeup().unwrap();
        assert!(wakeup >= start + LOOP_MIN_TIME && wakeup <= Instant::now() + LOOP_MIN_TIME);

        let late = registrar(12102);
        add_to_table(&service, &late);
        service.topic_table_changes();
        service.registration_step(TOPIC, wakeup - Duration::from_millis(1));
        assert!(next_request(&mut handler).is_none());
        service.registration_step(TOPIC, wakeup);
        let (contact, _) = next_request(&mut handler).unwrap();
        assert_eq!(contact.node_id(), late.node_id());
    }

    // A node that leaves the routing table loses its ads and its parked registration
    // attempts. An attempt with a request in flight stays.
    #[tokio::test]
    async fn routing_table_removal_evicts() {
        let nodes: Vec<Enr> = (0..3).map(|i| registrar(12101 + i)).collect();
        let (mut service, mut handler) = service(TopicConfig::default(), &nodes);
        let now = Instant::now();
        for enr in &nodes {
            assert!(
                service
                    .topic_table
                    .as_ref()
                    .unwrap()
                    .write()
                    .add(enr, TOPIC, now)
            );
            assert!(
                service
                    .topic_table
                    .as_ref()
                    .unwrap()
                    .write()
                    .add(enr, [4u8; 32], now)
            );
        }
        service.register_topic(TOPIC);
        let (in_flight, _) = next_request(&mut handler).unwrap();
        let parked = nodes
            .iter()
            .find(|enr| enr.node_id() != in_flight.node_id())
            .unwrap();

        for enr in &nodes {
            assert!(service.kbuckets.write().remove(&enr.node_id().into()));
        }
        service.topic_table_changes();

        assert_eq!(
            service.topic_table.as_ref().unwrap().read().occupancy().0,
            0
        );
        assert!(
            service
                .topic_table
                .as_ref()
                .unwrap()
                .read()
                .nodes(&TOPIC, now)
                .is_empty()
        );
        let reg = &service.topics.registrations[&TOPIC];
        assert!(reg.table.attempt(&parked.node_id()).is_none());
        assert!(reg.table.attempt(&in_flight.node_id()).is_some());
        assert_eq!(reg.table.node_count(), 1);
        assert!(reg.next.is_none());
    }

    /// Answers the queries of a search until it stops to send them. Returns the nodes
    /// asked.
    fn answer_queries(
        service: &mut Service,
        handler: &mut UnboundedReceiver<HandlerIn>,
        mut answer: impl FnMut(&NodeContact) -> Vec<ResponseBody>,
    ) -> Vec<NodeId> {
        let mut asked = Vec::new();
        while let Some((contact, request)) = next_request(handler) {
            assert!(
                matches!(request.body, RequestBody::TopicQuery { topic, .. } if topic == TOPIC)
            );
            asked.push(contact.node_id());
            for body in answer(&contact) {
                respond(service, &contact, &request.id, body);
            }
        }
        asked
    }

    // A pass asks every registrar of the farthest bucket once, the next pass asks one of
    // them again, each advertiser is returned once, and the search ends when it has the
    // wanted number.
    #[tokio::test]
    async fn search_passes() {
        let registrars: Vec<Enr> = (0..6).map(|i| far_registrar(12101 + i)).collect();
        let advertisers: Vec<Enr> = (0..3).map(|i| registrar(12201 + i)).collect();
        let not_capable = record(Ipv4Addr::LOCALHOST, 12210, false);
        let (mut service, mut handler) = service(TopicConfig::default(), &registrars);
        let (events_tx, mut events) = mpsc::channel(10);
        service.event_stream = Some(events_tx);

        let (tx, mut rx) = oneshot::channel();
        let start = Instant::now();
        service.start_topic_search(TOPIC, 3, tx);
        let id = *service.topics.searches.keys().next().unwrap();

        // Every registrar returns the same two advertisers.
        let ads = vec![advertisers[0].clone(), not_capable, advertisers[1].clone()];
        let asked = answer_queries(&mut service, &mut handler, |_| {
            vec![topic_nodes(ads.clone())]
        });
        let mut sorted = asked.clone();
        sorted.sort_by_key(|id| id.raw());
        sorted.dedup();
        assert_eq!(sorted.len(), 6, "asked {asked:?}");
        assert_eq!(asked.len(), 6);
        assert!(rx.try_recv().is_err());
        let Some(Event::TopicNodesFound { topic, nodes }) = events.try_recv().ok() else {
            panic!("no search event");
        };
        assert_eq!(topic, TOPIC);
        assert_eq!(ids(&nodes), ids(&advertisers[..2]));
        assert!(events.try_recv().is_err());

        // The pass is over. The next one starts after the pause.
        let search = &service.topics.searches[&id];
        assert!(search.pass.is_none() && search.in_flight.is_none());
        assert_eq!(search.found.len(), 2);
        let wakeup = service.topic_next_wakeup().unwrap();
        assert!(wakeup >= start + LOOP_MIN_TIME && wakeup <= Instant::now() + LOOP_MIN_TIME);
        service.search_step(id, wakeup - Duration::from_millis(1));
        assert!(next_request(&mut handler).is_none());

        // The second pass asks a spare registrar, which brings one new advertiser with
        // the known ones.
        service.search_step(id, wakeup);
        let (contact, request) = next_request(&mut handler).unwrap();
        let ads = vec![advertisers[1].clone(), advertisers[2].clone()];
        respond(&mut service, &contact, &request.id, topic_nodes(ads));
        let found = rx.try_recv().unwrap();
        assert_eq!(ids(&found), ids(&advertisers));
        let Some(Event::TopicNodesFound { nodes, .. }) = events.try_recv().ok() else {
            panic!("no search event");
        };
        assert_eq!(ids(&nodes), ids(&advertisers[2..]));
        assert!(service.topics.searches.is_empty());
        // Nothing but the answer in flight is left of the search.
        assert!(next_request(&mut handler).is_none());
    }

    // Auxiliary nodes join the pass. A registrar that fails after it sent nodes is
    // kept, one that sent nothing is dropped from the pass.
    #[tokio::test]
    async fn search_aux_nodes_and_failures() {
        let registrars: Vec<Enr> = (0..2).map(|i| registrar(12101 + i)).collect();
        let aux = registrar(12110);
        let (mut service, mut handler) = service(TopicConfig::default(), &registrars);
        let (tx, mut rx) = oneshot::channel();
        service.start_topic_search(TOPIC, 1, tx);

        // The first registrar names the auxiliary node and then times out.
        let (contact, request) = next_request(&mut handler).unwrap();
        let first = contact.node_id();
        assert!(matches!(
            request.body,
            RequestBody::TopicQuery { ref buckets, .. }
                if buckets == &[256, 255]
        ));
        respond(
            &mut service,
            &contact,
            &request.id,
            ResponseBody::Nodes {
                total: 2,
                nodes: vec![aux.clone()],
            },
        );
        assert!(next_request(&mut handler).is_none());
        service.rpc_failure(request.id, RequestError::Timeout);

        // The others fail with nothing. The search goes on until the table is used up.
        let mut asked = vec![first];
        while let Some((contact, request)) = next_request(&mut handler) {
            asked.push(contact.node_id());
            service.rpc_failure(request.id, RequestError::Timeout);
        }
        asked.sort_by_key(|id| id.raw());
        let mut expected = ids(&registrars);
        expected.push(aux.node_id());
        expected.sort_by_key(|id| id.raw());
        assert_eq!(asked, expected);
        assert!(rx.try_recv().is_err());
        assert!(service.topics.responses.is_empty());
        assert!(service.active_requests.is_empty());
    }

    // The search keeps the active bucket and the asked nodes from pass to pass: a node
    // is not asked again, except one spare node per pass.
    #[tokio::test]
    async fn adaptive_search_passes() {
        let registrars: Vec<Enr> = (0..8).map(|i| registrar(12101 + i)).collect();
        let advertiser = registrar(12201);
        let (mut service, mut handler) = service(TopicConfig::default(), &registrars);
        let (tx, mut rx) = oneshot::channel();
        service.start_topic_search(TOPIC, 1, tx);
        let id = *service.topics.searches.keys().next().unwrap();
        // The empty passes back off; the search must outlive them.
        service.topics.searches.get_mut(&id).unwrap().deadline =
            Instant::now() + Duration::from_secs(3600);

        // Only the buckets around the active one are requested.
        let (contact, request) = next_request(&mut handler).unwrap();
        assert!(matches!(
            request.body,
            RequestBody::TopicQuery { ref buckets, .. }
                if !buckets.is_empty() && buckets.iter().all(|d| *d >= 255)
        ));
        respond(&mut service, &contact, &request.id, topic_nodes(vec![]));
        let mut asked = vec![contact.node_id()];

        // Empty answers move the search closer until the pass ends.
        while asked.len() < registrars.len() {
            let more = answer_queries(&mut service, &mut handler, |_| vec![topic_nodes(vec![])]);
            for id in &more {
                assert!(!asked.contains(id), "asked again in a later pass");
            }
            asked.extend(more);
            let search = &service.topics.searches[&id];
            assert!(search.pass.is_none(), "pass not over");
            assert!(search.active_bucket > 0);
            assert!(
                search
                    .asked_filter
                    .as_ref()
                    .is_some_and(|f| f.len() == asked.len())
            );
            if asked.len() < registrars.len() {
                let next_pass = service.topic_next_wakeup().unwrap();
                service.search_step(id, next_pass);
            }
        }
        let active_bucket = service.topics.searches[&id].active_bucket;

        // All nodes are asked: the next pass asks one of them again, and no second.
        let next_pass = service.topic_next_wakeup().unwrap();
        service.search_step(id, next_pass);
        let search = &service.topics.searches[&id];
        assert_eq!(
            search.pass.as_ref().map(Search::active_bucket),
            Some(active_bucket)
        );
        let (contact, request) = next_request(&mut handler).unwrap();
        assert!(asked.contains(&contact.node_id()));
        assert!(next_request(&mut handler).is_none());
        respond(
            &mut service,
            &contact,
            &request.id,
            topic_nodes(vec![advertiser.clone()]),
        );
        assert_eq!(ids(&rx.try_recv().unwrap()), vec![advertiser.node_id()]);
        assert!(service.topics.searches.is_empty());
    }

    // A search whose future was dropped ends.
    #[tokio::test]
    async fn dropped_search_ends() {
        let registrars: Vec<Enr> = (0..2).map(|i| registrar(12101 + i)).collect();
        let (mut service, mut handler) = service(TopicConfig::default(), &registrars);
        let (tx, rx) = oneshot::channel();
        service.start_topic_search(TOPIC, 1, tx);
        let (contact, request) = next_request(&mut handler).unwrap();
        drop(rx);
        respond(&mut service, &contact, &request.id, topic_nodes(vec![]));
        assert!(service.topics.searches.is_empty());
        assert!(next_request(&mut handler).is_none());
    }

    // The gap between passes: the minimum after a pass with results, doubling from it
    // after every empty pass up to the back-off cap.
    #[test]
    fn search_pass_gap() {
        let registrars: Vec<Enr> = (0..2).map(|i| registrar(12101 + i)).collect();
        let (mut service, mut handler) = service(TopicConfig::default(), &registrars);
        let (tx, _rx) = oneshot::channel();
        service.start_topic_search(TOPIC, 1, tx);
        let id = *service.topics.searches.keys().next().unwrap();
        let _ = next_request(&mut handler);
        let search = service.topics.searches.get_mut(&id).unwrap();

        for want in [4, 8, 16, 32, 60, 60] {
            assert_eq!(search.pass_gap(0), Duration::from_secs(want));
        }
        assert_eq!(search.pass_gap(3), SEARCH_PASS_GAP);
        assert_eq!(search.pass_gap(0), Duration::from_secs(4));
    }

    // An empty pass delays the next one by the back-off gap; a pass with results does
    // not.
    #[tokio::test]
    async fn search_pass_backoff() {
        let registrars: Vec<Enr> = (0..1).map(|i| registrar(12101 + i)).collect();
        let advertiser = registrar(12201);
        let (mut service, mut handler) = service(TopicConfig::default(), &registrars);
        let (tx, _rx) = oneshot::channel();
        let start = Instant::now();
        service.start_topic_search(TOPIC, 2, tx);
        let id = *service.topics.searches.keys().next().unwrap();

        // An empty pass: the next one waits 4 s.
        let (contact, request) = next_request(&mut handler).unwrap();
        respond(&mut service, &contact, &request.id, topic_nodes(vec![]));
        let wakeup = service.topic_next_wakeup().unwrap();
        assert!(wakeup >= start + 2 * LOOP_MIN_TIME);
        service.search_step(id, start + LOOP_MIN_TIME + Duration::from_millis(1));
        assert!(next_request(&mut handler).is_none());

        // Another empty pass: 8 s. Then a pass with a result: back to 2 s.
        service.search_step(id, wakeup);
        let (contact, request) = next_request(&mut handler).unwrap();
        respond(&mut service, &contact, &request.id, topic_nodes(vec![]));
        assert_eq!(service.topics.searches[&id].gap, 4 * LOOP_MIN_TIME);
        service.search_step(id, wakeup + 4 * LOOP_MIN_TIME);
        let (contact, request) = next_request(&mut handler).unwrap();
        respond(
            &mut service,
            &contact,
            &request.id,
            topic_nodes(vec![advertiser]),
        );
        assert_eq!(service.topics.searches[&id].gap, LOOP_MIN_TIME);
    }
}
