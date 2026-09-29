//! Registrar-side handling of REGTOPIC and TOPICQUERY.

use super::Service;
use crate::{
    Enr,
    handler::HandlerIn,
    kbucket,
    node_info::NodeAddress,
    rpc::{RequestId, Response, ResponseBody},
    topic::{
        Ticket, TopicId,
        netutil::{check_regtopic_record_ip, check_relay_addr, preferred_ip, unmap},
        supports_topic_discovery,
    },
};
use enr::NodeId;
use rand::RngExt;
use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    time::{Duration, Instant},
};
use tracing::{debug, trace, warn};

/// Records per NODES / TOPICNODES message, as the Go implementation packs them.
const NODES_RESPONSE_ITEM_LIMIT: usize = 3;

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
        let now = Instant::now();
        let ticket = match self.ticket_sealer.unpack(&topic, &ticket, now) {
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

        let new_wait = self.topic_table.register(&enr, topic, waited, now);
        let (ticket, wait_time) = if new_wait > Duration::ZERO {
            let first_issued = ticket.map_or(now, |t| t.first_issued);
            let mut ticket = Ticket {
                key_id: 0,
                topic,
                wait_time_issued: new_wait,
                last_used: now,
                first_issued,
            };
            (
                self.ticket_sealer.pack(&mut ticket, now),
                wait_time_ms(new_wait),
            )
        } else {
            (Vec::new(), wait_time_ms(self.topic_table.ad_lifetime()))
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
        let src_ip = node_address.socket_addr.ip();
        let aux_responses =
            pack_node_records(self.collect_topic_aux_nodes(&topic, &buckets, src_ip));

        let topic_nodes =
            self.topic_table
                .random_nodes(&topic, self.config.topic.topic_nodes_limit, |enr| {
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
        let limit = self.config.topic.aux_nodes_limit.min(requested.len());
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
}

/// Wait time in whole milliseconds, rounded up.
fn wait_time_ms(d: Duration) -> u64 {
    (d.as_secs_f64() * 1000.0).ceil() as u64
}

fn pack_node_records(nodes: Vec<Enr>) -> Vec<Vec<Enr>> {
    nodes
        .chunks(NODES_RESPONSE_ITEM_LIMIT)
        .map(<[Enr]>::to_vec)
        .collect()
}
