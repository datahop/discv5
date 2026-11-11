use alloy_rlp::{
    Decodable, Encodable, Error as DecoderError, Header,
    bytes::{Buf, Bytes, BytesMut},
};
use enr::{CombinedKey, Enr};
use std::{
    convert::TryInto,
    net::{IpAddr, Ipv6Addr},
    num::NonZeroU16,
};
use tracing::{debug, warn};

/// Type to manage the request IDs.
#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct RequestId(pub Vec<u8>);

impl From<RequestId> for Vec<u8> {
    fn from(id: RequestId) -> Self {
        id.0
    }
}

impl RequestId {
    /// Decodes the ID from a raw bytes.
    pub fn decode(data: Vec<u8>) -> Result<Self, DecoderError> {
        if data.len() > 8 {
            return Err(DecoderError::Custom("Invalid ID length"));
        }
        Ok(RequestId(data))
    }

    pub fn random() -> Self {
        let rand: u64 = rand::random();
        RequestId(rand.to_be_bytes().to_vec())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A combined type representing requests and responses.
pub enum Message {
    /// A request, which contains its [`RequestId`].
    Request(Request),
    /// A Response, which contains the [`RequestId`] of its associated request.
    Response(Response),
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A request sent between nodes.
pub struct Request {
    /// The [`RequestId`] of the request.
    pub id: RequestId,
    /// The body of the request.
    pub body: RequestBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A response sent in response to a [`Request`]
pub struct Response {
    /// The [`RequestId`] of the request that triggered this response.
    pub id: RequestId,
    /// The body of this response.
    pub body: ResponseBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestBody {
    /// A PING request.
    Ping {
        /// Our current ENR sequence number.
        enr_seq: u64,
    },
    /// A FINDNODE request.
    FindNode {
        /// The distance(s) of peers we expect to be returned in the response.
        distances: Vec<u64>,
    },
    /// A Talk request.
    Talk {
        /// The protocol requesting.
        protocol: Vec<u8>,
        /// The request.
        request: Vec<u8>,
    },
    /// A REGTOPIC request: register the sender's record for a topic.
    #[cfg(feature = "topic-discovery")]
    RegTopic {
        /// The topic to register for.
        topic: [u8; 32],
        /// The ticket issued by the recipient on the previous attempt; empty on the first.
        ticket: Vec<u8>,
        /// The sender's current record.
        enr: RegTopicEnr,
        /// Log2 distances from the topic where the sender's service table has space.
        buckets: Vec<u64>,
    },
    /// A TOPICQUERY request: ask for advertisers registered for a topic.
    #[cfg(feature = "topic-discovery")]
    TopicQuery {
        /// The topic to look up.
        topic: [u8; 32],
        /// Log2 distances from the topic where the sender's service table has space.
        buckets: Vec<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseBody {
    /// A PONG response.
    Pong {
        /// The current ENR sequence number of the responder.
        enr_seq: u64,
        /// Our external IP address as observed by the responder.
        ip: IpAddr,
        /// Our external UDP port as observed by the responder.
        port: NonZeroU16,
    },
    /// A NODES response.
    Nodes {
        /// The total number of responses that make up this response.
        total: u64,
        /// A list of ENR's returned by the responder.
        nodes: Vec<Enr<CombinedKey>>,
    },
    /// The TALK response.
    Talk {
        /// The response for the talk.
        response: Vec<u8>,
    },
    /// The REGCONFIRMATION response to a REGTOPIC.
    #[cfg(feature = "topic-discovery")]
    RegConfirmation {
        /// The total number of responses (this one plus auxiliary NODES) to the request.
        total: u64,
        /// The ticket for the next attempt; empty when the registration was admitted.
        ticket: Vec<u8>,
        /// Milliseconds to wait before the next attempt, or the advertisement lifetime
        /// when admitted.
        wait_time: u64,
    },
    /// A TOPICNODES response to a TOPICQUERY.
    #[cfg(feature = "topic-discovery")]
    TopicNodes {
        /// The total number of responses (TOPICNODES plus auxiliary NODES) to the request.
        total: u64,
        /// Advertisers registered for the requested topic.
        nodes: Vec<Enr<CombinedKey>>,
    },
}

#[cfg(feature = "topic-discovery")]
use crate::topic::params::{MAX_TOPIC_DISTANCES, MAX_TOPIC_NODES};

#[cfg(feature = "topic-discovery")]
impl RequestBody {
    /// REGTOPIC and TOPICQUERY: the requests answered by several kinds of messages.
    pub fn is_topic_request(&self) -> bool {
        matches!(
            self,
            RequestBody::RegTopic { .. } | RequestBody::TopicQuery { .. }
        )
    }
}

impl Request {
    pub fn msg_type(&self) -> u8 {
        match self.body {
            RequestBody::Ping { .. } => 1,
            RequestBody::FindNode { .. } => 3,
            RequestBody::Talk { .. } => 5,
            #[cfg(feature = "topic-discovery")]
            RequestBody::RegTopic { .. } => 7,
            #[cfg(feature = "topic-discovery")]
            RequestBody::TopicQuery { .. } => 9,
        }
    }

    /// Encodes a Message to RLP-encoded bytes.
    pub fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(10);
        let msg_type = self.msg_type();
        buf.push(msg_type);
        let id = &self.id;
        match self.body {
            RequestBody::Ping { enr_seq } => {
                let mut list = Vec::<u8>::new();
                id.as_bytes().encode(&mut list);
                enr_seq.encode(&mut list);
                let header = Header {
                    list: true,
                    payload_length: list.len(),
                };
                header.encode(&mut buf);
                buf.extend_from_slice(&list);
                buf
            }
            RequestBody::FindNode { distances } => {
                let mut list = Vec::<u8>::new();
                id.as_bytes().encode(&mut list);
                distances.encode(&mut list);
                let header = Header {
                    list: true,
                    payload_length: list.len(),
                };
                header.encode(&mut buf);
                buf.extend_from_slice(&list);
                buf
            }
            RequestBody::Talk { protocol, request } => {
                let mut list = Vec::<u8>::new();
                id.as_bytes().encode(&mut list);
                protocol.as_slice().encode(&mut list);
                request.as_slice().encode(&mut list);
                let header = Header {
                    list: true,
                    payload_length: list.len(),
                };
                header.encode(&mut buf);
                buf.extend_from_slice(&list);
                buf
            }
            // Field order follows the Go implementation (ticket before ENR).
            #[cfg(feature = "topic-discovery")]
            RequestBody::RegTopic {
                topic,
                ticket,
                enr,
                buckets,
            } => {
                let mut list = Vec::<u8>::new();
                id.as_bytes().encode(&mut list);
                topic.encode(&mut list);
                ticket.as_slice().encode(&mut list);
                list.extend_from_slice(&enr.rlp);
                buckets.encode(&mut list);
                wrap_list(buf, &list)
            }
            #[cfg(feature = "topic-discovery")]
            RequestBody::TopicQuery { topic, buckets } => {
                let mut list = Vec::<u8>::new();
                id.as_bytes().encode(&mut list);
                topic.encode(&mut list);
                buckets.encode(&mut list);
                wrap_list(buf, &list)
            }
        }
    }
}

impl Response {
    pub fn msg_type(&self) -> u8 {
        match &self.body {
            ResponseBody::Pong { .. } => 2,
            ResponseBody::Nodes { .. } => 4,
            ResponseBody::Talk { .. } => 6,
            #[cfg(feature = "topic-discovery")]
            ResponseBody::RegConfirmation { .. } => 8,
            #[cfg(feature = "topic-discovery")]
            ResponseBody::TopicNodes { .. } => 10,
        }
    }

    /// Determines if the response is a valid response to the given request.
    pub fn match_request(&self, req: &RequestBody) -> bool {
        match self.body {
            ResponseBody::Pong { .. } => matches!(req, RequestBody::Ping { .. }),
            #[cfg(not(feature = "topic-discovery"))]
            ResponseBody::Nodes { .. } => {
                matches!(req, RequestBody::FindNode { .. })
            }
            // Topic requests are also answered with NODES, for the auxiliary nodes.
            #[cfg(feature = "topic-discovery")]
            ResponseBody::Nodes { .. } => {
                matches!(
                    req,
                    RequestBody::FindNode { .. }
                        | RequestBody::RegTopic { .. }
                        | RequestBody::TopicQuery { .. }
                )
            }
            ResponseBody::Talk { .. } => matches!(req, RequestBody::Talk { .. }),
            #[cfg(feature = "topic-discovery")]
            ResponseBody::RegConfirmation { .. } => matches!(req, RequestBody::RegTopic { .. }),
            #[cfg(feature = "topic-discovery")]
            ResponseBody::TopicNodes { .. } => matches!(req, RequestBody::TopicQuery { .. }),
        }
    }

    /// Encodes a Message to RLP-encoded bytes.
    pub fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(10);
        let msg_type = self.msg_type();
        buf.push(msg_type);
        let id = &self.id;
        match self.body {
            ResponseBody::Pong { enr_seq, ip, port } => {
                let mut list = Vec::<u8>::new();
                id.as_bytes().encode(&mut list);
                enr_seq.encode(&mut list);
                match ip {
                    IpAddr::V4(addr) => addr.encode(&mut list),
                    IpAddr::V6(addr) => addr.encode(&mut list),
                };
                port.get().encode(&mut list);
                let header = Header {
                    list: true,
                    payload_length: list.len(),
                };
                header.encode(&mut buf);
                buf.extend_from_slice(&list);
                buf
            }
            ResponseBody::Nodes { total, nodes } => {
                let mut list = Vec::<u8>::new();
                id.as_bytes().encode(&mut list);
                total.encode(&mut list);
                encode_enr_list(&nodes, &mut list);
                wrap_list(buf, &list)
            }
            ResponseBody::Talk { response } => {
                let mut list = Vec::<u8>::new();
                id.as_bytes().encode(&mut list);
                response.as_slice().encode(&mut list);
                let header = Header {
                    list: true,
                    payload_length: list.len(),
                };
                header.encode(&mut buf);
                buf.extend_from_slice(&list);
                buf
            }
            #[cfg(feature = "topic-discovery")]
            ResponseBody::RegConfirmation {
                total,
                ticket,
                wait_time,
            } => {
                let mut list = Vec::<u8>::new();
                id.as_bytes().encode(&mut list);
                total.encode(&mut list);
                ticket.as_slice().encode(&mut list);
                wait_time.encode(&mut list);
                wrap_list(buf, &list)
            }
            #[cfg(feature = "topic-discovery")]
            ResponseBody::TopicNodes { total, nodes } => {
                let mut list = Vec::<u8>::new();
                id.as_bytes().encode(&mut list);
                total.encode(&mut list);
                encode_enr_list(&nodes, &mut list);
                wrap_list(buf, &list)
            }
        }
    }
}

impl ResponseBody {
    /// The announced number of messages making up the whole response, for the response
    /// types that can span several messages.
    pub fn total(&self) -> Option<u64> {
        match self {
            ResponseBody::Nodes { total, .. } => Some(*total),
            #[cfg(feature = "topic-discovery")]
            ResponseBody::RegConfirmation { total, .. }
            | ResponseBody::TopicNodes { total, .. } => Some(*total),
            _ => None,
        }
    }
}

/// Appends the RLP list header for `list` to `buf`, followed by `list`.
fn wrap_list(mut buf: Vec<u8>, list: &[u8]) -> Vec<u8> {
    Header {
        list: true,
        payload_length: list.len(),
    }
    .encode(&mut buf);
    buf.extend_from_slice(list);
    buf
}

/// Encodes records as an RLP list of records.
fn encode_enr_list(nodes: &[Enr<CombinedKey>], out: &mut Vec<u8>) {
    let mut items = BytesMut::new();
    for node in nodes {
        node.encode(&mut items);
    }
    Header {
        list: true,
        payload_length: items.len(),
    }
    .encode(out);
    out.extend_from_slice(&items);
}

/// Decodes one record, bounding the decoder to the record's own RLP item.
fn decode_enr(payload: &mut &[u8]) -> Result<Enr<CombinedKey>, DecoderError> {
    let header = Header::decode(&mut &payload[..])?;
    if !header.list {
        return Err(DecoderError::Custom("Invalid format of header"));
    }
    let len = header.length_with_payload();
    if len > payload.len() {
        return Err(DecoderError::Custom(
            "Payload size is smaller than payload_length",
        ));
    }
    let enr = Enr::<CombinedKey>::decode(&mut &payload[..len])?;
    payload.advance(len);
    Ok(enr)
}

/// The record of a REGTOPIC: its RLP, and the record itself when it is valid. A
/// well-formed record that does not verify is kept so the registrar can reject it.
#[cfg(feature = "topic-discovery")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegTopicEnr {
    pub rlp: Vec<u8>,
    pub enr: Option<Box<Enr<CombinedKey>>>,
}

#[cfg(feature = "topic-discovery")]
impl From<Enr<CombinedKey>> for RegTopicEnr {
    fn from(enr: Enr<CombinedKey>) -> Self {
        let mut rlp = Vec::new();
        enr.encode(&mut rlp);
        RegTopicEnr {
            rlp,
            enr: Some(Box::new(enr)),
        }
    }
}

#[cfg(feature = "topic-discovery")]
impl RegTopicEnr {
    /// Decodes a record that is well-formed as in go-ethereum's `enr.Record`: at most
    /// 300 bytes, a list of signature, sequence number and strictly sorted key/value
    /// pairs. Anything else is a decoding error.
    pub(crate) fn decode(payload: &mut &[u8]) -> Result<Self, DecoderError> {
        const MAX_ENR_SIZE: usize = 300;
        let mut rest = *payload;
        let header = Header::decode(&mut rest)?;
        if !header.list {
            return Err(DecoderError::UnexpectedString);
        }
        if header.payload_length > rest.len() {
            return Err(DecoderError::InputTooShort);
        }
        let len = payload.len() - rest.len() + header.payload_length;
        let rlp = payload[..len].to_vec();
        if len > MAX_ENR_SIZE {
            return Err(DecoderError::Custom("enr exceeds max size"));
        }

        let mut items = &rest[..header.payload_length];
        Header::decode_bytes(&mut items, false)?;
        u64::decode(&mut items)?;
        let mut prev: Option<&[u8]> = None;
        while !items.is_empty() {
            let key = Header::decode_bytes(&mut items, false)?;
            if prev.is_some_and(|prev| prev >= key) {
                return Err(DecoderError::Custom("Unsorted or duplicate keys"));
            }
            prev = Some(key);
            if items.is_empty() {
                return Err(DecoderError::Custom("Incomplete key/value pair"));
            }
            let value = Header::decode(&mut items)?;
            if value.payload_length > items.len() {
                return Err(DecoderError::InputTooShort);
            }
            items.advance(value.payload_length);
        }

        let enr = Enr::<CombinedKey>::decode(&mut &rlp[..]).ok().map(Box::new);
        payload.advance(len);
        Ok(RegTopicEnr { rlp, enr })
    }
}

/// Decodes the REGCONFIRMATION wait time. Values wider than 64 bits saturate.
#[cfg(feature = "topic-discovery")]
fn decode_wait_time(payload: &mut &[u8]) -> Result<u64, DecoderError> {
    let mut rest = *payload;
    let bytes = Header::decode_bytes(&mut rest, false)?;
    if bytes.len() <= 8 {
        return u64::decode(payload);
    }
    if bytes[0] == 0 {
        return Err(DecoderError::LeadingZero);
    }
    *payload = rest;
    Ok(u64::MAX)
}

/// Decodes an RLP list of records. With `skipped`, a well-formed record that fails
/// verification is dropped and counted instead of failing the list.
fn decode_enr_list(
    payload: &mut &[u8],
    skipped: Option<&mut usize>,
) -> Result<Vec<Enr<CombinedKey>>, DecoderError> {
    let header = Header::decode(payload)?;
    if !header.list {
        return Err(DecoderError::Custom("Invalid format of header"));
    }
    if header.payload_length > payload.len() {
        return Err(DecoderError::Custom(
            "Payload size is smaller than payload_length",
        ));
    }
    let (mut items, rest) = payload.split_at(header.payload_length);
    let mut nodes = Vec::new();
    match skipped {
        None => {
            while !items.is_empty() {
                nodes.push(decode_enr(&mut items)?);
            }
        }
        #[cfg(feature = "topic-discovery")]
        Some(skipped) => {
            while !items.is_empty() {
                match RegTopicEnr::decode(&mut items)?.enr {
                    Some(enr) => nodes.push(*enr),
                    None => *skipped += 1,
                }
            }
        }
        #[cfg(not(feature = "topic-discovery"))]
        Some(_) => unreachable!("records are only skipped with topic discovery"),
    }
    *payload = rest;
    Ok(nodes)
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Request(request) => write!(f, "{request}"),
            Message::Response(response) => write!(f, "{response}"),
        }
    }
}

impl std::fmt::Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Response: id: {}: {}", self.id, self.body)
    }
}

impl std::fmt::Display for ResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseBody::Pong { enr_seq, ip, port } => {
                write!(f, "PONG: Enr-seq: {enr_seq}, Ip: {ip:?},  Port: {port}")
            }
            ResponseBody::Nodes { total, nodes } => {
                write!(f, "NODES: total: {total}, Nodes: [")?;
                let mut first = true;
                for id in nodes {
                    if !first {
                        write!(f, ", {id}")?;
                    } else {
                        write!(f, "{id}")?;
                    }
                    first = false;
                }

                write!(f, "]")
            }
            ResponseBody::Talk { response } => {
                write!(f, "Response: Response {}", hex::encode(response))
            }
            #[cfg(feature = "topic-discovery")]
            ResponseBody::RegConfirmation {
                total,
                ticket,
                wait_time,
            } => write!(
                f,
                "REGCONFIRMATION: total: {total}, admitted: {}, wait_time: {wait_time} ms",
                ticket.is_empty()
            ),
            #[cfg(feature = "topic-discovery")]
            ResponseBody::TopicNodes { total, nodes } => {
                write!(f, "TOPICNODES: total: {total}, Nodes: [")?;
                for (i, node) in nodes.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{node}")?;
                }
                write!(f, "]")
            }
        }
    }
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Request: id: {}: {}", self.id, self.body)
    }
}

impl std::fmt::Display for RequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestBody::Ping { enr_seq } => write!(f, "PING: enr_seq: {enr_seq}"),
            RequestBody::FindNode { distances } => {
                write!(f, "FINDNODE Request: distance: {distances:?}")
            }
            RequestBody::Talk { protocol, request } => write!(
                f,
                "TALK: protocol: {}, request: {}",
                hex::encode(protocol),
                hex::encode(request)
            ),
            #[cfg(feature = "topic-discovery")]
            RequestBody::RegTopic {
                topic,
                ticket,
                buckets,
                ..
            } => write!(
                f,
                "REGTOPIC: topic: {}, ticket: {} bytes, distances: {buckets:?}",
                hex::encode(topic),
                ticket.len()
            ),
            #[cfg(feature = "topic-discovery")]
            RequestBody::TopicQuery { topic, buckets } => write!(
                f,
                "TOPICQUERY: topic: {}, distances: {buckets:?}",
                hex::encode(topic)
            ),
        }
    }
}
#[allow(dead_code)]
impl Message {
    pub fn encode(self) -> Vec<u8> {
        match self {
            Self::Request(request) => request.encode(),
            Self::Response(response) => response.encode(),
        }
    }

    pub fn decode(data: &[u8]) -> Result<Self, DecoderError> {
        Self::decode_counting(data, None)
    }

    /// Decodes like `decode`, but drops the records of NODES and TOPICNODES that fail
    /// verification instead of the whole message, and returns their number.
    #[cfg(feature = "topic-discovery")]
    pub fn decode_lenient(data: &[u8]) -> Result<(Self, usize), DecoderError> {
        let mut skipped = 0;
        let message = Self::decode_counting(data, Some(&mut skipped))?;
        Ok((message, skipped))
    }

    fn decode_counting(data: &[u8], skipped: Option<&mut usize>) -> Result<Self, DecoderError> {
        if data.len() < 3 {
            return Err(DecoderError::InputTooShort);
        }

        let msg_type = data[0];

        let payload = &mut &data[1..];

        let header = Header::decode(payload)?;
        if !header.list {
            return Err(DecoderError::Custom("Invalid format of header"));
        }

        if header.payload_length != payload.len() {
            return Err(DecoderError::Custom("Reject the extra data"));
        }

        let id_bytes = Bytes::decode(payload)?;
        let id = RequestId::decode(id_bytes.to_vec())?;

        let message = match msg_type {
            1 => {
                // PingRequest
                let enr_seq = u64::decode(payload)?;
                if !payload.is_empty() {
                    return Err(DecoderError::Custom("Payload should be empty"));
                }
                Message::Request(Request {
                    id,
                    body: RequestBody::Ping { enr_seq },
                })
            }
            2 => {
                // PingResponse
                let enr_seq = u64::decode(payload)?;
                let ip_bytes = Bytes::decode(payload)?;
                let ip = match ip_bytes.len() {
                    4 => {
                        let mut ip = [0u8; 4];
                        ip.copy_from_slice(&ip_bytes);
                        IpAddr::from(ip)
                    }
                    16 => {
                        let mut ip = [0u8; 16];
                        ip.copy_from_slice(&ip_bytes);
                        let ipv6 = Ipv6Addr::from(ip);

                        if ipv6.is_loopback() {
                            // Checking if loopback address since IPv6Addr::to_ipv4 returns
                            // IPv4 address for IPv6 loopback address.
                            IpAddr::V6(ipv6)
                        } else if let Some(ipv4) = ipv6.to_ipv4() {
                            // If the ipv6 is ipv4 compatible/mapped, simply return the ipv4.
                            IpAddr::V4(ipv4)
                        } else {
                            IpAddr::V6(ipv6)
                        }
                    }
                    _ => {
                        debug!("Ping Response has incorrect byte length for IP");
                        return Err(DecoderError::Custom("Incorrect List Length"));
                    }
                };
                let raw_port = u16::decode(payload)?;
                if let Ok(port) = raw_port.try_into() {
                    if !payload.is_empty() {
                        return Err(DecoderError::Custom("Payload should be empty"));
                    }
                    Message::Response(Response {
                        id,
                        body: ResponseBody::Pong { enr_seq, ip, port },
                    })
                } else {
                    debug!(raw_port, "The port number should be non zero");
                    return Err(DecoderError::Custom("PONG response port number invalid"));
                }
            }
            3 => {
                // FindNodeRequest
                let distances = Vec::<u64>::decode(payload)?;

                for distance in distances.iter() {
                    if distance > &256u64 {
                        warn!(
                            distance,
                            "Rejected FindNode request asking for unknown distance maximum 256",
                        );
                        return Err(DecoderError::Custom("FINDNODE request distance invalid"));
                    }
                }
                if !payload.is_empty() {
                    return Err(DecoderError::Custom("Payload should be empty"));
                }
                Message::Request(Request {
                    id,
                    body: RequestBody::FindNode { distances },
                })
            }
            4 => {
                // NodesResponse
                let total = u64::decode(payload)?;
                let nodes = decode_enr_list(payload, skipped)?;
                if !payload.is_empty() {
                    return Err(DecoderError::Custom("Payload should be empty"));
                }
                Message::Response(Response {
                    id,
                    body: ResponseBody::Nodes { total, nodes },
                })
            }
            5 => {
                // Talk Request
                let protocol = Bytes::decode(payload)?.to_vec();
                let request = Bytes::decode(payload)?.to_vec();
                if !payload.is_empty() {
                    return Err(DecoderError::Custom("Payload should be empty"));
                }
                Message::Request(Request {
                    id,
                    body: RequestBody::Talk { protocol, request },
                })
            }
            6 => {
                // Talk Response
                let response = Bytes::decode(payload)?;
                if !payload.is_empty() {
                    return Err(DecoderError::Custom("Payload should be empty"));
                }
                Message::Response(Response {
                    id,
                    body: ResponseBody::Talk {
                        response: response.to_vec(),
                    },
                })
            }
            #[cfg(feature = "topic-discovery")]
            7 => {
                // REGTOPIC: [request-id, topic, ticket, ENR, [distance, ...]]
                let topic = <[u8; 32]>::decode(payload)?;
                let ticket = Bytes::decode(payload)?.to_vec();
                let enr = RegTopicEnr::decode(payload)?;
                let buckets = Vec::<u64>::decode(payload)?;
                if buckets.len() > MAX_TOPIC_DISTANCES {
                    return Err(DecoderError::Custom("Too many distances in REGTOPIC"));
                }
                if !payload.is_empty() {
                    return Err(DecoderError::Custom("Payload should be empty"));
                }
                Message::Request(Request {
                    id,
                    body: RequestBody::RegTopic {
                        topic,
                        ticket,
                        enr,
                        buckets,
                    },
                })
            }
            #[cfg(feature = "topic-discovery")]
            8 => {
                // REGCONFIRMATION: [request-id, total, ticket, wait-time]
                let total = u64::from(u8::decode(payload)?);
                let ticket = Bytes::decode(payload)?.to_vec();
                let wait_time = decode_wait_time(payload)?;
                if !payload.is_empty() {
                    return Err(DecoderError::Custom("Payload should be empty"));
                }
                Message::Response(Response {
                    id,
                    body: ResponseBody::RegConfirmation {
                        total,
                        ticket,
                        wait_time,
                    },
                })
            }
            #[cfg(feature = "topic-discovery")]
            9 => {
                // TOPICQUERY: [request-id, topic, [distance, ...]]
                let topic = <[u8; 32]>::decode(payload)?;
                let buckets = Vec::<u64>::decode(payload)?;
                if buckets.len() > MAX_TOPIC_DISTANCES {
                    return Err(DecoderError::Custom("Too many distances in TOPICQUERY"));
                }
                if !payload.is_empty() {
                    return Err(DecoderError::Custom("Payload should be empty"));
                }
                Message::Request(Request {
                    id,
                    body: RequestBody::TopicQuery { topic, buckets },
                })
            }
            #[cfg(feature = "topic-discovery")]
            10 => {
                // TOPICNODES: [request-id, total, [ENR, ...]]
                let total = u64::from(u8::decode(payload)?);
                let mut dropped = 0;
                let nodes = decode_enr_list(payload, skipped.is_some().then_some(&mut dropped))?;
                if nodes.len() + dropped > MAX_TOPIC_NODES {
                    return Err(DecoderError::Custom("Too many nodes in TOPICNODES"));
                }
                if let Some(skipped) = skipped {
                    *skipped += dropped;
                }
                if !payload.is_empty() {
                    return Err(DecoderError::Custom("Payload should be empty"));
                }
                Message::Response(Response {
                    id,
                    body: ResponseBody::TopicNodes { total, nodes },
                })
            }
            _ => {
                return Err(DecoderError::Custom("Unknown RPC message type"));
            }
        };

        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn ref_test_encode_request_ping() {
        // reference input
        let id = RequestId(vec![1]);
        let enr_seq = 1;
        let message = Message::Request(Request {
            id,
            body: RequestBody::Ping { enr_seq },
        });

        // expected hex output
        let expected_output = hex::decode("01c20101").unwrap();

        dbg!(hex::encode(message.clone().encode()));
        assert_eq!(message.encode(), expected_output);
    }

    #[test]
    fn ref_test_encode_request_findnode() {
        // reference input
        let id = RequestId(vec![1]);
        let distances = vec![256];
        let message = Message::Request(Request {
            id,
            body: RequestBody::FindNode { distances },
        });

        // expected hex output
        let expected_output = hex::decode("03c501c3820100").unwrap();
        dbg!(hex::encode(message.clone().encode()));

        assert_eq!(message.encode(), expected_output);
    }

    #[test]
    fn ref_test_encode_response_ping() {
        // reference input
        let id = RequestId(vec![1]);
        let enr_seq = 1;
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let port = 5000;
        let message = Message::Response(Response {
            id,
            body: ResponseBody::Pong {
                enr_seq,
                ip,
                port: port.try_into().unwrap(),
            },
        });

        // expected hex output
        let expected_output = hex::decode("02ca0101847f000001821388").unwrap();

        dbg!(hex::encode(message.clone().encode()));
        assert_eq!(message.encode(), expected_output);
    }

    #[test]
    fn ref_test_encode_response_nodes_empty() {
        // reference input
        let id = RequestId(vec![1]);
        let total = 1;

        // expected hex output
        let expected_output = hex::decode("04c30101c0").unwrap();

        let message = Message::Response(Response {
            id,
            body: ResponseBody::Nodes {
                total,
                nodes: vec![],
            },
        });
        assert_eq!(message.encode(), expected_output);
    }

    #[test]
    fn ref_test_encode_response_nodes() {
        // reference input
        let id = RequestId(vec![1]);
        let total = 1;

        let enr = "-HW4QCjfjuCfSmIJHxqLYfGKrSz-Pq3G81DVJwd_muvFYJiIOkf0bGtJu7kZVCOPnhSTMneyvR4MRbF3G5TNB4wy2ssBgmlkgnY0iXNlY3AyNTZrMaEDymNMrg1JrLQB2KTGtv6MVbcNEVv0AHacwUAPMljNMTg".parse::<Enr<CombinedKey>>().unwrap();
        // expected hex output
        let expected_output = hex::decode("04f87b0101f877f875b84028df8ee09f4a62091f1a8b61f18aad2cfe3eadc6f350d527077f9aebc56098883a47f46c6b49bbb91954238f9e14933277b2bd1e0c45b1771b94cd078c32dacb0182696482763489736563703235366b31a103ca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138").unwrap();

        let message = Message::Response(Response {
            id,
            body: ResponseBody::Nodes {
                total,
                nodes: vec![enr],
            },
        });
        dbg!(hex::encode(message.clone().encode()));
        assert_eq!(message.encode(), expected_output);
    }

    #[test]
    fn ref_test_encode_response_nodes_multiple() {
        // reference input
        let id = RequestId(vec![1]);
        let total = 1;
        let enr = "enr:-HW4QBzimRxkmT18hMKaAL3IcZF1UcfTMPyi3Q1pxwZZbcZVRI8DC5infUAB_UauARLOJtYTxaagKoGmIjzQxO2qUygBgmlkgnY0iXNlY3AyNTZrMaEDymNMrg1JrLQB2KTGtv6MVbcNEVv0AHacwUAPMljNMTg".parse::<Enr<CombinedKey>>().unwrap();

        let enr2 = "enr:-HW4QNfxw543Ypf4HXKXdYxkyzfcxcO-6p9X986WldfVpnVTQX1xlTnWrktEWUbeTZnmgOuAY_KUhbVV1Ft98WoYUBMBgmlkgnY0iXNlY3AyNTZrMaEDDiy3QkHAxPyOgWbxp5oF1bDdlYE6dLCUUp8xfVw50jU".parse::<Enr<CombinedKey>>().unwrap();

        // expected hex output
        let expected_output = hex::decode("04f8f20101f8eef875b8401ce2991c64993d7c84c29a00bdc871917551c7d330fca2dd0d69c706596dc655448f030b98a77d4001fd46ae0112ce26d613c5a6a02a81a6223cd0c4edaa53280182696482763489736563703235366b31a103ca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138f875b840d7f1c39e376297f81d7297758c64cb37dcc5c3beea9f57f7ce9695d7d5a67553417d719539d6ae4b445946de4d99e680eb8063f29485b555d45b7df16a1850130182696482763489736563703235366b31a1030e2cb74241c0c4fc8e8166f1a79a05d5b0dd95813a74b094529f317d5c39d235").unwrap();

        let message = Message::Response(Response {
            id,
            body: ResponseBody::Nodes {
                total,
                nodes: vec![enr, enr2],
            },
        });
        dbg!(hex::encode(message.clone().encode()));
        assert_eq!(message.encode(), expected_output);
    }

    #[test]
    fn ref_decode_response_nodes_multiple() {
        let input = hex::decode("04f8f20101f8eef875b8401ce2991c64993d7c84c29a00bdc871917551c7d330fca2dd0d69c706596dc655448f030b98a77d4001fd46ae0112ce26d613c5a6a02a81a6223cd0c4edaa53280182696482763489736563703235366b31a103ca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138f875b840d7f1c39e376297f81d7297758c64cb37dcc5c3beea9f57f7ce9695d7d5a67553417d719539d6ae4b445946de4d99e680eb8063f29485b555d45b7df16a1850130182696482763489736563703235366b31a1030e2cb74241c0c4fc8e8166f1a79a05d5b0dd95813a74b094529f317d5c39d235").unwrap();

        let expected_enr1 = "enr:-HW4QBzimRxkmT18hMKaAL3IcZF1UcfTMPyi3Q1pxwZZbcZVRI8DC5infUAB_UauARLOJtYTxaagKoGmIjzQxO2qUygBgmlkgnY0iXNlY3AyNTZrMaEDymNMrg1JrLQB2KTGtv6MVbcNEVv0AHacwUAPMljNMTg".parse::<Enr<CombinedKey>>().unwrap();
        let expected_enr2 = "enr:-HW4QNfxw543Ypf4HXKXdYxkyzfcxcO-6p9X986WldfVpnVTQX1xlTnWrktEWUbeTZnmgOuAY_KUhbVV1Ft98WoYUBMBgmlkgnY0iXNlY3AyNTZrMaEDDiy3QkHAxPyOgWbxp5oF1bDdlYE6dLCUUp8xfVw50jU".parse::<Enr<CombinedKey>>().unwrap();

        let decoded = Message::decode(&input).unwrap();

        match decoded {
            Message::Response(response) => match response.body {
                ResponseBody::Nodes { total, nodes } => {
                    assert_eq!(total, 1);
                    assert_eq!(nodes[0], expected_enr1);
                    assert_eq!(nodes[1], expected_enr2);
                }
                _ => panic!("Invalid decoding"),
            },
            _ => panic!("Invalid decoding"),
        }
    }

    #[test]
    fn encode_decode_ping_request() {
        let id = RequestId(vec![1]);
        let request = Message::Request(Request {
            id,
            body: RequestBody::Ping { enr_seq: 15 },
        });

        let encoded = request.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();

        assert_eq!(request, decoded);
    }

    #[test]
    fn encode_decode_ping_response() {
        let id = RequestId(vec![1]);
        let request = Message::Response(Response {
            id,
            body: ResponseBody::Pong {
                enr_seq: 15,
                ip: "127.0.0.1".parse().unwrap(),
                port: 80.try_into().unwrap(),
            },
        });

        let encoded = request.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();

        assert_eq!(request, decoded);
    }

    #[test]
    fn encode_decode_ping_response_ipv4_mapped() {
        let id = RequestId(vec![1]);
        let request = Message::Response(Response {
            id: id.clone(),
            body: ResponseBody::Pong {
                enr_seq: 15,
                ip: IpAddr::V6(Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped()),
                port: 80.try_into().unwrap(),
            },
        });

        let encoded = request.encode();
        let decoded = Message::decode(&encoded).unwrap();
        let expected = Message::Response(Response {
            id,
            body: ResponseBody::Pong {
                enr_seq: 15,
                ip: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
                port: 80.try_into().unwrap(),
            },
        });

        assert_eq!(expected, decoded);
    }

    #[test]
    fn encode_decode_ping_response_ipv6_loopback() {
        let id = RequestId(vec![1]);
        let request = Message::Response(Response {
            id,
            body: ResponseBody::Pong {
                enr_seq: 15,
                ip: IpAddr::V6(Ipv6Addr::LOCALHOST),
                port: 80.try_into().unwrap(),
            },
        });

        let encoded = request.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();

        assert_eq!(request, decoded);
    }

    #[test]
    fn encode_decode_find_node_request() {
        let id = RequestId(vec![1]);
        let request = Message::Request(Request {
            id,
            body: RequestBody::FindNode {
                distances: vec![12],
            },
        });

        let encoded = request.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();

        assert_eq!(request, decoded);
    }

    #[test]
    fn encode_decode_nodes_response() {
        let key = CombinedKey::generate_secp256k1();
        let enr1 = Enr::builder()
            .ip4("127.0.0.1".parse().unwrap())
            .udp4(500)
            .build(&key)
            .unwrap();
        let enr2 = Enr::builder()
            .ip4("10.0.0.1".parse().unwrap())
            .tcp4(8080)
            .build(&key)
            .unwrap();
        let enr3 = Enr::builder()
            .ip("10.4.5.6".parse().unwrap())
            .build(&key)
            .unwrap();

        let enr_list = vec![enr1, enr2, enr3];
        let id = RequestId(vec![1]);
        let request = Message::Response(Response {
            id,
            body: ResponseBody::Nodes {
                total: 1,
                nodes: enr_list,
            },
        });

        let encoded = request.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();

        assert_eq!(request, decoded);
    }

    #[test]
    fn reject_extra_data() {
        let data = [6, 194, 0, 75];
        let msg = Message::decode(&data).unwrap();
        assert_eq!(
            msg,
            Message::Response(Response {
                id: RequestId(vec![0]),
                body: ResponseBody::Talk { response: vec![75] }
            })
        );
        assert_eq!(data.to_vec(), msg.encode());

        let data2 = [6, 193, 0, 75, 252];
        Message::decode(&data2).expect_err("should reject extra data");

        let data3 = [6, 194, 0, 75, 252];
        Message::decode(&data3).expect_err("should reject extra data");

        let data4 = [6, 193, 0, 63];
        Message::decode(&data4).expect_err("should reject extra data");

        let data5 = [6, 193, 128, 75];
        Message::decode(&data5).expect_err("should reject extra data");

        let data6 = [6, 193, 128, 128];
        Message::decode(&data6).expect_err("should reject extra data");
    }

    #[test]
    fn test_encode_request_talk_request() {
        // reference input
        let id = RequestId(vec![113, 236, 255, 66, 31, 191, 221, 86]);
        let message = Message::Request(Request {
            id,
            body: RequestBody::Talk {
                protocol: hex::decode("757470").unwrap(),
                request: hex::decode("0100a028839e1549000003ef001000007619dde7").unwrap(),
            },
        });

        // expected hex output
        let expected_output =
            hex::decode("05e28871ecff421fbfdd5683757470940100a028839e1549000003ef001000007619dde7")
                .unwrap();
        dbg!(hex::encode(message.clone().encode()));

        let encoded_message = message.clone().encode();
        assert_eq!(encoded_message.clone(), expected_output);
        assert_eq!(Message::decode(&encoded_message).unwrap(), message);
    }

    #[test]
    fn test_encode_request_talk_response() {
        // reference input
        let id = RequestId(vec![113, 236, 255, 66, 31, 191, 221, 86]);
        let message = Message::Response(Response {
            id,
            body: ResponseBody::Talk {
                response: hex::decode("0100a028839e1549000003ef001000007619dde7").unwrap(),
            },
        });

        // expected hex output
        let expected_output =
            hex::decode("06de8871ecff421fbfdd56940100a028839e1549000003ef001000007619dde7")
                .unwrap();

        dbg!(hex::encode(message.clone().encode()));

        let encoded_message = message.clone().encode();
        assert_eq!(encoded_message.clone(), expected_output);
        assert_eq!(Message::decode(&encoded_message).unwrap(), message);
    }

    #[test]
    fn reject_oversized_request_id() {
        // A PING request with a 9-byte request ID should be rejected.
        // The discv5 spec limits request IDs to a maximum of 8 bytes.
        let id = RequestId(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]); // 9 bytes
        let request = Request {
            id,
            body: RequestBody::Ping { enr_seq: 1 },
        };
        let encoded = request.encode();
        Message::decode(&encoded).expect_err("should reject request ID longer than 8 bytes");

        // An 8-byte request ID should be accepted.
        let id = RequestId(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let request = Message::Request(Request {
            id,
            body: RequestBody::Ping { enr_seq: 1 },
        });
        let encoded = request.clone().encode();
        let decoded = Message::decode(&encoded).expect("8-byte request ID should be valid");
        assert_eq!(request, decoded);
    }

    #[test]
    fn test_large_enr_hex_data() {
        // This is the exact hex data from the logs that was failing to decode
        let hex_data = "04f9012b88000000000000000001f9011ef9011bb840f986dbbaaf794e76c2dcd945e01686a6566409b4cf6ab2690fcdd347e958d35c2846bf61a001486623b6c805525e73a534212d313021c29b02bd0d85e799b8b9820162876174746e657473880080010000000000836367630986636c69656e74d18a4c69676874686f75736585372e312e30846574683290ce852d7070202361ffffffffffffffff8269648276348269708492be185a83697036902a03b0c0000200f000000000ca96f001836e666484ce852d70847175696382232985717569633682232989736563703235366b31a102463c7e28da88f61941e66ae44dd6d38a27f107ca2106158746b7d7f47eb765278873796e636e6574730f837463708223288474637036822328837564708223288475647036822328";

        let data = hex::decode(hex_data).expect("Valid hex data");

        // This should fail to decode with the current (original) logic
        Message::decode(&data).expect("This should be decodable");
    }
}

#[cfg(all(test, feature = "topic-discovery"))]
mod topic_tests {
    //! The spec's "TopDisc Message Encodings" test vectors, kept verbatim in
    //! `topic/testdata`.
    use super::*;
    use crate::topic::{supports_topic_discovery, topic_id};
    use enr::{EnrKey, NodeId};
    use std::collections::HashMap;

    const VECTORS: &str = include_str!("topic/testdata/topdisc-message-encodings.txt");

    /// The indented blocks of the vector file, in order, without the indent.
    fn blocks() -> Vec<Vec<&'static str>> {
        let mut blocks = Vec::new();
        let mut block = Vec::new();
        for line in VECTORS.lines() {
            match line.strip_prefix("    ") {
                Some(line) => block.push(line.trim()),
                None if !block.is_empty() => blocks.push(std::mem::take(&mut block)),
                None => {}
            }
        }
        if !block.is_empty() {
            blocks.push(block);
        }
        blocks
    }

    fn hex_block(block: &[&str]) -> Vec<u8> {
        hex::decode(block.concat()).unwrap()
    }

    /// The ENR of the vectors.
    fn record() -> Enr<CombinedKey> {
        Enr::decode(&mut &hex_block(&blocks()[0])[..]).unwrap()
    }

    fn topic() -> [u8; 32] {
        topic_id("example-service")
    }

    /// The bytes of the first vector whose comment starts with `first_field`.
    fn vector(first_field: &str) -> Vec<u8> {
        let blocks = blocks();
        let i = blocks
            .iter()
            .position(|b| b[0] == first_field)
            .expect("no such vector");
        hex_block(&blocks[i + 1])
    }

    /// Builds the message the `# kind.field = value` comment lines describe.
    fn message_from_fields(fields: &[&str]) -> Message {
        let mut kind = None;
        let mut values = HashMap::new();
        for line in fields {
            let (name, value) = line
                .strip_prefix("# ")
                .and_then(|l| l.split_once(" = "))
                .unwrap_or_else(|| panic!("bad field line {line}"));
            let (k, field) = name.split_once('.').unwrap();
            assert!(
                kind.is_none_or(|kind| kind == k),
                "mixed kinds in {fields:?}"
            );
            kind = Some(k);
            values.insert(field, value);
        }
        let mut take = |field: &str| {
            values
                .remove(field)
                .unwrap_or_else(|| panic!("missing field {field}"))
        };
        let bytes = |v: &str| match v {
            "(empty)" => Vec::new(),
            v => hex::decode(v.strip_prefix("0x").unwrap()).unwrap(),
        };
        let int = |v: &str| v.parse::<u64>().unwrap();
        let topic = |v: &str| {
            let service = v
                .strip_prefix("SHA-256(\"")
                .and_then(|v| v.strip_suffix("\")"))
                .unwrap();
            topic_id(service)
        };
        let enr = |v: &str| {
            assert_eq!(v, "the ENR above");
            record()
        };
        let list = |v: &str| -> Vec<String> {
            let v = v
                .strip_prefix('[')
                .and_then(|v| v.strip_suffix(']'))
                .unwrap();
            v.split(", ")
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        };
        let distances = |v: &str| list(v).iter().map(|d| int(d)).collect::<Vec<_>>();

        let id = RequestId(bytes(take("req-id")));
        let message = match kind.unwrap() {
            "regtopic" => Message::Request(Request {
                id,
                body: RequestBody::RegTopic {
                    topic: topic(take("topic")),
                    ticket: bytes(take("ticket")),
                    enr: enr(take("ENR")).into(),
                    buckets: distances(take("topic-distances")),
                },
            }),
            "regconfirmation" => Message::Response(Response {
                id,
                body: ResponseBody::RegConfirmation {
                    total: int(take("total")),
                    ticket: bytes(take("ticket")),
                    wait_time: int(take("wait-time")),
                },
            }),
            "topicquery" => Message::Request(Request {
                id,
                body: RequestBody::TopicQuery {
                    topic: topic(take("topic")),
                    buckets: distances(take("topic-distances")),
                },
            }),
            "topicnodes" => Message::Response(Response {
                id,
                body: ResponseBody::TopicNodes {
                    total: int(take("total")),
                    nodes: list(take("ENRs")).iter().map(|v| enr(v)).collect(),
                },
            }),
            kind => panic!("unknown message kind {kind}"),
        };
        assert!(values.is_empty(), "unchecked fields {values:?}");
        message
    }

    // Every spec vector decodes to the fields in its comment and encodes back to the
    // same bytes.
    #[test]
    fn spec_vectors_decode_encode() {
        let blocks = blocks();

        let enr = record();
        // node-a-key of the spec's test vectors.
        let mut node_a_key =
            hex::decode("eef77acb6c6a6eebc5b363a475ac583ec7eccdb42b6481424c60f59aa326547f")
                .unwrap();
        let node_a_key = CombinedKey::secp256k1_from_bytes(&mut node_a_key).unwrap();
        assert_eq!(enr.public_key(), node_a_key.public());
        assert_eq!(enr.node_id(), NodeId::from(node_a_key.public()));
        assert_eq!(enr.seq(), 1);
        assert_eq!(enr.ip4(), Some("127.0.0.1".parse().unwrap()));
        assert_eq!(enr.udp4(), Some(30303));
        assert!(supports_topic_discovery(&enr));
        let mut encoded = Vec::new();
        enr.encode(&mut encoded);
        assert_eq!(encoded, hex_block(&blocks[0]));

        assert_eq!(blocks[1][0], "topic = SHA-256(\"example-service\")");
        assert_eq!(
            blocks[1][1],
            format!("= 0x{}", hex::encode(topic_id("example-service")))
        );

        let mut checked = 0;
        for pair in blocks[2..].chunks(2) {
            let [fields, data] = pair else {
                panic!("vector without bytes: {pair:?}");
            };
            let expected = message_from_fields(fields);
            let data = hex_block(data);
            assert_eq!(
                Message::decode(&data).unwrap(),
                expected,
                "decode {fields:?}"
            );
            assert_eq!(
                hex::encode(expected.encode()),
                hex::encode(&data),
                "encode {fields:?}"
            );
            checked += 1;
        }
        assert_eq!(checked, 8);
    }

    // A wait time wider than 64 bits decodes as the largest one.
    #[test]
    fn wait_time_saturates() {
        let wait_time = |hex_msg: &str| match Message::decode(&hex::decode(hex_msg).unwrap()) {
            Ok(Message::Response(Response {
                body: ResponseBody::RegConfirmation { wait_time, .. },
                ..
            })) => Ok(wait_time),
            Ok(other) => panic!("not a REGCONFIRMATION: {other:?}"),
            Err(e) => Err(e),
        };
        let max = "08d08400000001018088ffffffffffffffff";
        assert_eq!(wait_time(max), Ok(u64::MAX));
        let wide = "08d184000000010180890100000000000000ff";
        assert_eq!(wait_time(wide), Ok(u64::MAX));
        let huge = format!("08e884000000010180a0{}", "ff".repeat(32));
        assert_eq!(wait_time(&huge), Ok(u64::MAX));
        // Leading zeros stay invalid.
        assert!(wait_time("08d1840000000101808900ffffffffffffffff").is_err());

        let saturated = Message::decode(&hex::decode(wide).unwrap()).unwrap();
        assert_eq!(hex::encode(saturated.encode()), max);
    }

    #[test]
    fn nodes_vector_unchanged() {
        // NODES shares the record-list codec of TOPICNODES.
        let mut data = vector("# topicnodes.req-id = 0x00000003");
        data[0] = 4;
        let message = Message::Response(Response {
            id: RequestId(vec![0, 0, 0, 3]),
            body: ResponseBody::Nodes {
                total: 1,
                nodes: vec![record()],
            },
        });
        assert_eq!(message.clone().encode(), data);
        assert_eq!(Message::decode(&data).unwrap(), message);
    }

    #[test]
    fn response_totals() {
        let confirmation = ResponseBody::RegConfirmation {
            total: 2,
            ticket: vec![],
            wait_time: 0,
        };
        assert_eq!(confirmation.total(), Some(2));
        assert!(
            Response {
                id: RequestId(vec![1]),
                body: confirmation
            }
            .match_request(&RequestBody::RegTopic {
                topic: topic(),
                ticket: vec![],
                enr: record().into(),
                buckets: vec![]
            })
        );
        let topic_nodes = ResponseBody::TopicNodes {
            total: 4,
            nodes: vec![],
        };
        assert_eq!(topic_nodes.total(), Some(4));
        assert!(
            Response {
                id: RequestId(vec![1]),
                body: topic_nodes
            }
            .match_request(&RequestBody::TopicQuery {
                topic: topic(),
                buckets: vec![]
            })
        );
        assert_eq!(ResponseBody::Talk { response: vec![] }.total(), None);
    }

    #[test]
    fn rejects_malformed() {
        // Distances above the per-message cap.
        let many = Message::Request(Request {
            id: RequestId(vec![1]),
            body: RequestBody::TopicQuery {
                topic: topic(),
                buckets: vec![1; MAX_TOPIC_DISTANCES + 1],
            },
        })
        .encode();
        assert!(Message::decode(&many).is_err());
        let at_cap = Message::Request(Request {
            id: RequestId(vec![1]),
            body: RequestBody::RegTopic {
                topic: topic(),
                ticket: vec![],
                enr: record().into(),
                buckets: vec![1; MAX_TOPIC_DISTANCES],
            },
        })
        .encode();
        assert!(Message::decode(&at_cap).is_ok());

        // More records than fit a TOPICNODES.
        let many_nodes = Message::Response(Response {
            id: RequestId(vec![1]),
            body: ResponseBody::TopicNodes {
                total: 1,
                nodes: vec![record(); MAX_TOPIC_NODES + 1],
            },
        })
        .encode();
        assert!(Message::decode(&many_nodes).is_err());

        // Total does not fit the Go uint8.
        assert!(Message::decode(&hex::decode("08c904820100820707820101").unwrap()).is_err());

        // Trailing and truncated bodies.
        let good = vector("# topicquery.req-id = 0x00000003");
        let mut trailing = good.clone();
        trailing.push(0);
        assert!(Message::decode(&trailing).is_err());
        assert!(Message::decode(&good[..good.len() - 1]).is_err());

        // The topic must be exactly 32 bytes.
        assert!(Message::decode(&hex::decode("09c5059f0102c0").unwrap()).is_err());

        // The record and the ticket swapped.
        let mut list = Vec::new();
        RequestId(vec![1]).as_bytes().encode(&mut list);
        topic().encode(&mut list);
        record().encode(&mut list);
        [9u8, 9].as_slice().encode(&mut list);
        vec![250u64, 251].encode(&mut list);
        assert!(Message::decode(&wrap_list(vec![7], &list)).is_err());
    }
}
