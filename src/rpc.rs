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
        enr: Enr<CombinedKey>,
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

/// Decode-time bounds on the variable-length fields of the topic discovery messages.
#[cfg(feature = "topic-discovery")]
const MAX_TOPIC_DISTANCES: usize = 256;
#[cfg(feature = "topic-discovery")]
const MAX_TOPIC_NODES: usize = 16;

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
                enr.encode(&mut list);
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
            ResponseBody::Nodes { .. } => {
                matches!(req, RequestBody::FindNode { .. })
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

/// Decodes an RLP list of records.
fn decode_enr_list(payload: &mut &[u8]) -> Result<Vec<Enr<CombinedKey>>, DecoderError> {
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
    while !items.is_empty() {
        nodes.push(decode_enr(&mut items)?);
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
                let nodes = decode_enr_list(payload)?;
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
                let enr = decode_enr(payload)?;
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
                let wait_time = u64::decode(payload)?;
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
                let nodes = decode_enr_list(payload)?;
                if nodes.len() > MAX_TOPIC_NODES {
                    return Err(DecoderError::Custom("Too many nodes in TOPICNODES"));
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
    //! Byte vectors produced by the Go implementation: each message below was built with
    //! the same field values in `v5wire`, encoded with `rlp.EncodeToBytes` and prefixed
    //! with its message type byte. The record is signed with the secp256k1 key
    //! eef77acb6c6a6eebc5b363a475ac583ec7eccdb42b6481424c60f59aa326547f (ip 127.0.0.1,
    //! udp 30303, seq 1); the signature is deterministic (RFC 6979), so the bytes are
    //! reproducible from the Go code.
    use super::*;

    const ENR: &str = "f884b8404ee3d46c3bfc895484b582e2a5892c2eccfd61c6259bcb57e12bcfbd3544b46b6922188402582c89eae528dd0ea691ebfb2c13cc974b357ba721ea9b84a285a080826964827634826970847f00000189736563703235366b31a10313d14211e0287b2361a1615890a9b5212080546d0a257ae4cff96cf534992cb98375647082765f";

    fn record() -> Enr<CombinedKey> {
        Enr::decode(&mut &hex::decode(ENR).unwrap()[..]).unwrap()
    }

    fn topic() -> [u8; 32] {
        let mut topic = [0u8; 32];
        topic[..5].copy_from_slice(&[1, 2, 3, 4, 5]);
        topic
    }

    const TOPIC_HEX: &str = "a00102030405000000000000000000000000000000000000000000000000000000";

    fn check(message: Message, expected_hex: &str) {
        let expected = hex::decode(expected_hex).unwrap();
        assert_eq!(hex::encode(message.clone().encode()), expected_hex);
        assert_eq!(Message::decode(&expected).unwrap(), message);
    }

    #[test]
    fn regtopic_vector() {
        check(
            Message::Request(Request {
                id: RequestId(vec![1, 2, 3]),
                body: RequestBody::RegTopic {
                    topic: topic(),
                    ticket: vec![9, 9],
                    enr: record(),
                    buckets: vec![250, 251],
                },
            }),
            &format!("07f8b383010203{TOPIC_HEX}820909{ENR}c481fa81fb"),
        );
    }

    #[test]
    fn regtopic_vector_no_ticket() {
        check(
            Message::Request(Request {
                id: RequestId(vec![1]),
                body: RequestBody::RegTopic {
                    topic: topic(),
                    ticket: vec![],
                    enr: record(),
                    buckets: vec![],
                },
            }),
            &format!("07f8aa01{TOPIC_HEX}80{ENR}c0"),
        );
    }

    #[test]
    fn regconfirmation_vectors() {
        check(
            Message::Response(Response {
                id: RequestId(vec![4]),
                body: ResponseBody::RegConfirmation {
                    total: 2,
                    ticket: vec![7, 7],
                    wait_time: 1500,
                },
            }),
            "08c804028207078205dc",
        );
        // Admitted: empty ticket, wait time carries the ad lifetime (15 min).
        check(
            Message::Response(Response {
                id: RequestId(vec![4]),
                body: ResponseBody::RegConfirmation {
                    total: 1,
                    ticket: vec![],
                    wait_time: 900_000,
                },
            }),
            "08c7040180830dbba0",
        );
    }

    #[test]
    fn topicquery_vectors() {
        check(
            Message::Request(Request {
                id: RequestId(vec![5]),
                body: RequestBody::TopicQuery {
                    topic: topic(),
                    buckets: vec![248],
                },
            }),
            &format!("09e505{TOPIC_HEX}c281f8"),
        );
        check(
            Message::Request(Request {
                id: RequestId(vec![5]),
                body: RequestBody::TopicQuery {
                    topic: topic(),
                    buckets: vec![],
                },
            }),
            &format!("09e305{TOPIC_HEX}c0"),
        );
    }

    #[test]
    fn topicnodes_vectors() {
        check(
            Message::Response(Response {
                id: RequestId(vec![6]),
                body: ResponseBody::TopicNodes {
                    total: 3,
                    nodes: vec![record()],
                },
            }),
            &format!("0af88a0603f886{ENR}"),
        );
        check(
            Message::Response(Response {
                id: RequestId(vec![6]),
                body: ResponseBody::TopicNodes {
                    total: 1,
                    nodes: vec![],
                },
            }),
            "0ac30601c0",
        );
    }

    #[test]
    fn nodes_vector_unchanged() {
        // The shared record-list codec still produces the plain NODES encoding.
        check(
            Message::Response(Response {
                id: RequestId(vec![6]),
                body: ResponseBody::Nodes {
                    total: 3,
                    nodes: vec![record()],
                },
            }),
            &format!("04f88a0603f886{ENR}"),
        );
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
                enr: record(),
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
                enr: record(),
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
        let good = hex::decode(format!("09e505{TOPIC_HEX}c281f8")).unwrap();
        let mut trailing = good.clone();
        trailing.push(0);
        assert!(Message::decode(&trailing).is_err());
        assert!(Message::decode(&good[..good.len() - 1]).is_err());

        // The topic must be exactly 32 bytes.
        assert!(Message::decode(&hex::decode("09c5059f0102c0").unwrap()).is_err());

        // A record where the ticket is expected (the field order of the spec draft)
        // does not decode.
        let spec_order =
            hex::decode(format!("07f8b083010203{TOPIC_HEX}{ENR}820909c481fa81fb")).unwrap();
        assert!(Message::decode(&spec_order).is_err());
    }
}
