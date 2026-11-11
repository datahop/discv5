pub use crate::node_info::NodeContact;
use crate::{
    packet::Packet,
    rpc::{Request, RequestBody},
};
#[cfg(feature = "topic-discovery")]
use crate::{
    rpc::ResponseBody,
    topic::params::{QUERY_RESP_LIMIT, REG_RESP_LIMIT, RESP_TIMEOUT},
};
use std::time::Duration;

use super::HandlerReqId;

/// Counts the response packets of a REGTOPIC or TOPICQUERY to tell when the request is
/// over, as in the spec's "Response Splitting".
#[cfg(feature = "topic-discovery")]
#[derive(Debug, Default, Clone)]
pub(crate) struct TopicRespCount {
    /// The `total` of the first packet of a kind that answers the request.
    total: Option<u64>,
    /// Packets of a kind that answers the request.
    received: u64,
    /// Packets of any kind.
    packets: u64,
    confirmed: bool,
}

#[cfg(feature = "topic-discovery")]
impl TopicRespCount {
    /// Counts one packet. Returns true when no further packet is awaited: the capped
    /// `total` has arrived, with the REGCONFIRMATION for a REGTOPIC, or the packet cap
    /// is reached.
    pub(crate) fn count(&mut self, request: &RequestBody, response: &ResponseBody) -> bool {
        let (limit, regtopic) = match request {
            RequestBody::RegTopic { .. } => (REG_RESP_LIMIT as u64, true),
            _ => (QUERY_RESP_LIMIT as u64, false),
        };
        self.packets += 1;
        let answers = match response {
            ResponseBody::Nodes { .. } => true,
            ResponseBody::RegConfirmation { .. } => regtopic,
            ResponseBody::TopicNodes { .. } => !regtopic,
            _ => false,
        };
        if answers {
            self.total.get_or_insert(response.total().unwrap_or(1));
            self.received += 1;
            self.confirmed |= matches!(response, ResponseBody::RegConfirmation { .. });
        }
        let complete = self
            .total
            .is_some_and(|total| self.received >= total.min(limit));
        (complete && (self.confirmed || !regtopic)) || self.packets >= limit
    }
}

/// A request to a node that we are waiting for a response.
#[derive(Debug)]
pub(super) struct RequestCall {
    contact: NodeContact,
    /// The raw discv5 packet sent.
    packet: Packet,
    /// Request id
    request_id: HandlerReqId,
    /// The message body. Required if need to re-encrypt and re-send.
    request: RequestBody,
    /// Handshakes attempted.
    handshake_sent: bool,
    /// The number of times this request has been re-sent.
    retries: u8,
    /// If we receive a Nodes Response with a total greater than 1. This keeps track of the
    /// remaining responses expected.
    remaining_responses: Option<u64>,
    /// Signifies if we are initiating the session with a random packet. This is only used to
    /// determine the connection direction of the session.
    initiating_session: bool,
    /// The packets received so far for a topic request.
    #[cfg(feature = "topic-discovery")]
    topic_responses: Option<TopicRespCount>,
}

impl RequestCall {
    pub fn new(
        contact: NodeContact,
        packet: Packet,
        request_id: HandlerReqId,
        request: RequestBody,
        initiating_session: bool,
    ) -> Self {
        RequestCall {
            contact,
            packet,
            request_id,
            handshake_sent: false,
            retries: 1,
            remaining_responses: None,
            initiating_session,
            #[cfg(feature = "topic-discovery")]
            topic_responses: request.is_topic_request().then(TopicRespCount::default),
            request,
        }
    }

    /// Returns the contact associated with this call.
    pub fn contact(&self) -> &NodeContact {
        &self.contact
    }

    /// Returns the id associated with this call.
    pub fn id(&self) -> &HandlerReqId {
        &self.request_id
    }

    /// Returns the associated request for this call.
    pub fn body(&self) -> &RequestBody {
        &self.request
    }

    /// Returns the packet associated with this call.
    pub fn packet(&self) -> &Packet {
        &self.packet
    }

    pub fn encode(&self) -> Vec<u8> {
        match &self.request_id {
            HandlerReqId::Internal(id) | HandlerReqId::External(id) => {
                let request = Request {
                    id: id.clone(),
                    body: self.request.clone(),
                };
                request.encode()
            }
        }
    }

    /// The number of times this request has been resent.
    pub fn retries(&self) -> u8 {
        self.retries
    }

    /// Increments the number of retries for this call
    pub fn increment_retries(&mut self) {
        self.retries += 1;
    }

    /// Returns whether the handshake has been sent for this call or not.
    pub fn handshake_sent(&self) -> bool {
        self.handshake_sent
    }

    /// Indicates the handshake has been sent.
    pub fn set_handshake_sent(&mut self) {
        self.handshake_sent = true;
    }

    /// Indicates a session has been initiated for this call.
    pub fn set_initiating_session(&mut self, state: bool) {
        self.initiating_session = state;
    }

    /// Returns whether a session is being initiated for this call.
    pub fn initiating_session(&self) -> bool {
        self.initiating_session
    }

    /// Updates the underlying packet for the call.
    pub fn update_packet(&mut self, packet: Packet) {
        self.packet = packet;
    }

    /// The response timeout when it is not the configured one: `RESP_TIMEOUT` per
    /// packet for topic requests.
    pub fn response_timeout(&self) -> Option<Duration> {
        #[cfg(feature = "topic-discovery")]
        if self.topic_responses.is_some() {
            return Some(RESP_TIMEOUT);
        }
        None
    }

    /// REGTOPIC and TOPICQUERY are never resent.
    #[cfg(feature = "topic-discovery")]
    pub fn is_topic_request(&self) -> bool {
        self.topic_responses.is_some()
    }

    /// Counts a response packet of a topic request. `None` for other requests, else
    /// whether the request is over.
    #[cfg(feature = "topic-discovery")]
    pub fn count_topic_response(&mut self, response: &ResponseBody) -> Option<bool> {
        let count = self.topic_responses.as_mut()?;
        Some(count.count(&self.request, response))
    }

    /// Gets a mutable reference to the remaining repsonses.
    pub fn remaining_responses_mut(&mut self) -> &mut Option<u64> {
        &mut self.remaining_responses
    }
}
