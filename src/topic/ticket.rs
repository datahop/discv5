use super::{
    TopicId,
    params::{DELTA, EPSILON, USED_TICKET_LIMIT, USED_TICKET_TTL},
};
use enr::NodeId;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use std::{
    collections::{HashSet, VecDeque},
    fmt,
    time::{Duration, Instant},
};

type HmacSha256 = Hmac<Sha256>;

const TICKET_MAC_SIZE: usize = 32;
// key id (u16) | topic (32) | node id (32) | twait (u64 ns) | tmod (u64 ns) | tinit (u64 ns)
const TICKET_BODY_SIZE: usize = 2 + 32 + 32 + 8 + 8 + 8;
/// Size of a sealed ticket on the wire. The layout matches the Go implementation:
/// a 32-byte HMAC-SHA256 followed by the big-endian body.
pub const TICKET_SIZE: usize = TICKET_MAC_SIZE + TICKET_BODY_SIZE;

const TICKET_KEY_LIFETIME: Duration = Duration::from_secs(6 * 3600);
const TICKET_REKEY_INTERVAL: usize = 100_000;

/// The content of a ticket issued by this registrar. Times are registrar-local: a
/// ticket is opaque to the advertiser and only ever comes back to the node that
/// sealed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ticket {
    pub key_id: u16,
    pub topic: TopicId,
    /// The advertiser the ticket was issued to.
    pub node_id: NodeId,
    /// Wait time returned in the last REGCONFIRMATION (`twait`).
    pub wait_time_issued: Duration,
    /// Last request in this waiting session (`tmod`).
    pub last_used: Instant,
    /// Creation time of the first ticket in this waiting session (`tinit`).
    pub first_issued: Instant,
}

/// Where a time falls relative to a ticket's window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TicketTiming {
    Early,
    OnTime,
    Late,
    Expired,
}

impl Ticket {
    /// The earliest time the ticket may be used, `tmod + twait`.
    pub fn window_start(&self) -> Instant {
        self.last_used + self.wait_time_issued
    }

    pub fn timing_at(&self, now: Instant) -> TicketTiming {
        let start = self.window_start();
        if now < start {
            TicketTiming::Early
        } else if now >= start + DELTA + EPSILON {
            TicketTiming::Expired
        } else if now >= start + DELTA {
            TicketTiming::Late
        } else {
            TicketTiming::OnTime
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TicketError {
    /// The encoded ticket has the wrong size.
    BadSize,
    /// The MAC does not verify.
    BadMac,
    /// The key that sealed the ticket is no longer held.
    NoKey,
    /// The ticket was issued for another topic.
    WrongTopic,
    /// The body cannot be decoded.
    Malformed,
}

impl fmt::Display for TicketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            TicketError::BadSize => "encoded ticket has wrong size",
            TicketError::BadMac => "invalid ticket MAC",
            TicketError::NoKey => "unknown ticket MAC key id",
            TicketError::WrongTopic => "ticket used with wrong topic",
            TicketError::Malformed => "malformed ticket body",
        };
        f.write_str(msg)
    }
}

impl std::error::Error for TicketError {}

struct TicketKey {
    key_id: u16,
    secret: [u8; 32],
    uses: usize,
    last_used: Instant,
}

impl TicketKey {
    fn mac(&self, body: &[u8]) -> [u8; TICKET_MAC_SIZE] {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("hmac accepts any key size");
        mac.update(body);
        mac.finalize().into_bytes().into()
    }

    fn verify(&self, body: &[u8], tag: &[u8]) -> bool {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("hmac accepts any key size");
        mac.update(body);
        mac.verify_slice(tag).is_ok()
    }
}

/// Seals and authenticates tickets with rotating HMAC keys.
pub struct TicketSealer {
    keys: Vec<TicketKey>,
    id_counter: u16,
    /// Instants are encoded as nanoseconds since this point.
    epoch: Instant,
    rekey_interval: usize,
}

impl TicketSealer {
    /// `epoch` must not be later than any time subsequently passed to `pack`.
    pub fn new(epoch: Instant) -> Self {
        TicketSealer {
            keys: Vec::new(),
            id_counter: 0,
            epoch,
            rekey_interval: TICKET_REKEY_INTERVAL,
        }
    }

    /// Authenticates and decodes a non-empty ticket presented for `topic`. Neither the
    /// timing nor the node the ticket was issued to is checked.
    pub fn unpack(
        &mut self,
        topic: &TopicId,
        input: &[u8],
        now: Instant,
    ) -> Result<Ticket, TicketError> {
        if input.len() != TICKET_SIZE {
            return Err(TicketError::BadSize);
        }
        let (tag, body) = input.split_at(TICKET_MAC_SIZE);
        let key_id = u16::from_be_bytes([body[0], body[1]]);

        self.gc_keys(now);
        let key = self
            .keys
            .iter()
            .find(|k| k.key_id == key_id)
            .ok_or(TicketError::NoKey)?;
        if !key.verify(body, tag) {
            return Err(TicketError::BadMac);
        }
        let ticket = self.decode_body(body)?;
        if ticket.topic != *topic {
            return Err(TicketError::WrongTopic);
        }
        Ok(ticket)
    }

    /// Encodes and seals a ticket, assigning the key id of the key used.
    pub fn pack(&mut self, ticket: &mut Ticket, now: Instant) -> Vec<u8> {
        self.gc_keys(now);
        let epoch = self.epoch;
        let key = self.valid_key(now);
        ticket.key_id = key.key_id;

        let body = encode_body(ticket, epoch);
        let tag = key.mac(&body);
        let mut out = Vec::with_capacity(TICKET_SIZE);
        out.extend_from_slice(&tag);
        out.extend_from_slice(&body);
        out
    }

    fn decode_body(&self, body: &[u8]) -> Result<Ticket, TicketError> {
        let u64_at = |at: usize| u64::from_be_bytes(body[at..at + 8].try_into().expect("8 bytes"));
        let since_epoch = |ns: u64| {
            self.epoch
                .checked_add(Duration::from_nanos(ns))
                .ok_or(TicketError::Malformed)
        };
        let ticket = Ticket {
            key_id: u16::from_be_bytes([body[0], body[1]]),
            topic: body[2..34].try_into().expect("32 bytes"),
            node_id: NodeId::new(&body[34..66].try_into().expect("32 bytes")),
            wait_time_issued: Duration::from_nanos(u64_at(66)),
            last_used: since_epoch(u64_at(74))?,
            first_issued: since_epoch(u64_at(82))?,
        };
        // The window must be representable for `window_start` and `timing_at`.
        ticket
            .last_used
            .checked_add(ticket.wait_time_issued + DELTA + EPSILON)
            .ok_or(TicketError::Malformed)?;
        Ok(ticket)
    }

    /// Drops keys that have not been used for longer than the key lifetime.
    fn gc_keys(&mut self, now: Instant) {
        let Some(deadline) = now.checked_sub(TICKET_KEY_LIFETIME) else {
            return;
        };
        let cut = self
            .keys
            .iter()
            .take_while(|k| k.last_used <= deadline)
            .count();
        if cut > 0 {
            self.keys.drain(..cut);
        }
    }

    /// Returns the current key, generating a new one when the latest key has reached
    /// its use limit.
    fn valid_key(&mut self, now: Instant) -> &TicketKey {
        let reuse = self
            .keys
            .last()
            .is_some_and(|k| k.uses < self.rekey_interval);
        if !reuse {
            let key_id = self.id_counter;
            self.id_counter = self.id_counter.wrapping_add(1);
            self.keys.push(TicketKey {
                key_id,
                secret: rand::random(),
                uses: 0,
                last_used: now,
            });
        }
        let key = self.keys.last_mut().expect("at least one key");
        key.uses += 1;
        key.last_used = now;
        key
    }
}

fn encode_body(ticket: &Ticket, epoch: Instant) -> [u8; TICKET_BODY_SIZE] {
    let ns = |t: Instant| t.saturating_duration_since(epoch).as_nanos() as u64;
    let mut body = [0u8; TICKET_BODY_SIZE];
    body[0..2].copy_from_slice(&ticket.key_id.to_be_bytes());
    body[2..34].copy_from_slice(&ticket.topic);
    body[34..66].copy_from_slice(&ticket.node_id.raw());
    body[66..74].copy_from_slice(&(ticket.wait_time_issued.as_nanos() as u64).to_be_bytes());
    body[74..82].copy_from_slice(&ns(ticket.last_used).to_be_bytes());
    body[82..90].copy_from_slice(&ns(ticket.first_issued).to_be_bytes());
    body
}

/// Remembers credited tickets by their MAC, so that a ticket is credited once.
#[derive(Default)]
pub struct UsedTickets {
    set: HashSet<[u8; TICKET_MAC_SIZE]>,
    /// In insertion order, with the time each entry is forgotten.
    queue: VecDeque<([u8; TICKET_MAC_SIZE], Instant)>,
}

impl UsedTickets {
    /// Records a ticket as credited at `now` and reports whether it already was. The
    /// ticket must be one that `TicketSealer::unpack` accepted.
    pub fn use_ticket(&mut self, enc: &[u8], now: Instant) -> bool {
        while self.queue.front().is_some_and(|(_, exp)| *exp <= now) {
            self.drop_oldest();
        }
        let mac: [u8; TICKET_MAC_SIZE] = enc[..TICKET_MAC_SIZE].try_into().expect("sealed ticket");
        if self.set.contains(&mac) {
            return true;
        }
        if self.queue.len() >= USED_TICKET_LIMIT {
            self.drop_oldest();
        }
        self.set.insert(mac);
        self.queue.push_back((mac, now + USED_TICKET_TTL));
        false
    }

    /// The number of remembered tickets.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn drop_oldest(&mut self) {
        if let Some((mac, _)) = self.queue.pop_front() {
            self.set.remove(&mac);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOPIC1: TopicId = {
        let mut t = [0u8; 32];
        t[0] = 1;
        t[1] = 2;
        t[2] = 3;
        t
    };
    const TOPIC2: TopicId = [8u8; 32];

    fn node() -> NodeId {
        let mut id = [0u8; 32];
        id[..3].copy_from_slice(&[1, 2, 3]);
        NodeId::new(&id)
    }

    fn ticket(topic: TopicId, wait: Duration, now: Instant) -> Ticket {
        Ticket {
            key_id: 0,
            topic,
            node_id: node(),
            wait_time_issued: wait,
            last_used: now,
            first_issued: now,
        }
    }

    #[test]
    fn seal_unseal_round_trip() {
        let t0 = Instant::now();
        let mut sealer = TicketSealer::new(t0);
        let mut t = ticket(TOPIC1, Duration::from_secs(60), t0);
        let mut enc = sealer.pack(&mut t, t0);
        assert_eq!(enc.len(), TICKET_SIZE);
        assert_eq!(TICKET_SIZE, 122);

        let now = t0 + Duration::from_secs(60);
        assert_eq!(sealer.unpack(&TOPIC1, &enc, now), Ok(t));

        // The node is covered by the MAC.
        enc[TICKET_MAC_SIZE + 2 + 32] ^= 0xff;
        assert_eq!(sealer.unpack(&TOPIC1, &enc, now), Err(TicketError::BadMac));
    }

    #[test]
    fn wrong_topic_and_early_ticket() {
        let t0 = Instant::now();
        let mut sealer = TicketSealer::new(t0);
        let mut t = ticket(TOPIC1, Duration::from_secs(300), t0);
        let enc = sealer.pack(&mut t, t0);

        assert_eq!(
            sealer.unpack(&TOPIC2, &enc, t0),
            Err(TicketError::WrongTopic)
        );
        // Unpacking does not check the timing.
        let got = sealer.unpack(&TOPIC1, &enc, t0).unwrap();
        assert_eq!(got.timing_at(t0), TicketTiming::Early);
    }

    #[test]
    fn ticket_timing() {
        let issued = Instant::now();
        let wait = Duration::from_secs(60);
        let start = issued + wait;
        let t = ticket(TOPIC1, wait, issued);
        assert_eq!(t.window_start(), start);

        let ns = Duration::from_nanos(1);
        let cases = [
            (start - ns, TicketTiming::Early),
            (start, TicketTiming::OnTime),
            (start + DELTA - ns, TicketTiming::OnTime),
            (start + DELTA, TicketTiming::Late),
            (start + DELTA + EPSILON - ns, TicketTiming::Late),
            (start + DELTA + EPSILON, TicketTiming::Expired),
        ];
        for (at, want) in cases {
            assert_eq!(t.timing_at(at), want, "at start{:+?}", at - issued);
        }
    }

    #[test]
    fn rejects_tampered_and_malformed() {
        let t0 = Instant::now();
        let mut sealer = TicketSealer::new(t0);
        let mut t = ticket(TOPIC1, Duration::from_secs(10), t0);
        let enc = sealer.pack(&mut t, t0);
        let now = t0 + Duration::from_secs(10);

        let mut bad_mac = enc.clone();
        bad_mac[3] ^= 0xff;
        assert_eq!(
            sealer.unpack(&TOPIC1, &bad_mac, now),
            Err(TicketError::BadMac)
        );

        // Shortening the issued wait time to skip the wait breaks the MAC.
        let mut bad_body = enc.clone();
        bad_body[TICKET_MAC_SIZE + 72] ^= 0x01;
        assert_eq!(
            sealer.unpack(&TOPIC1, &bad_body, now),
            Err(TicketError::BadMac)
        );

        let mut bad_key = enc.clone();
        bad_key[TICKET_MAC_SIZE + 1] ^= 0x01;
        assert_eq!(
            sealer.unpack(&TOPIC1, &bad_key, now),
            Err(TicketError::NoKey)
        );

        assert_eq!(
            sealer.unpack(&TOPIC1, &enc[..enc.len() - 1], now),
            Err(TicketError::BadSize)
        );
        assert_eq!(sealer.unpack(&TOPIC1, &[1], now), Err(TicketError::BadSize));
        // A ticket sealed by another node fails to authenticate.
        let mut other = TicketSealer::new(t0);
        let enc2 = other.pack(&mut ticket(TOPIC1, Duration::from_secs(10), t0), t0);
        assert_eq!(sealer.unpack(&TOPIC1, &enc2, now), Err(TicketError::BadMac));
    }

    #[test]
    fn key_reuse_and_rotation() {
        let t0 = Instant::now();
        let mut sealer = TicketSealer::new(t0);
        sealer.rekey_interval = 10;
        let wait = TICKET_KEY_LIFETIME - Duration::from_nanos(3);

        let mut encs = Vec::new();
        for _ in 0..25 {
            encs.push(sealer.pack(&mut ticket(TOPIC1, wait, t0), t0));
        }
        // Tickets 1..10, 11..20 and 21..25 use three keys.
        assert_eq!(sealer.keys.len(), 3);

        // All keys are still held one nanosecond before the key lifetime.
        let now = t0 + TICKET_KEY_LIFETIME - Duration::from_nanos(1);
        for enc in &encs {
            sealer.unpack(&TOPIC1, enc, now).unwrap();
        }
        // Once the key lifetime has passed the keys are garbage collected.
        let now = t0 + TICKET_KEY_LIFETIME;
        for enc in &encs {
            assert_eq!(sealer.unpack(&TOPIC1, enc, now), Err(TicketError::NoKey));
        }
        assert!(sealer.keys.is_empty());
    }

    #[test]
    fn used_tickets_ttl_and_limit() {
        let t0 = Instant::now();
        let mac = |i: u32| {
            let mut enc = vec![0u8; TICKET_SIZE];
            enc[..4].copy_from_slice(&i.to_be_bytes());
            enc
        };

        let mut used = UsedTickets::default();
        assert!(!used.use_ticket(&mac(0), t0));
        assert!(used.use_ticket(&mac(0), t0));
        let ns = Duration::from_nanos(1);
        assert!(
            used.use_ticket(&mac(0), t0 + USED_TICKET_TTL - ns),
            "forgotten before its TTL"
        );
        assert!(
            !used.use_ticket(&mac(0), t0 + USED_TICKET_TTL),
            "remembered past its TTL"
        );

        let mut used = UsedTickets::default();
        for i in 0..=USED_TICKET_LIMIT as u32 {
            assert!(
                !used.use_ticket(&mac(i), t0),
                "ticket {i} reported as replay"
            );
        }
        assert_eq!(used.len(), USED_TICKET_LIMIT);
        assert!(
            !used.use_ticket(&mac(0), t0),
            "oldest ticket not dropped past the limit"
        );
    }
}
