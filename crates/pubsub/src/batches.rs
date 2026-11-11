use alloy_json_rpc::{Id, RequestPacket, Response, SerializedRequest};
use alloy_primitives::map::{HashMap, HashSet};
use alloy_transport::{TransportErrorKind, TransportResult};
use serde_json::value::RawValue;
use tokio::sync::oneshot;

const MAX_BATCH_ITEMS: usize = 64;
const MAX_PENDING_BATCHES: usize = 256;

type BatchReply = oneshot::Sender<TransportResult<Vec<Response>>>;

/// Native packets share the stream with independent calls. The client reserves
/// all IDs from the same sequence. Any member can identify an unordered reply.
#[derive(Debug, Default)]
pub(crate) struct PendingBatches {
    groups: HashMap<u64, (Vec<Id>, BatchReply)>,
    member_groups: HashMap<Id, u64>,
}

impl PendingBatches {
    pub(crate) fn contains(&self, id: &Id) -> bool {
        self.member_groups.contains_key(id)
    }

    pub(crate) fn prepare(
        &mut self,
        group: u64,
        requests: Vec<SerializedRequest>,
        reply: BatchReply,
        mut id_in_use: impl FnMut(&Id) -> bool,
    ) -> Option<Box<RawValue>> {
        if reply.is_closed() {
            return None;
        }
        if requests.len() > MAX_BATCH_ITEMS || self.groups.len() >= MAX_PENDING_BATCHES {
            let _ = reply.send(Err(TransportErrorKind::custom_str("native batch limit exceeded")));
            return None;
        }
        let mut ids: HashSet<&Id> = HashSet::default();
        if requests.is_empty()
            || requests.iter().any(|request| {
                let id = request.id();
                request.is_subscription()
                    || *id == Id::None
                    || self.contains(id)
                    || id_in_use(id)
                    || !ids.insert(id)
            })
        {
            let _ = reply.send(Err(TransportErrorKind::custom_str(
                "invalid or duplicate native batch request ID",
            )));
            return None;
        }
        let ids = requests.iter().map(|req| req.id().clone()).collect::<Vec<_>>();
        match RequestPacket::Batch(requests).serialize() {
            Ok(packet) => {
                self.member_groups.extend(ids.iter().cloned().map(|id| (id, group)));
                self.groups.insert(group, (ids, reply));
                Some(packet)
            }
            Err(error) => {
                let _ = reply.send(Err(TransportErrorKind::custom(error)));
                None
            }
        }
    }

    pub(crate) fn cancel(&mut self, group: &u64) {
        self.remove(group);
    }

    pub(crate) fn finish(&mut self, replies: Vec<Response>) {
        let Some(group) =
            replies.iter().find_map(|reply| self.member_groups.get(&reply.id)).copied()
        else {
            return;
        };
        let (ids, waiter) = self.remove(&group).expect("member has a pending group");
        let mut expected: HashSet<_> = ids.into_iter().collect();
        let valid = replies.iter().all(|reply| expected.remove(&reply.id)) && expected.is_empty();
        let result = if valid {
            Ok(replies)
        } else {
            Err(TransportErrorKind::custom_str(
                "native batch response has missing, unknown, or duplicate IDs",
            ))
        };
        let _ = waiter.send(result);
    }

    pub(crate) fn reject_single(&mut self, id: &Id) -> bool {
        let Some(group) = self.member_groups.get(id).copied() else {
            return false;
        };
        let (_, waiter) = self.remove(&group).expect("member has a pending group");
        let _ = waiter
            .send(Err(TransportErrorKind::custom_str("expected a native batch response array")));
        true
    }

    pub(crate) fn disconnected(&mut self) {
        self.member_groups.clear();
        for (_, (_, waiter)) in self.groups.drain() {
            let _ = waiter.send(Err(TransportErrorKind::backend_gone()));
        }
    }

    fn remove(&mut self, group: &u64) -> Option<(Vec<Id>, BatchReply)> {
        let (ids, waiter) = self.groups.remove(group)?;
        for id in &ids {
            self.member_groups.remove(id);
        }
        Some((ids, waiter))
    }
}
