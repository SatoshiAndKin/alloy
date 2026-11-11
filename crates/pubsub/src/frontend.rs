use crate::{ix::PubSubInstruction, managers::InFlight, RawSubscription};
use alloy_json_rpc::{RequestPacket, Response, ResponsePacket, SerializedRequest};
use alloy_primitives::B256;
use alloy_transport::{TransportError, TransportErrorKind, TransportFut, TransportResult};
use futures::{future::try_join_all, FutureExt, TryFutureExt};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, debug_span, Instrument};

/// A `PubSubFrontend` is [`Transport`] composed of a channel to a running
/// PubSub service.
///
/// [`Transport`]: alloy_transport::Transport
#[derive(Debug, Clone)]
pub struct PubSubFrontend {
    tx: mpsc::UnboundedSender<PubSubInstruction>,
    /// The number of items to buffer in new subscription channels. Defaults to
    /// 16. See [`tokio::sync::broadcast::channel`] for a description.
    channel_size: Arc<AtomicUsize>,
    wire_batches: Option<Arc<AtomicU64>>,
}

impl PubSubFrontend {
    /// Create a new frontend.
    pub fn new(tx: mpsc::UnboundedSender<PubSubInstruction>) -> Self {
        Self { tx, channel_size: Arc::new(AtomicUsize::new(16)), wire_batches: None }
    }

    pub(crate) fn with_wire_batches(mut self, enabled: bool) -> Self {
        self.wire_batches = enabled.then(|| Arc::new(AtomicU64::new(0)));
        self
    }

    /// Send a native JSON-RPC batch on a transport that explicitly enables it.
    /// Use client-allocated IDs. Subscription and notification requests are not supported.
    /// A disconnect fails the packet so the caller can decide whether to retry it.
    pub async fn send_wire_batch(
        &self,
        requests: Vec<SerializedRequest>,
    ) -> TransportResult<Vec<Response>> {
        let sequence = self.wire_batches.as_ref().ok_or_else(|| {
            TransportErrorKind::custom_str("native batches are disabled on this connection")
        })?;
        let id = sequence.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(PubSubInstruction::WireBatch(id, requests, tx))
            .map_err(|_| TransportErrorKind::backend_gone())?;
        let mut cancellation = BatchCancellation { channel: &self.tx, id: Some(id) };
        let result = rx.await.map_err(|_| TransportErrorKind::backend_gone());
        cancellation.id = None;
        result?
    }

    /// Get the subscription ID for a local ID.
    pub fn get_subscription(
        &self,
        id: B256,
    ) -> impl Future<Output = TransportResult<RawSubscription>> + Send + 'static {
        let backend_tx = self.tx.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            backend_tx
                .send(PubSubInstruction::GetSub(id, tx))
                .map_err(|_| TransportErrorKind::backend_gone())?;
            rx.await
                .map_err(|_| TransportErrorKind::backend_gone())?
                .map_or_else(|| Err(TransportErrorKind::custom_str("subscription not found")), Ok)
        }
    }

    /// Queue an unsubscribe instruction for a local subscription ID.
    ///
    /// A successful return means the instruction was sent to the pubsub
    /// service. It does not wait for the server's `eth_unsubscribe` response.
    /// Dropping a [`RawSubscription`] does not call this automatically. This is
    /// best effort: during reconnection it can race with a replacement
    /// subscription response, so success does not confirm server-side teardown.
    ///
    /// [`RawSubscription`]: crate::RawSubscription
    pub fn unsubscribe(&self, id: B256) -> TransportResult<()> {
        self.tx
            .send(PubSubInstruction::Unsubscribe(id))
            .map_err(|_| TransportErrorKind::backend_gone())
    }

    /// Send a request.
    pub fn send(
        &self,
        req: SerializedRequest,
    ) -> impl Future<Output = TransportResult<Response>> + Send + 'static {
        let tx = self.tx.clone();
        let channel_size = self.channel_size.load(Ordering::Relaxed);
        let method_name = req.method_clone();

        async move {
            debug!("sending request to backend");
            let (in_flight, rx) = InFlight::new(req, channel_size);
            tx.send(PubSubInstruction::Request(in_flight))
                .map_err(|_| TransportErrorKind::backend_gone())?;
            let resp = rx.await.map_err(|_| TransportErrorKind::backend_gone())?;
            if tracing::enabled!(tracing::Level::TRACE) {
                trace!(?resp, "retrieved response");
            } else {
                debug!(resp=?resp.as_ref().map(|_| ()), "retrieved response");
            };
            resp
        }
        .instrument(debug_span!("request", %method_name))
    }

    /// Send a packet of requests, by breaking it up into individual requests.
    ///
    /// Once all responses are received, we return a single response packet.
    pub fn send_packet(&self, req: RequestPacket) -> TransportFut<'static> {
        match req {
            RequestPacket::Single(req) => self.send(req).map_ok(ResponsePacket::Single).boxed(),
            RequestPacket::Batch(reqs) => try_join_all(reqs.into_iter().map(|req| self.send(req)))
                .map_ok(ResponsePacket::Batch)
                .boxed(),
        }
    }

    /// Get the currently configured channel size. This is the number of items
    /// to buffer in new subscription channels. Defaults to 16. See
    /// [`tokio::sync::broadcast`] for a description of relevant
    /// behavior.
    pub fn channel_size(&self) -> usize {
        self.channel_size.load(Ordering::Relaxed)
    }

    /// Set the channel size. This is the number of items to buffer in new
    /// subscription channels. Defaults to 16. See
    /// [`tokio::sync::broadcast`] for a description of relevant
    /// behavior.
    pub fn set_channel_size(&self, channel_size: usize) {
        debug_assert_ne!(channel_size, 0, "channel size must be non-zero");
        self.channel_size.store(channel_size, Ordering::Relaxed);
    }
}

struct BatchCancellation<'a> {
    channel: &'a mpsc::UnboundedSender<PubSubInstruction>,
    id: Option<u64>,
}

impl Drop for BatchCancellation<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let _ = self.channel.send(PubSubInstruction::CancelWireBatch(id));
        }
    }
}

impl tower::Service<RequestPacket> for PubSubFrontend {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result =
            if self.tx.is_closed() { Err(TransportErrorKind::backend_gone()) } else { Ok(()) };
        Poll::Ready(result)
    }

    #[inline]
    fn call(&mut self, req: RequestPacket) -> Self::Future {
        self.send_packet(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_json_rpc::Request;

    #[tokio::test]
    async fn dropping_a_submitted_native_batch_cancels_its_own_group() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let frontend = PubSubFrontend::new(tx).with_wire_batches(true);
        let request = Request::new("eth_call", 42.into(), ()).serialize().unwrap();
        let (group, reply) = {
            let future = frontend.send_wire_batch(vec![request]);
            tokio::pin!(future);
            tokio::select! {
                result = &mut future => panic!("batch cannot complete before reply: {result:?}"),
                instruction = rx.recv() => {
                    let Some(PubSubInstruction::WireBatch(group, requests, reply)) = instruction else {
                        panic!("expected native batch");
                    };
                    assert_eq!(requests[0].id(), &42.into());
                    (group, reply)
                }
            }
        };
        assert!(reply.is_closed());
        assert!(matches!(rx.try_recv(), Ok(PubSubInstruction::CancelWireBatch(id)) if id == group));
    }
}
