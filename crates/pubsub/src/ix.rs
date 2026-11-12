use crate::{managers::InFlight, RawSubscription};
use alloy_json_rpc::{Response, SerializedRequest};
use alloy_primitives::B256;
use alloy_transport::TransportResult;
use std::{fmt, time::Duration};
use tokio::sync::oneshot;

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
pub(crate) use std::time::Instant;
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
pub(crate) use wasmtimer::std::Instant;

pub(crate) type DispatchObserver = Box<dyn FnOnce(Duration, Duration) + Send>;

/// Optional, caller-sampled timing for dispatching a native batch.
pub struct WireBatchObservation {
    pub(crate) queued_at: Instant,
    pub(crate) observe: DispatchObserver,
}

impl fmt::Debug for WireBatchObservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WireBatchObservation").finish_non_exhaustive()
    }
}

/// Instructions for the pubsub service.
pub enum PubSubInstruction {
    /// Send a request.
    Request(InFlight),
    /// Send one native JSON-RPC batch and deliver its complete reply array.
    WireBatch(
        u64,
        Vec<SerializedRequest>,
        oneshot::Sender<TransportResult<Vec<Response>>>,
        Option<WireBatchObservation>,
    ),
    /// Forget an abandoned native batch.
    CancelWireBatch(u64),
    /// Get the subscription ID for a local ID.
    GetSub(B256, oneshot::Sender<Option<RawSubscription>>),
    /// Unsubscribe from a subscription.
    Unsubscribe(B256),
}

impl fmt::Debug for PubSubInstruction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(arg0) => f.debug_tuple("Request").field(arg0).finish(),
            Self::WireBatch(_, requests, _, _) => {
                f.debug_tuple("WireBatch").field(&requests.len()).finish()
            }
            Self::CancelWireBatch(id) => f.debug_tuple("CancelWireBatch").field(id).finish(),
            Self::GetSub(arg0, _) => f.debug_tuple("GetSub").field(arg0).finish(),
            Self::Unsubscribe(arg0) => f.debug_tuple("Unsubscribe").field(arg0).finish(),
        }
    }
}
