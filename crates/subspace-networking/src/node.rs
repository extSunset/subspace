use crate::protocols::request_response::handlers::generic_request_handler::GenericRequest;
use crate::protocols::request_response::request_response_factory;
pub use crate::shared::NewPeerInfo;
use crate::shared::{Command, CreatedSubscription, Shared};
use crate::utils::multihash::Multihash;
use crate::utils::{HandlerFn, ResizableSemaphorePermit};
use bytes::Bytes;
use event_listener_primitives::HandlerId;
use futures::channel::mpsc::SendError;
use futures::channel::{mpsc, oneshot};
use futures::{SinkExt, Stream, StreamExt};
use libp2p::gossipsub::{Sha256Topic, SubscriptionError};
use libp2p::kad::PeerRecord;
use libp2p::{Multiaddr, PeerId};
use parity_scale_codec::Decode;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;
use tracing::{debug, error, trace};

/// Topic subscription, will unsubscribe when last instance is dropped for a particular topic.
#[derive(Debug)]
#[pin_project::pin_project(PinnedDrop)]
pub struct TopicSubscription {
    topic: Option<Sha256Topic>,
    subscription_id: usize,
    command_sender: Option<mpsc::Sender<Command>>,
    #[pin]
    receiver: mpsc::UnboundedReceiver<Bytes>,
    _permit: ResizableSemaphorePermit,
}

impl Stream for TopicSubscription {
    type Item = Bytes;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().receiver.poll_next(cx)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.receiver.size_hint()
    }
}

#[pin_project::pinned_drop]
impl PinnedDrop for TopicSubscription {
    fn drop(mut self: std::pin::Pin<&mut Self>) {
        let topic = self
            .topic
            .take()
            .expect("Always specified on creation and only removed on drop; qed");
        let subscription_id = self.subscription_id;
        let mut command_sender = self
            .command_sender
            .take()
            .expect("Always specified on creation and only removed on drop; qed");

        tokio::spawn(async move {
            // Doesn't matter if node runner is already dropped.
            let _ = command_sender
                .send(Command::Unsubscribe {
                    topic,
                    subscription_id,
                })
                .await;
        });
    }
}

#[derive(Debug, Error)]
pub enum GetValueError {
    /// Failed to send command to the node runner
    #[error("Failed to send command to the node runner: {0}")]
    SendCommand(#[from] SendError),
    /// Node runner was dropped
    #[error("Node runner was dropped")]
    NodeRunnerDropped,
}

impl From<oneshot::Canceled> for GetValueError {
    #[inline]
    fn from(oneshot::Canceled: oneshot::Canceled) -> Self {
        Self::NodeRunnerDropped
    }
}

#[derive(Debug, Error)]
pub enum PutValueError {
    /// Failed to send command to the node runner
    #[error("Failed to send command to the node runner: {0}")]
    SendCommand(#[from] SendError),
    /// Node runner was dropped
    #[error("Node runner was dropped")]
    NodeRunnerDropped,
}

impl From<oneshot::Canceled> for PutValueError {
    #[inline]
    fn from(oneshot::Canceled: oneshot::Canceled) -> Self {
        Self::NodeRunnerDropped
    }
}

/// Defines errors for `get-closest-peers` operation.
#[derive(Debug, Error)]
pub enum GetClosestPeersError {
    /// Failed to send command to the node runner
    #[error("Failed to send command to the node runner: {0}")]
    SendCommand(#[from] SendError),
    /// Node runner was dropped
    #[error("Node runner was dropped")]
    NodeRunnerDropped,
}

impl From<oneshot::Canceled> for GetClosestPeersError {
    #[inline]
    fn from(oneshot::Canceled: oneshot::Canceled) -> Self {
        Self::NodeRunnerDropped
    }
}

/// Defines errors for `subscribe` operation.
#[derive(Debug, Error)]
pub enum SubscribeError {
    /// Failed to send command to the node runner
    #[error("Failed to send command to the node runner: {0}")]
    SendCommand(#[from] SendError),
    /// Node runner was dropped
    #[error("Node runner was dropped")]
    NodeRunnerDropped,
    /// Failed to create subscription.
    #[error("Failed to create subscription: {0}")]
    Subscription(#[from] SubscriptionError),
}

impl From<oneshot::Canceled> for SubscribeError {
    #[inline]
    fn from(oneshot::Canceled: oneshot::Canceled) -> Self {
        Self::NodeRunnerDropped
    }
}

#[derive(Debug, Error)]
pub enum PublishError {
    /// Failed to send command to the node runner
    #[error("Failed to send command to the node runner: {0}")]
    SendCommand(#[from] SendError),
    /// Node runner was dropped
    #[error("Node runner was dropped")]
    NodeRunnerDropped,
    /// Failed to publish message.
    #[error("Failed to publish message: {0}")]
    Publish(#[from] libp2p::gossipsub::PublishError),
}

impl From<oneshot::Canceled> for PublishError {
    #[inline]
    fn from(oneshot::Canceled: oneshot::Canceled) -> Self {
        Self::NodeRunnerDropped
    }
}

#[derive(Debug, Error)]
pub enum GetProvidersError {
    /// Failed to send command to the node runner
    #[error("Failed to send command to the node runner: {0}")]
    SendCommand(#[from] SendError),
    /// Node runner was dropped
    #[error("Node runner was dropped")]
    NodeRunnerDropped,
    /// Failed to get providers.
    #[error("Failed to get providers.")]
    GetProviders,
}

impl From<oneshot::Canceled> for GetProvidersError {
    #[inline]
    fn from(oneshot::Canceled: oneshot::Canceled) -> Self {
        Self::NodeRunnerDropped
    }
}

/// Defines errors for `send-request` operation.
#[derive(Debug, Error)]
pub enum SendRequestError {
    /// Failed to send command to the node runner
    #[error("Failed to send command to the node runner: {0}")]
    SendCommand(#[from] SendError),
    /// Node runner was dropped
    #[error("Node runner was dropped")]
    NodeRunnerDropped,
    /// Underlying protocol returned an error, impossible to get response.
    #[error("Underlying protocol returned an error: {0}")]
    ProtocolFailure(#[from] request_response_factory::RequestFailure),
    /// Underlying protocol returned an incorrect format, impossible to get response.
    #[error("Received incorrectly formatted response: {0}")]
    IncorrectResponseFormat(#[from] parity_scale_codec::Error),
}

impl From<oneshot::Canceled> for SendRequestError {
    #[inline]
    fn from(oneshot::Canceled: oneshot::Canceled) -> Self {
        Self::NodeRunnerDropped
    }
}

#[derive(Debug, Error)]
pub enum ConnectedPeersError {
    /// Failed to send command to the node runner
    #[error("Failed to send command to the node runner: {0}")]
    SendCommand(#[from] SendError),
    /// Node runner was dropped
    #[error("Node runner was dropped")]
    NodeRunnerDropped,
    /// Failed to get connected peers.
    #[error("Failed to get connected peers.")]
    ConnectedPeers,
}

impl From<oneshot::Canceled> for ConnectedPeersError {
    #[inline]
    fn from(oneshot::Canceled: oneshot::Canceled) -> Self {
        Self::NodeRunnerDropped
    }
}

#[derive(Debug, Error)]
pub enum BootstrapError {
    /// Failed to send command to the node runner
    #[error("Failed to send command to the node runner: {0}")]
    SendCommand(#[from] SendError),
    /// Node runner was dropped
    #[error("Node runner was dropped")]
    NodeRunnerDropped,
    /// Failed to bootstrap a peer.
    #[error("Failed to bootstrap a peer.")]
    Bootstrap,
}

impl From<oneshot::Canceled> for BootstrapError {
    #[inline]
    fn from(oneshot::Canceled: oneshot::Canceled) -> Self {
        Self::NodeRunnerDropped
    }
}

/// Implementation of a network node on Subspace Network.
#[derive(Debug, Clone)]
#[must_use = "Node doesn't do anything if dropped"]
pub struct Node {
    shared: Arc<Shared>,
}

impl Node {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }

    /// Node's own local ID.
    pub fn id(&self) -> PeerId {
        self.shared.id
    }

    /// Return a value from the Kademlia network of the DSN.
    pub async fn get_value(
        &self,
        key: Multihash,
    ) -> Result<impl Stream<Item = PeerRecord>, GetValueError> {
        let permit = self.shared.kademlia_tasks_semaphore.acquire().await;
        let (result_sender, result_receiver) = mpsc::unbounded();

        self.shared
            .command_sender
            .clone()
            .send(Command::GetValue {
                key,
                result_sender,
                permit,
            })
            .await?;

        // TODO: A wrapper that'll immediately cancel query on drop
        Ok(result_receiver)
    }

    /// Puts a value into the Kademlia network of the DSN.
    pub async fn put_value(
        &self,
        key: Multihash,
        value: Vec<u8>,
    ) -> Result<impl Stream<Item = ()>, PutValueError> {
        let permit = self.shared.kademlia_tasks_semaphore.acquire().await;
        let (result_sender, result_receiver) = mpsc::unbounded();

        self.shared
            .command_sender
            .clone()
            .send(Command::PutValue {
                key,
                value,
                result_sender,
                permit,
            })
            .await?;

        // TODO: A wrapper that'll immediately cancel query on drop
        Ok(result_receiver)
    }

    /// Subcribe to some topic on the DSN.
    pub async fn subscribe(&self, topic: Sha256Topic) -> Result<TopicSubscription, SubscribeError> {
        let permit = self.shared.regular_tasks_semaphore.acquire().await;
        let (result_sender, result_receiver) = oneshot::channel();

        self.shared
            .command_sender
            .clone()
            .send(Command::Subscribe {
                topic: topic.clone(),
                result_sender,
            })
            .await?;

        let CreatedSubscription {
            subscription_id,
            receiver,
        } = result_receiver.await??;

        Ok(TopicSubscription {
            topic: Some(topic),
            subscription_id,
            command_sender: Some(self.shared.command_sender.clone()),
            receiver,
            _permit: permit,
        })
    }

    /// Subcribe a messgo to some topic on the DSN.
    pub async fn publish(&self, topic: Sha256Topic, message: Vec<u8>) -> Result<(), PublishError> {
        let _permit = self.shared.regular_tasks_semaphore.acquire().await;
        let (result_sender, result_receiver) = oneshot::channel();

        self.shared
            .command_sender
            .clone()
            .send(Command::Publish {
                topic,
                message,
                result_sender,
            })
            .await?;

        result_receiver.await?.map_err(PublishError::Publish)
    }

    /// Sends the generic request to the peer and awaits the result.
    pub async fn send_generic_request<Request>(
        &self,
        peer_id: PeerId,
        request: Request,
    ) -> Result<Request::Response, SendRequestError>
    where
        Request: GenericRequest,
    {
        let _permit = self.shared.regular_tasks_semaphore.acquire().await;
        let (result_sender, result_receiver) = oneshot::channel();
        let command = Command::GenericRequest {
            peer_id,
            protocol_name: Request::PROTOCOL_NAME,
            request: request.encode(),
            result_sender,
        };

        self.shared.command_sender.clone().send(command).await?;

        let result = result_receiver.await??;

        Request::Response::decode(&mut result.as_slice()).map_err(Into::into)
    }

    /// Get closest peers by multihash key using Kademlia DHT.
    pub async fn get_closest_peers(
        &self,
        key: Multihash,
    ) -> Result<impl Stream<Item = PeerId>, GetClosestPeersError> {
        let permit = self.shared.kademlia_tasks_semaphore.acquire().await;
        trace!(?key, "Starting 'GetClosestPeers' request.");

        let (result_sender, result_receiver) = mpsc::unbounded();

        self.shared
            .command_sender
            .clone()
            .send(Command::GetClosestPeers {
                key,
                result_sender,
                permit,
            })
            .await?;

        // TODO: A wrapper that'll immediately cancel query on drop
        Ok(result_receiver)
    }

    /// Get item providers by its key. Initiate 'providers' Kademlia operation.
    pub async fn get_providers(
        &self,
        key: Multihash,
    ) -> Result<impl Stream<Item = PeerId>, GetProvidersError> {
        let permit = self.shared.kademlia_tasks_semaphore.acquire().await;
        let (result_sender, result_receiver) = mpsc::unbounded();

        trace!(?key, "Starting 'get_providers' request.");

        self.shared
            .command_sender
            .clone()
            .send(Command::GetProviders {
                key,
                result_sender,
                permit,
            })
            .await?;

        // TODO: A wrapper that'll immediately cancel query on drop
        Ok(result_receiver)
    }

    /// Ban peer with specified peer ID.
    pub async fn ban_peer(&self, peer_id: PeerId) -> Result<(), SendError> {
        self.shared
            .command_sender
            .clone()
            .send(Command::BanPeer { peer_id })
            .await
    }

    /// Dial multiaddress.
    /// It could be used to test libp2p transports bypassing protocol checks for bootstrap
    /// or listen-on addresses.
    #[doc(hidden)]
    pub async fn dial(&self, address: Multiaddr) -> Result<(), SendError> {
        self.shared
            .command_sender
            .clone()
            .send(Command::Dial { address })
            .await
    }

    /// Node's own addresses where it listens for incoming requests.
    pub fn listeners(&self) -> Vec<Multiaddr> {
        self.shared.listeners.lock().clone()
    }

    /// Node's own addresses observed remotely.
    pub fn external_addresses(&self) -> Vec<Multiaddr> {
        self.shared.external_addresses.lock().clone()
    }

    /// Callback is called when node starts listening on new address.
    pub fn on_new_listener(&self, callback: HandlerFn<Multiaddr>) -> HandlerId {
        self.shared.handlers.new_listener.add(callback)
    }

    /// Callback is called when number of established peer connections changes.
    pub fn on_num_established_peer_connections_change(
        &self,
        callback: HandlerFn<usize>,
    ) -> HandlerId {
        self.shared
            .handlers
            .num_established_peer_connections_change
            .add(callback)
    }

    /// Returns a collection of currently connected peers.
    pub async fn connected_peers(&self) -> Result<Vec<PeerId>, ConnectedPeersError> {
        let (result_sender, result_receiver) = oneshot::channel();

        trace!("Starting 'connected_peers' request.");

        self.shared
            .command_sender
            .clone()
            .send(Command::ConnectedPeers { result_sender })
            .await?;

        result_receiver
            .await
            .map_err(|_| ConnectedPeersError::ConnectedPeers)
    }

    /// Bootstraps Kademlia network
    pub async fn bootstrap(&self) -> Result<(), BootstrapError> {
        let (result_sender, mut result_receiver) = mpsc::unbounded();

        debug!("Starting 'bootstrap' request.");

        self.shared
            .command_sender
            .clone()
            .send(Command::Bootstrap { result_sender })
            .await?;

        for step in 0.. {
            let result = result_receiver.next().await;

            if result.is_some() {
                debug!(%step, "Kademlia bootstrapping...");
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Callback is called when we receive new [`crate::protocols::peer_info::PeerInfo`]
    pub fn on_peer_info(&self, callback: HandlerFn<NewPeerInfo>) -> HandlerId {
        self.shared.handlers.new_peer_info.add(callback)
    }

    /// Callback is called when a peer is disconnected.
    pub fn on_disconnected_peer(&self, callback: HandlerFn<PeerId>) -> HandlerId {
        self.shared.handlers.disconnected_peer.add(callback)
    }

    /// Callback is called when a peer is connected.
    pub fn on_connected_peer(&self, callback: HandlerFn<PeerId>) -> HandlerId {
        self.shared.handlers.connected_peer.add(callback)
    }
}
