//! Establishing a peer link: the inbound accept path
//! ([`handle_connection`]), the outbound dial-and-retry loop
//! ([`connect_to_peer`]), and the handshake read that gates both
//! ([`read_handshake`]). All three hand off to
//! [`super::session::run_peer_session`] once the peer's identity is verified.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::SplitStream;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_util::sync::CancellationToken;

use crate::configuration::{Configuration, Peer};
use crate::operations;
use crate::peer::handshake::{HandshakeMessage, Identity};
use crate::peer::session::{PeerContext, run_peer_session};

pub async fn handle_connection(
    configuration: Configuration,
    identity: Arc<Identity>,
    main_db_path: PathBuf,
    context: PeerContext,
    raw_stream: TcpStream,
    address: SocketAddr,
    shutdown: CancellationToken,
) {
    log::debug!("Incoming TCP connection from: {:?}", address);

    let Ok(websoccket_stream) = tokio_tungstenite::accept_async(raw_stream).await else {
        log::error!("Error during the websocket handshake occurred");
        return;
    };

    log::debug!("WebSocket connection established: {:?}", address);

    let (mut outgoing, mut incoming) = websoccket_stream.split();

    // Read the peer's handshake first (they initiated the TCP connection).
    let peer_public_key = match read_handshake(&mut incoming, &configuration, &identity).await {
        HandshakeResult::Accepted(public_key) => public_key,
        HandshakeResult::Rejected => return,
    };

    // Respond: sign the peer's public key to prove we own our private key.
    let response = match identity.sign_handshake(&peer_public_key) {
        Ok(response) => response,
        Err(error) => {
            log::warn!("Failed to build handshake response for {address}: {error}");
            return;
        }
    };

    if let Err(error) = outgoing
        .send(Message::text(serde_json::to_string(&response).unwrap()))
        .await
    {
        log::warn!("Failed to send handshake to {address}: {error}");
        return;
    }

    let peer_name = configuration.peer_name(&peer_public_key).to_owned();

    log::info!("Inbound peer at {address} identified as {peer_name} ({peer_public_key})");

    run_peer_session(
        &peer_public_key,
        &peer_name,
        &main_db_path,
        outgoing,
        incoming,
        operations::Direction::Inbound,
        context,
        &shutdown,
    )
    .await;

    log::info!("Inbound connection from {peer_name} closed");
}

/// Maintain an outbound WebSocket connection to a single peer.
///
/// On each successful connection, a fresh `(peer_tx, peer_rx)` channel is
/// created. `peer_tx` is stored in
/// `RuntimeConfiguration.peers[public_key].outbound` so that `forward_to_peers`
/// can send `Change`s to this peer. When the connection drops, `outbound` is
/// reset to `None` and the task sleeps before retrying.
pub async fn connect_to_peer(
    identity: Arc<Identity>,
    peer: Peer,
    main_db_path: PathBuf,
    context: PeerContext,
    shutdown: CancellationToken,
) {
    // TODO: Make this configurable.
    const RETRY_INTERVAL: Duration = Duration::from_secs(5);

    let Some((ip, port)) = peer.address else {
        // Caller should have filtered these out, but be defensive.
        return;
    };
    let url = format!("ws://{ip}:{port}");

    loop {
        if shutdown.is_cancelled() {
            return;
        }

        log::debug!("Attempting outbound connection to {} ({url})", peer.name);
        // Surface the connection attempt as a live operation. It resolves when
        // we hand off to `run_peer_session` (completed) or the attempt fails
        // (the handle is dropped -> aborted).
        let connecting = context
            .operations
            .begin(operations::OperationKind::connecting_to_peer(
                peer.name.clone(),
                url.clone(),
            ));
        let connect = tokio::select! {
            _ = shutdown.cancelled() => return,
            connect = tokio_tungstenite::connect_async(&url) => connect,
        };
        match connect {
            Ok((ws_stream, _response)) => {
                log::info!("Outbound connection established to {} ({url})", peer.name);

                let (mut outgoing, mut incoming) = ws_stream.split();

                // Build our handshake: sign the peer's public key to prove our identity.
                let handshake = match identity.sign_handshake(&peer.public_key) {
                    Ok(handshake) => handshake,
                    Err(error) => {
                        log::error!("Cannot build handshake for peer {}: {error}", peer.name);
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                };

                // Send our handshake first.
                if let Err(error) = outgoing
                    .send(Message::text(serde_json::to_string(&handshake).unwrap()))
                    .await
                {
                    log::warn!("Failed to send handshake to {}: {error}", peer.name);
                    tokio::time::sleep(RETRY_INTERVAL).await;
                    continue;
                }

                // Read their response.
                let received = match incoming.next().await {
                    Some(Ok(message)) => message.to_string(),
                    Some(Err(error)) => {
                        log::warn!("Handshake read error from {}: {error}", peer.name);
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                    None => {
                        log::warn!("Peer {} closed before sending handshake", peer.name);
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                };
                let response: HandshakeMessage = match serde_json::from_str(&received) {
                    Ok(response) => response,
                    Err(error) => {
                        log::warn!("Invalid handshake JSON from {}: {error}", peer.name);
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                };

                // Verify their public key matches what we expect.
                if response.public_key != peer.public_key {
                    log::warn!(
                        "Peer {} announced public_key {:?}, expected {:?}; dropping connection",
                        peer.name,
                        response.public_key,
                        peer.public_key
                    );
                    tokio::time::sleep(RETRY_INTERVAL).await;
                    continue;
                }

                // Verify their signature proves ownership of that public key.
                if let Err(error) = identity.verify_handshake(&response) {
                    log::warn!(
                        "Peer {} handshake verification failed ({error}); dropping connection",
                        peer.name
                    );
                    tokio::time::sleep(RETRY_INTERVAL).await;
                    continue;
                }

                // Connected: the attempt operation is done. The live link is
                // now tracked as connection *state* by `run_peer_session` (it
                // registers with the connection registry), not as an operation.
                connecting.complete();

                run_peer_session(
                    &peer.public_key,
                    &peer.name,
                    &main_db_path,
                    outgoing,
                    incoming,
                    operations::Direction::Outbound,
                    context.clone(),
                    &shutdown,
                )
                .await;

                log::info!("Outbound connection to {} dropped", peer.name);
            }
            Err(error) => {
                log::debug!("Outbound connection to {url} failed: {error}");
            }
        }

        if shutdown.is_cancelled() {
            return;
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(RETRY_INTERVAL) => {}
        }
    }
}

pub enum HandshakeResult {
    Accepted(String),
    Rejected,
}

pub async fn read_handshake(
    incoming: &mut SplitStream<WebSocketStream<TcpStream>>,
    configuration: &Configuration,
    identity: &Identity,
) -> HandshakeResult {
    let Some(first) = incoming.next().await else {
        log::warn!("Peer closed before sending handshake");
        return HandshakeResult::Rejected;
    };
    let first = match first {
        Ok(message) => message.to_string(),
        Err(error) => {
            log::warn!("Handshake read error: {error}");
            return HandshakeResult::Rejected;
        }
    };
    let message: HandshakeMessage = match serde_json::from_str(&first) {
        Ok(message) => message,
        Err(error) => {
            log::warn!("Invalid handshake JSON: {error}");
            return HandshakeResult::Rejected;
        }
    };

    if !configuration
        .peers
        .iter()
        .any(|peer| peer.public_key == message.public_key)
    {
        log::warn!(
            "Rejecting connection: unknown public_key {:?}",
            message.public_key
        );
        return HandshakeResult::Rejected;
    }

    // Verify the peer's signature proves ownership of that public key.
    match identity.verify_handshake(&message) {
        Ok(peer_public_key) => HandshakeResult::Accepted(peer_public_key),
        Err(error) => {
            log::warn!("Peer handshake verification failed ({error}); rejecting connection");
            HandshakeResult::Rejected
        }
    }
}
