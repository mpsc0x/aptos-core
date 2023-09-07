// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    application::{
        error::Error,
        metadata::{ConnectionState, PeerMetadata},
    },
    transport::{ConnectionId, ConnectionMetadata},
    ProtocolId,
};
use aptos_config::{
    config::PeerSet,
    network_id::{NetworkId, PeerNetworkId},
};
use aptos_infallible::RwLock;
use aptos_peer_monitoring_service_types::PeerMonitoringMetadata;
use aptos_types::PeerId;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use std::sync::atomic::{AtomicU32, Ordering};

/// A simple container that tracks all peers and peer metadata for the node.
/// This container is updated by both the networking code (e.g., for new
/// peer connections and lost peer connections), as well as individual
/// applications (e.g., peer monitoring service).
#[derive(Debug)]
pub struct PeersAndMetadata {
    network_ids: Vec<NetworkId>,
    peers_and_metadata: RwLock<HashMap<NetworkId, HashMap<PeerId, PeerMetadata>>>,
    generation: AtomicU32,

    // trusted_peers are configured at system startup and not part of the generational peer data
    trusted_peers: HashMap<NetworkId, Arc<RwLock<PeerSet>>>,
}

impl PeersAndMetadata {
    pub fn new(network_ids: &[NetworkId]) -> Arc<PeersAndMetadata> {
        // Create the container
        let network_ids = network_ids.to_vec();
        let mut peers_and_metadata = PeersAndMetadata {
            network_ids,
            peers_and_metadata: RwLock::new(HashMap::new()),
            generation: AtomicU32::new(1),
            trusted_peers: HashMap::new(),
        };

        // Initialize each network mapping and trusted peer set
        {
            let mut wat = peers_and_metadata.peers_and_metadata.write();
            peers_and_metadata.network_ids.iter().for_each(|network_id| {
                wat.insert(*network_id, HashMap::new());

                peers_and_metadata
                    .trusted_peers
                    .insert(*network_id, Arc::new(RwLock::new(PeerSet::new())));
            });
        }

        Arc::new(peers_and_metadata)
    }

    /// Returns the networks currently held in the container
    pub fn get_registered_networks(&self) -> impl Iterator<Item = NetworkId> + '_ {
        self.network_ids.iter().copied()
    }

    /// Returns all peers. Note: this will return disconnected and unhealthy peers, so
    /// it is not recommended for applications to use this interface. Instead,
    /// `get_connected_peers_and_metadata()` should be used.
    pub fn get_all_peers(&self) -> Vec<PeerNetworkId> {
        let mut all_peers = Vec::new();
        let read = self.peers_and_metadata.read();
        for (network_id, peers) in read.iter() {
            for (peer_id, _) in peers.iter() {
                let peer_network_id = PeerNetworkId::new(*network_id, *peer_id);
                all_peers.push(peer_network_id);
            }
        }
        return all_peers;
    }

    /// Return copy of list of peers if generation is different than previously held.
    /// May optionally filter for connected peers only.
    /// May optionally filter on match for any of a set of ProtocolId.
    pub fn get_all_peers_generational(
        &self,
        generation: u32,
        require_connected: bool,
        protocol_ids: &[ProtocolId],
    ) -> Option<(Vec<PeerNetworkId>, u32)> {
        let generation_test = self.generation.load(Ordering::SeqCst);
        if generation == generation_test {
            return None;
        }
        let mut all_peers = Vec::new();
        let read = self.peers_and_metadata.read();
        for (network_id, peers) in read.iter() {
            for (peer_id, peer_metadata) in peers.iter() {
                if require_connected && !peer_metadata.is_connected() {
                    continue;
                }
                if (protocol_ids.len() != 0) && !peer_metadata.supports_any_protocol(protocol_ids) {
                    continue;
                }
                let peer_network_id = PeerNetworkId::new(*network_id, *peer_id);
                all_peers.push(peer_network_id);
            }
        }
        let generation_actual = self.generation.load(Ordering::SeqCst);
        return Some((all_peers, generation_actual));
    }

    /// Returns all connected peers that support at least one of
    /// the given protocols.
    pub fn get_connected_supported_peers(
        &self,
        protocol_ids: &[ProtocolId],
    ) -> Result<Vec<PeerNetworkId>, Error> {
        let mut connected_supported_peers = Vec::new();
        let read = self.peers_and_metadata.read();
        for (network_id, peers) in read.iter() {
            for (peer_id, peer_metadata) in peers.iter() {
                if peer_metadata.is_connected() && peer_metadata.supports_any_protocol(protocol_ids)
                {
                    let peer_network_id = PeerNetworkId::new(*network_id, *peer_id);
                    connected_supported_peers.push(peer_network_id);
                }
            }
        }

        Ok(connected_supported_peers)
    }

    /// Returns metadata for all peers currently connected to the node
    pub fn get_connected_peers_and_metadata(
        &self,
    ) -> Result<HashMap<PeerNetworkId, PeerMetadata>, Error> {
        let mut connected_peers_and_metadata = HashMap::new();
        let read = self.peers_and_metadata.read();
        for (network_id, peers) in read.iter() {
            for (peer_id, peer_metadata) in peers.iter() {
                if peer_metadata.is_connected() {
                    let peer_network_id = PeerNetworkId::new(*network_id, *peer_id);
                    connected_peers_and_metadata.insert(peer_network_id, peer_metadata.clone());
                }
            }
        }

        Ok(connected_peers_and_metadata)
    }

    /// Returns the metadata for the specified peer
    pub fn get_metadata_for_peer(
        &self,
        peer_network_id: PeerNetworkId,
    ) -> Result<PeerMetadata, Error> {
        let read = self.peers_and_metadata.read();
        match read.get(&peer_network_id.network_id()) {
            None => {Err(missing_metadata_error(&peer_network_id))}
            Some(peers) => {
                match peers.get(&peer_network_id.peer_id()) {
                    None => {Err(missing_metadata_error(&peer_network_id))}
                    Some(pm) => {
                        Ok(pm.clone())
                    }
                }
            }
        }
    }

    /// Returns the trusted peer set for the given network ID
    pub fn get_trusted_peers(&self, network_id: &NetworkId) -> Result<Arc<RwLock<PeerSet>>, Error> {
        self.trusted_peers.get(network_id).cloned().ok_or_else(|| {
            Error::UnexpectedError(format!(
                "No trusted peers were found for the given network id: {:?}",
                network_id
            ))
        })
    }

    /// Updates the connection metadata associated with the given peer.
    /// If no peer metadata exists, a new one is created.
    pub fn insert_connection_metadata(
        &self,
        peer_network_id: PeerNetworkId,
        connection_metadata: ConnectionMetadata,
    ) -> Result<(), Error> {
        let mut writer = self.peers_and_metadata.write();
        let mut peer_metadata_for_network = writer.get_mut(&peer_network_id.network_id()).unwrap();
        self.generation.fetch_add(1 , Ordering::SeqCst);
        peer_metadata_for_network.entry(peer_network_id.peer_id())
            .and_modify(|peer_metadata| {
                println!("");
                peer_metadata.connection_metadata = connection_metadata.clone()
            })
            .or_insert_with(|| PeerMetadata::new(connection_metadata));

        Ok(())
    }

    /// Updates the connection state associated with the given peer.
    /// If no peer metadata exists, an error is returned.
    pub fn update_connection_state(
        &self,
        peer_network_id: PeerNetworkId,
        connection_state: ConnectionState,
    ) -> Result<(), Error> {
        let mut writer = self.peers_and_metadata.write();
        let mut peer_metadata_for_network = writer.get_mut(&peer_network_id.network_id()).unwrap();

        // Update the connection state for the peer or return a missing metadata error
        if let Some(peer_metadata) = peer_metadata_for_network
            .get_mut(&peer_network_id.peer_id())
        {
            self.generation.fetch_add(1 , Ordering::SeqCst);
            peer_metadata.connection_state = connection_state;
            Ok(())
        } else {
            Err(missing_metadata_error(&peer_network_id))
        }
    }

    /// Updates the peer monitoring state associated with the given peer.
    /// If no peer metadata exists, an error is returned.
    pub fn update_peer_monitoring_metadata(
        &self,
        peer_network_id: PeerNetworkId,
        peer_monitoring_metadata: PeerMonitoringMetadata,
    ) -> Result<(), Error> {
        let mut writer = self.peers_and_metadata.write();
        let mut peer_metadata_for_network = writer.get_mut(&peer_network_id.network_id()).unwrap();

        // Update the peer monitoring metadata for the peer or return a missing metadata error
        if let Some(peer_metadata) = peer_metadata_for_network
            .get_mut(&peer_network_id.peer_id())
        {
            self.generation.fetch_add(1 , Ordering::SeqCst);
            peer_metadata.peer_monitoring_metadata = peer_monitoring_metadata;
            Ok(())
        } else {
            Err(missing_metadata_error(&peer_network_id))
        }
    }

    /// Removes the peer metadata from the container. If the peer
    /// doesn't exist, or the connection id doesn't match, an error is
    /// returned. Otherwise, the existing peer metadata is returned.
    pub fn remove_peer_metadata(
        &self,
        peer_network_id: PeerNetworkId,
        connection_id: ConnectionId,
    ) -> Result<PeerMetadata, Error> {
        let mut writer = self.peers_and_metadata.write();
        let mut peer_metadata_for_network = writer.get_mut(&peer_network_id.network_id()).unwrap();

        // Remove the peer metadata for the peer or return a missing metadata error
        if let Entry::Occupied(entry) = peer_metadata_for_network
            .entry(peer_network_id.peer_id())
        {
            // Don't remove the peer if the connection doesn't match!
            // For now, remove the peer entirely, we could in the future
            // have multiple connections for a peer
            let active_connection_id = entry.get().connection_metadata.connection_id;
            if active_connection_id == connection_id {
                self.generation.fetch_add(1 , Ordering::SeqCst);
                Ok(entry.remove())
            } else {
                Err(Error::UnexpectedError(format!(
                    "The peer connection id did not match! Given: {:?}, found: {:?}.",
                    connection_id, active_connection_id
                )))
            }
        } else {
            Err(missing_metadata_error(&peer_network_id))
        }
    }
}

/// A simple helper for returning a missing metadata error
/// for the specified peer.
fn missing_metadata_error(peer_network_id: &PeerNetworkId) -> Error {
    Error::UnexpectedError(format!(
        "No metadata was found for the given peer: {:?}",
        peer_network_id
    ))
}
//
// #[derive(Debug)]
// pub struct PeersAndMetadataGenerations {
//     peers_and_metadata: Arc<RwLock<PeersAndMetadata>>,
//     generation: AtomicU32,
// }
//
// impl PeersAndMetadataGenerations {
//     pub fn new(peers_and_metadata: Arc<PeersAndMetadata>) -> Self {
//         Self{
//             peers_and_metadata,
//             generation: AtomicU32::new(1),
//         }
//     }
//
//     pub fn get_generation(&self) -> u32 {
//         self.generation.load(Ordering::Relaxed)
//     }
//
//     pub fn maybe_get_peers(&self) -> Option<HashMap<NetworkId, HashMap<PeerId, PeerMetadata>>> {
//
//     }
// }