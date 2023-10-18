// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::marker::PhantomData;
use aptos_logger::info;
use crate::ProtocolId;
use crate::protocols::network::ReceivedMessage;

pub mod error;
pub mod interface;
pub mod metadata;
pub mod storage;

/// Container for connection to application code listening on a ProtocolId
pub struct ApplicationConnections {
    pub protocol_id: ProtocolId,

    /// sender receives messages from network, towards application code
    pub sender: tokio::sync::mpsc::Sender<ReceivedMessage>,

    /// label used in metrics counters
    pub label: String,
}

impl ApplicationConnections {
    pub fn build(protocol_id: ProtocolId, queue_size: usize, label: &str) -> (ApplicationConnections, tokio::sync::mpsc::Receiver<ReceivedMessage>) {
        let (sender, receiver) = tokio::sync::mpsc::channel(queue_size);
        info!("app_int setup AC.build {} {} -> {:?} -> {:?}", label, protocol_id.as_str(), &sender, &receiver);
        (ApplicationConnections {
            protocol_id,
            sender,
            label: label.to_string(),
        }, receiver)
    }
}

/// Routing by ProtocolId for all application code built into a node.
/// Typically built early in startup code and then read-only.
pub struct ApplicationCollector {
    // apps: BTreeMap<ProtocolId,ApplicationConnections>,
    apps: Vec<ApplicationConnections>,
}

// type Iter<'a,K,V> = std::collections::btree_map::Iter<'a,K,V>;

impl ApplicationCollector {
    pub fn new() -> Self {
        Self {
            // apps: BTreeMap::new(),
            apps: Vec::new(),
        }
    }

    pub fn add(&mut self, connections: ApplicationConnections) {
        // self.apps.insert(connections.protocol_id, connections);
        self.apps.push(connections);
    }

    pub fn get(&self, protocol_id: &ProtocolId) -> Option<&ApplicationConnections> {
        // self.apps.get(protocol_id)
        for ac in self.apps.iter() {
            if ac.protocol_id == *protocol_id {
                return Some(ac);
            }
        }
        None
    }

    pub fn iter(&self) -> Iter {
        // self.apps.iter()
        Iter::new(self.apps.iter())
    }
}

pub struct Iter<'a> {
    subi: std::slice::Iter<'a,ApplicationConnections>,
}

impl<'a> Iter<'a> {
    fn new(subi: std::slice::Iter<'a,ApplicationConnections>) -> Self {
        Self{subi}
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a ProtocolId,&'a ApplicationConnections);

    fn next(&mut self) -> Option<Self::Item> {
        match self.subi.next() {
            None => {None}
            Some(sv) => {Some((&sv.protocol_id,sv))}
        }
    }
}


#[cfg(test)]
mod tests;