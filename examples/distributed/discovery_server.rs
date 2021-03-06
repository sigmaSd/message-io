use super::common::{Message};

use message_io::events::{EventQueue};
use message_io::network::{NetworkManager, NetEvent, Endpoint};

use std::net::{SocketAddr};
use std::collections::{HashMap};

enum Event {
    Network(NetEvent<Message>),
}

struct ParticipantInfo {
    addr: SocketAddr,
    endpoint: Endpoint,
}

pub struct DiscoveryServer {
    event_queue: EventQueue<Event>,
    network: NetworkManager,
    participants: HashMap<String, ParticipantInfo>,
}

impl DiscoveryServer {
    pub fn new() -> Option<DiscoveryServer> {
        let mut event_queue = EventQueue::new();

        let network_sender = event_queue.sender().clone();
        let mut network = NetworkManager::new(move |net_event| network_sender.send(Event::Network(net_event)));

        let listen_addr = "127.0.0.1:5000";
        match network.listen_tcp(listen_addr) {
            Ok(_) => {
                println!("Discovery server running at {}", listen_addr);
                Some(DiscoveryServer{
                    event_queue,
                    network,
                    participants: HashMap::new(),
                })
            },
            Err(_) => {
                println!("Can not listen on {}", listen_addr);
                None
            }
        }
    }

    pub fn run(mut self) {
        loop {
            match self.event_queue.receive() {
                Event::Network(net_event) => match net_event {
                    NetEvent::Message(endpoint, message) => match message {
                        Message::RegisterParticipant(name, addr) => {
                            self.register(&name, addr, endpoint);
                        }
                        Message::UnregisterParticipant(name) => {
                            self.unregister(&name);
                        }
                        _ => unreachable!(),
                    },
                    NetEvent::AddedEndpoint(_) => (),
                    NetEvent::RemovedEndpoint(endpoint) => {
                        //Participant disconection without explict unregistration. We must remove from the registry too.
                        if let Some(name) = self.participants.iter().find_map(|(name, info)| if info.endpoint == endpoint { Some(name.clone()) } else { None } ) {
                            self.unregister(&name)
                        }
                    }
                }
            }
        }
    }

    fn register(&mut self, name: &str, addr: SocketAddr, endpoint: Endpoint) {
        if !self.participants.contains_key(name) {
            // Update the new participant with the whole participants information
            let participant_list = self.participants.iter().map(|(name, info)| (name.clone(), info.addr)).collect();
            self.network.send(endpoint, Message::ParticipantList(participant_list)).unwrap();

            // Notify other participants about this new participant
            let participant_endpoints = self.participants.values().map(|info| &info.endpoint);
            self.network.send_all(participant_endpoints, Message::ParticipantNotificationAdded(name.to_string(), addr)).unwrap();

            // Register participant
            self.participants.insert(name.to_string(), ParticipantInfo {addr, endpoint});
            println!("Added participant '{}' with ip {}", name, addr);
        }
        else {
            println!("Participant with name '{}' already exists, please registry with another name", name);
        }
    }

    fn unregister(&mut self, name: &str) {
        if let Some(info) = self.participants.remove(name) {
            // Notify other participants about this removed participant
            let participant_endpoints = self.participants.values().map(|info| &info.endpoint);
            self.network.send_all(participant_endpoints, Message::ParticipantNotificationRemoved(name.to_string())).unwrap();
            println!("Removed participant '{}' with ip {}", name, info.addr);
        }
        else {
            println!("Can not unregister an non-existent participant with name '{}'", name);
        }
    }
}
