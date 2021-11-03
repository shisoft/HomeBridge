use bytes::{Buf, BufMut, BytesMut};
use core::sync::atomic::Ordering::AcqRel;
use std::sync::atomic::AtomicBool;
use futures::{SinkExt, StreamExt};
use lightning::map::{Map, ObjectMap};
use log::*;
use parking_lot::RwLock;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize},
    Arc,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, channel, Sender};
use tokio_util::codec::{BytesCodec, Framed};

use crate::server;

struct ServerConnection {
    id: u64,
    sender: Sender<(u32, u64, BytesMut)>,
}

struct PortServerConnections {
    list: Vec<u64>,
    balancer: AtomicUsize,
}

struct BridgePorts {
    conns: ObjectMap<Arc<RwLock<PortServerConnections>>>,
}

struct BridgeServers {
    conns: ObjectMap<Arc<ServerConnection>>,
}

struct Bridge {
    ports: BridgePorts,
    servs: BridgeServers,
    clients: ObjectMap<Arc<ClientConnection>>,
    conn_counter: AtomicU64,
    serv_counter: AtomicU64,
}

struct ClientConnection {
    id: u64,
    tx: Sender<BytesMut>,
}

pub async fn start<'a>(addr: &'a str) -> Result<(), Box<dyn Error>> {
    let bridge = Arc::new(Bridge::instance());
    let listener = TcpListener::bind(addr).await?;
    loop {
        // Asynchronously wait for an inbound TcpStream.
        let (stream, addr) = listener.accept().await?;
        // Spawn our handler to be run asynchronously.
        debug!("Accepted bridge connection from {:?}", &addr);
        let bridge = bridge.clone();
        let serv_id = bridge
            .serv_counter
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        tokio::spawn(async move {
            bridge
                .servs
                .new_server(&bridge, serv_id, stream, addr)
                .await;
        });
    }
}

impl PortServerConnections {
    fn new() -> Self {
        Self {
            list: Default::default(),
            balancer: Default::default(),
        }
    }

    fn add_server_conn(&mut self, serv_id: u64) {
        self.list.push(serv_id);
    }

    fn next_server_conn(&self) -> u64 {
        let id = self.balancer.fetch_add(1, AcqRel);
        let len = self.list.len();
        self.list[id % len]
    }
}

impl Bridge {
    pub fn instance() -> Self {
        Self {
            conn_counter: AtomicU64::new(0),
            serv_counter: AtomicU64::new(0),
            ports: BridgePorts::new(),
            servs: BridgeServers::new(),
            clients: ObjectMap::with_capacity(128),
        }
    }
}

impl BridgePorts {
    fn new() -> Self {
        Self {
            conns: ObjectMap::with_capacity(64),
        }
    }
}

impl BridgeServers {
    fn new() -> Self {
        Self {
            conns: ObjectMap::with_capacity(32),
        }
    }

    async fn new_server(
        &self,
        bridge: &Arc<Bridge>,
        serv_id: u64,
        stream: TcpStream,
        addr: SocketAddr,
    ) {
        let server_conn = ServerConnection::new(serv_id, bridge, stream, addr).await;
        info!("New server connection {:?}, id {}", addr, serv_id);
        self.conns
            .insert(&(serv_id as usize), Arc::new(server_conn));
    }
}

impl ServerConnection {
    async fn new(id: u64, bridge: &Arc<Bridge>, stream: TcpStream, addr: SocketAddr) -> Self {
        let sender = Self::init_connection(id, bridge, stream).await;
        Self { id, sender }
    }

    async fn init_connection(
        id: u64,
        bridge: &Arc<Bridge>,
        stream: TcpStream,
    ) -> Sender<(u32, u64, BytesMut)> {
        let bridge = bridge.clone();
        let transport = Framed::new(stream, BytesCodec::new());
        let (mut writer, mut reader) = transport.split();
        let mut num_ports_bytes = reader.next().await.unwrap().unwrap();
        let mut num_ports_data = [0u8; 8];
        num_ports_bytes.copy_to_slice(&mut num_ports_data);
        let num_ports = u64::from_le_bytes(num_ports_data);
        trace!("Connection {} will have {} ports", id, num_ports);
        let mut ports = Vec::with_capacity(num_ports as usize);
        for i in 0..num_ports {
            trace!("Reading conn {} port # {}", id, i);
            let mut port_bytes = match reader.next().await {
                Some(Ok(b)) => {
                    trace!("Conn {} port # {} have {} data", id, i, b.len());
                    b
                },
                Some(Err(e)) => {
                    error!("Error on reading conn {} port # {}, error {:?}", id, i, e);
                    panic!();
                }
                None => {
                    error!("EOF");
                    panic!();
                }
            };
            let mut port_data = [0u8; 4];
            port_bytes.copy_to_slice(&mut port_data);
            let port = u32::from_le_bytes(port_data);
            trace!("Conn {} port # {} is {}", id, i, port);
            ports.push(port);
        }
        info!("Connection {} accepts ports {:?}", id, ports);
        init_ports(ports, &bridge, id).await;
        let (write_tx, mut write_rx) = mpsc::channel::<(u32, u64, BytesMut)>(128);
        // Receving packets sending through the connection to the server
        tokio::spawn(async move {
            while let Some((port, conn, data)) = write_rx.recv().await {
                let data_size = data.len();
                let mut out_data = BytesMut::new();
                out_data.put_u32_le(port);
                out_data.put_u64_le(conn);
                out_data.put(data);
                if let Err(e) = writer.send(out_data.freeze()).await {
                    error!(
                        "Error on sending packets to server, port {}, conn {}, data size {}, error {:?}",
                        port, conn, data_size, e
                    );
                }
            }
        });
        // Receving packets sent from the server to some client
        tokio::spawn(async move {
            while let Some(Ok(mut res)) = reader.next().await {
                let conn_id = res.get_u64_le();
                if let Some(conn) = bridge.clients.get(&(conn_id as usize)) {
                    match conn.tx.send(res).await {
                        Ok(()) => {},
                        Err(e) => {
                            error!("Error on sending data to channel {:?}", e);
                        }
                    }
                } else {
                    error!("Received packet from conn {} but cannot find it", conn_id);
                }
            }
        });
        return write_tx;
    }
}

async fn init_ports(ports: Vec<u32>, bridge: &Arc<Bridge>, serv_id: u64) {
    for port in ports {
        let port_key = port as usize;
        loop {
            if let Some(servs) = bridge.ports.conns.get(&port_key) {
                servs.write().add_server_conn(serv_id);
                info!("Added new port {} with serv_id {}", port, serv_id);
                break;
            } else {
                debug!("Going to start bridge port server for port {}", port);
                let servs = Arc::new(RwLock::new(PortServerConnections::new()));
                if bridge
                    .ports
                    .conns
                    .try_insert(&port_key, servs.clone())
                    .is_none()
                {
                    debug!("Starting bridge port server for port {}", port);
                    servs.write().add_server_conn(serv_id);
                    let bridge = bridge.clone();
                    tokio::spawn(async move {
                        if let Err(e) = init_client_server(port, &bridge, &servs).await {
                            error!("Client server end with error {:?}", e)
                        }
                    });
                    break;
                }
            }
            warn!("Need to retry adding port {} with {}", port, serv_id);
        }
    }
}

async fn init_client_server(
    port: u32,
    bridge: &Arc<Bridge>,
    port_conns: &Arc<RwLock<PortServerConnections>>,
) -> io::Result<()> {
    info!("Starting to listen at port {}", port);
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        let bridge = bridge.clone();
        let port_conns = port_conns.clone();
        let conn_id = bridge.conn_counter.fetch_add(1, AcqRel);
        let (client_tx, mut client_rx) = channel::<BytesMut>(128);
        bridge.clients.insert(
            &(conn_id as usize),
            Arc::new(ClientConnection {
                id: conn_id,
                tx: client_tx,
            }),
        );
        tokio::spawn(async move {
            trace!(
                "Accepting connection {} at port {} from {:?}",
                conn_id,
                port,
                addr
            );
            let transport = Framed::new(stream, BytesCodec::new());
            let (mut writer, mut reader) = transport.split();
            tokio::spawn(async move {
                while let Some(data) = client_rx.recv().await {
                    writer.send(data.freeze()).await;
                }
                writer.close().await;
            });
            while let Some(Ok(res)) = reader.next().await {
                let serv_id = port_conns.read().next_server_conn();
                loop {
                    match bridge.servs.conns.get(&(serv_id as usize)) {
                        Some(serv) => {
                            serv.sender.send((port, conn_id, res)).await;
                            break;
                        }
                        None => {
                            error!("Cannot find service with id {}", serv_id);
                        }
                    }
                }
            }
            bridge.clients.remove(&(conn_id as usize));
            info!("Connection {} of port {} disconnected", conn_id, port);
        });
    }
}
