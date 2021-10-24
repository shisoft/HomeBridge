use bytes::{Buf, BufMut, BytesMut};
use core::sync::atomic::Ordering::AcqRel;
use futures::{SinkExt, StreamExt};
use lightning::map::{Map, ObjectMap};
use log::*;
use parking_lot::RwLock;
use std::error::Error;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize},
    Arc,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{self, Sender};
use tokio_util::codec::{BytesCodec, Framed};

struct ServerConnection {
    id: u64,
    sender: Sender<(u32, u64, BytesMut)>,
}

struct PortServerConnections {
    list: Vec<u64>,
    balancer: AtomicUsize,
}

struct BridgePorts {
    conns: ObjectMap<Arc<ObjectMap<Arc<RwLock<PortServerConnections>>>>>
}

struct BridgeServers {
    conns: ObjectMap<Arc<ServerConnection>>
}

struct Bridge {
    ports: BridgePorts,
    servs: BridgeServers,
    conn_counter: AtomicU64,
    serv_vounter: AtomicU64
}

struct ClientConnection {

}



struct ServerConnPool {}

pub async fn new<'a>(addr: &'a str) -> Result<(), Box<dyn Error>> {
    let bridge = Arc::new(Bridge::instance());
    let listener = TcpListener::bind(addr).await?;
    info!("Running bridge service on {}", addr);
    loop {
        // Asynchronously wait for an inbound TcpStream.
        let (stream, addr) = listener.accept().await?;
        // Spawn our handler to be run asynchronously.
        debug!("Accepted bridge connection from {:?}", &addr);
        let bridge = bridge.clone();
        tokio::spawn(async move {
            let transport = Framed::new(stream, BytesCodec::new());
            let (mut writer, mut reader) = transport.split();
            let mut num_ports_bytes = reader.next().await.unwrap().unwrap();
            let mut num_ports_data = [0u8; 8];
            num_ports_bytes.copy_to_slice(&mut num_ports_data);
            let num_ports = u64::from_le_bytes(num_ports_data);
            let mut ports = Vec::with_capacity(num_ports as usize);
            let serv_id = bridge_serv_counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            for _ in 0..num_ports {
                let mut port_bytes = reader.next().await.unwrap().unwrap();
                let mut port_data = [0u8; 4];
                port_bytes.copy_to_slice(&mut port_data);
                ports.push(u32::from_le_bytes(port_data));
            }
            info!(
                "Server {:?} requesting {} ports {:?}",
                addr, num_ports, ports
            );
            let (write_tx, mut write_rx) = mpsc::channel::<(u32, u64, BytesMut)>(128);
            tokio::spawn(async move {
                while let Some((port, conn, data)) = write_rx.recv().await {
                    let data_size = data.len();
                    let mut out_data = BytesMut::new();
                    out_data.put_u32_le(port);
                    out_data.put_u64_le(conn);
                    out_data.put(data);
                    if let Err(e) = writer.send(out_data.freeze()).await {
                        error!(
                            "Error on sending packets to server, port {}, conn {}, data size {}",
                            port, conn, data_size
                        );
                    }
                }
            });
            while let Some(Ok(mut res)) = reader.next().await {
                let conn_id = res.get_u64_le();
                if let Some(conn) = bridge_servs.get(&(conn_id as usize)) {
                    // TODO: redirect packets to the connection for clients
                } else {
                    error!("Received packet from conn {} but cannot find it", conn_id);
                }
            }
            for port in ports {
                let port_key = port as usize;
                loop {
                    if let Some(servs) = bridge_ports.get(&port_key) {
                        servs.write().add_server_conn(serv_id);
                        info!("Added new port {} with serv_id {}", port, serv_id);
                        break;
                    } else {
                        let servs = Arc::new(RwLock::new(Ports::new()));
                        if bridge_ports.try_insert(&port_key, servs.clone()).is_none() {
                            servs.write().add_server_conn(serv_id);
                            info!("Starting to listen at port {}", port);
                            match TcpListener::bind(format!("0.0.0.0:{}", port)).await {
                                Ok(listener) => {
                                    match listener.accept().await {
                                        Ok((stream, addr)) => {
                                            let conn_id = bridge_conn_counter.fetch_add(1, AcqRel);
                                            // let conn_chan = channel(128);
                                            trace!(
                                                "Accepting connection {} at port {} from {:?}",
                                                conn_id,
                                                port,
                                                addr
                                            );
                                            let servs = servs.clone();
                                            let bridge_servs = bridge_servs.clone();
                                            tokio::spawn(async move {
                                                let transport =
                                                    Framed::new(stream, BytesCodec::new());
                                                let (mut writer, mut reader) = transport.split();
                                                while let Some(Ok(mut res)) = reader.next().await {
                                                    let serv_id = servs.read().next_server_conn();
                                                    loop {
                                                        match bridge_servs.get(&(serv_id as usize))
                                                        {
                                                            Some(serv) => {
                                                                serv.sender
                                                                    .send((port, conn_id, res))
                                                                    .await;
                                                                break;
                                                            }
                                                            None => {
                                                                error!("Cannot find service with id {}", serv_id);
                                                            }
                                                        }
                                                    }
                                                }
                                            });
                                        }
                                        Err(e) => {
                                            error!(
                                                "Cannot accept connection at port {}, error {:?}",
                                                port, e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Cannot listen at port {}, error {:?}", port, e);
                                }
                            }
                        }
                    }
                    warn!("Need to retry adding port {} with {}", port, serv_id);
                }
            }
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
            serv_vounter: AtomicU64::new(0),
            ports: BridgePorts::new(),
            servs: BridgeServers::new()
        }
    }
}

impl BridgePorts {
    fn new() -> Self {
        Self {
            conns: ObjectMap::with_capacity(64)
        }
    }
}

impl BridgeServers {
    fn new() -> Self {
        Self {
            conns: ObjectMap::with_capacity(32)
        }
    }
}