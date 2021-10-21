use bytes::{Buf, BufMut, BytesMut};
use tokio::sync::mpsc::channel;
use core::num;
use futures::{SinkExt, StreamExt};
use lightning::map::{HashSet, Map, ObjectMap};
use log::*;
use std::error::Error;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize},
    Arc,
};
use tokio::net::TcpListener;
use tokio_util::codec::{BytesCodec, Framed};

struct Connection {
    id: u64,
}

struct ServerConnPool {}

pub async fn new<'a>(addr: &'a str) -> Result<(), Box<dyn Error>> {
    let bridge_ports = Arc::new(ObjectMap::<Arc<HashSet<u64>>>::with_capacity(16));
    let bridge_servs = Arc::new(ObjectMap::<Arc<Connection>>::with_capacity(128));
    let bridge_conn_counter = Arc::new(AtomicU64::new(0));
    let bridge_serv_counter = Arc::new(AtomicU64::new(0));
    let listener = TcpListener::bind(addr).await?;
    info!("Running bridge service on {}", addr);
    loop {
        // Asynchronously wait for an inbound TcpStream.
        let (stream, addr) = listener.accept().await?;
        // Spawn our handler to be run asynchronously.
        debug!("Accepted bridge connection from {:?}", &addr);
        let bridge_servs = bridge_servs.clone();
        let bridge_serv_counter = bridge_serv_counter.clone();
        let bridge_ports = bridge_ports.clone();
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
            let (write_tx, mut write_rx) = channel::<(u32, u64, BytesMut)>(128);
            tokio::spawn(async move {
                while let Some((port, conn, data)) = write_rx.recv().await {
                    let data_size = data.len();
                    let mut out_data = BytesMut::new();
                    out_data.put_u32_le(port);
                    out_data.put_u64_le(conn);
                    out_data.put(data);
                    if let Err(e) = writer.send(out_data.freeze()).await {
                        error!("Error on sending packets to server, port {}, conn {}, data size {}", port, conn, data_size);
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
                        servs.insert(&serv_id);
                        info!("Added new port {} with serv_id {}", port, serv_id);
                        break;
                    } else if bridge_ports
                        .try_insert(&port_key, Arc::new(HashSet::with_capacity(16)))
                        .is_none()
                    {
                        info!("Added serv_id {} as receipt to port {}", serv_id, port);
                        break;
                    }
                    warn!("Need to retry adding port {} with {}", port, serv_id);
                }
            }
        });
    }
}