use std::{
    collections::HashMap,
    error::Error,
    sync::{atomic::*, Arc},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{channel::mpsc::SendError, prelude::*};
use lightning::map::{Map, ObjectMap};
use log::{debug, error, info, trace, warn};
use tokio::{io::AsyncReadExt, stream::*};
use tokio::{io::AsyncWriteExt, sync::mpsc::Sender};
use tokio::{net::TcpStream, sync::mpsc::channel};
use tokio_util::codec::{BytesCodec, Framed, FramedRead, FramedWrite};

pub struct Connection {
    id: u64,
    port: u32,
    recv_pkt: AtomicUsize,
    sent_pkt: AtomicUsize,
    last_act: AtomicU64,
    outgoing_tx: Arc<Sender<(u64, BytesMut)>>,
    host_tx: Sender<Bytes>,
}

pub struct Server {
    threads: u32,
    conns: Arc<ObjectMap<Arc<Connection>>>,
}

impl Server {
    pub fn new(threads: u32) -> Self {
        let conns = Arc::new(ObjectMap::with_capacity(threads as usize));
        Self { conns, threads }
    }

    pub async fn start<'a>(
        &self,
        ports: Vec<(u32, u32)>,
        bridge: &'a str,
    ) -> Result<(), Box<dyn Error + '_>> {
        info!(
            "Running as server with ports {:?}, bridge {} and {} threads for each port",
            ports, bridge, self.threads
        );
        for tid in 0..self.threads {
            let socket = TcpStream::connect(bridge).await?;
            let transport = Framed::new(socket, BytesCodec::new());
            let (mut writer, mut reader) = transport.split();
            let ports = ports.clone();
            let conns = self.conns.clone();
            tokio::spawn(async move {
                writer
                    .send(Bytes::copy_from_slice(&ports.len().to_le_bytes()))
                    .await
                    .unwrap(); // Send length of ports
                for (_src, dest) in &ports {
                    writer
                        .send(Bytes::copy_from_slice(&dest.to_le_bytes()))
                        .await
                        .unwrap(); // Send destination ports
                }
                let (write_tx, mut write_rx) = channel::<(u64, BytesMut)>(128);
                tokio::spawn(async move {
                    while let Some((conn, data)) = write_rx.recv().await {
                        let data_len = data.len();
                        let mut buf = BytesMut::with_capacity(data_len + 8);
                        buf.put_u64_le(conn);
                        buf.put(data);
                        if let Err(e) = writer.send(buf.freeze()).await {
                            error!(
                                "Error on sending packet to connection {}, size {}, error {}",
                                conn, data_len, e
                            );
                        }
                    }
                    let _r = writer.close().await;
                    warn!("Bridge disconnected for thread {} due to channel closed", tid);
                });
                let rev_port = ports
                    .iter()
                    .map(|(src, dest)| (*dest, *src))
                    .collect::<HashMap<_, _>>();
                let out = Arc::new(write_tx);
                while let Some(res) = reader.next().await {
                    if let Ok(mut data) = res {
                        let dest_port = data.get_u32_le();
                        let src_port = rev_port.get(&dest_port).unwrap();
                        let conn_id = data.get_u64_le();
                        trace!(
                            "Received packet from bridge for {}, conn {}, size {}, sending to {}",
                            dest_port,
                            conn_id,
                            data.len(),
                            src_port
                        );
                        let connection = conns.get_or_insert(&(conn_id as usize), || {
                            Arc::new(Connection::new(conn_id, *src_port, out.clone()))
                        });
                        connection.send_to_host(data.freeze()).await;
                    }
                }
            });
        }
        Ok(())
    }
}

impl Connection {
    pub fn new(id: u64, port: u32, outgoing_tx: Arc<Sender<(u64, BytesMut)>>) -> Self {
        let out = outgoing_tx.clone();
        let (host_tx, mut host_rx) = channel::<Bytes>(64);
        tokio::spawn(async move {
            let socket = TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .expect(&format!("Cannot connect to {}", port));
            let transport = Framed::new(socket, BytesCodec::new());
            let (mut writer, mut reader) = transport.split();
            tokio::spawn(async move {
                while let Some(data) = host_rx.recv().await {
                    let len = data.len();
                    match writer.send(data).await {
                        Ok(_) => {
                            trace!("Sent packet with length of {} to {}", len, port);
                        },
                        Err(e) => {
                            error!("Failing to send packet with length of {} to {}, {:?}", len, port, e)
                        }
                    }
                }
                info!("Closing connection {}, port {}", id, port);
                if let Err(e) = writer.close().await {
                    error!("Failing to close connection to local service {}, {:?}", port, e);
                };
            });
            loop {
                while let Some(res) = reader.next().await {
                    match res {
                        Ok(bytes) => {
                            trace!(
                                "Received and sending packet to endpoint channel for conn {} port {}",
                                id,
                                port
                            );
                            if let Err(e) = out.send((id, bytes)).await {
                                error!("Error on sending to endpoint channel: {:?}", e);
                            }
                        }
                        Err(e) => {
                            error!("Cannot read from local service {}, Error {:?}", port, e);
                            break;
                        }
                    }
                }
            }
        });
        Self {
            id,
            port,
            recv_pkt: Default::default(),
            sent_pkt: Default::default(),
            last_act: Default::default(),
            outgoing_tx,
            host_tx,
        }
    }

    pub async fn send_to_host(&self, bytes: Bytes) {
        if let Err(e) = self.host_tx.send(bytes).await {
            error!("Error to send to host channel {}, {:?}", self.id, e)
        }
    }
}
