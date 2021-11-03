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

use crate::utils::unix_timestamp;

type ConnMap = Arc<ObjectMap<Arc<Connection>>>;

pub struct Connection {
    id: u64,
    port: u32,
    recv_pkt: Arc<AtomicUsize>,
    sent_pkt: Arc<AtomicUsize>,
    last_act: Arc<AtomicU64>,
    outgoing_tx: Arc<Sender<(u64, BytesMut)>>,
    host_tx: Sender<Bytes>,
}

pub struct Server {
    threads: u32,
    conns: ConnMap,
}

impl Server {
    pub fn new(threads: u32) -> Self {
        let conns = Arc::new(ObjectMap::with_capacity(
            threads.next_power_of_two() as usize
        ));
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
            writer
                .send(Bytes::copy_from_slice(&(ports.len() as u64).to_le_bytes()))
                .await
                .unwrap(); // Send length of ports
            debug!("Reading thread initialization message");
            let init_res = reader.next().await.unwrap().unwrap();
            debug!("Thread initialization message received");
            assert_eq!(init_res.chunk(), &ports.len().to_le_bytes());
            for (_src, dest) in &ports {
                match writer
                    .send(Bytes::copy_from_slice(&dest.to_le_bytes()))
                    .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Cannot send port {} for thread {}, error {}", dest, tid, e);
                    }
                }
                writer.flush().await.unwrap();
                trace!("Sent port {} for thread {}", dest, tid);
            }
            debug!("Reading thread port initialization message");
            let init_res = reader.next().await.unwrap().unwrap();
            debug!("Thread port initialization message received");
            assert_eq!(init_res.chunk(), &1u8.to_le_bytes());
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
                warn!(
                    "Bridge disconnected for thread {} due to channel closed",
                    tid
                );
            });
            let rev_port = ports
                .iter()
                .map(|(src, dest)| (*dest, *src))
                .collect::<HashMap<_, _>>();
            let out = Arc::new(write_tx);
            tokio::spawn(async move {
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
                            Arc::new(Connection::new(
                                conn_id,
                                *src_port,
                                out.clone(),
                                conns.clone(),
                            ))
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
    pub fn new(
        id: u64,
        port: u32,
        outgoing_tx: Arc<Sender<(u64, BytesMut)>>,
        conn_map: ConnMap,
    ) -> Self {
        let out = outgoing_tx.clone();
        let (host_tx, mut host_rx) = channel::<Bytes>(64);
        let recv_pkt: Arc<AtomicUsize> = Default::default();
        let sent_pkt: Arc<AtomicUsize> = Default::default();
        let last_act: Arc<AtomicU64> = Default::default();

        let recv_pkt_c = recv_pkt.clone();
        let sent_pkt_c = sent_pkt.clone();
        let last_act_c1 = last_act.clone();
        let last_act_c2 = last_act.clone();
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
                            sent_pkt_c.fetch_add(1, Ordering::Relaxed);
                            last_act_c1.store(unix_timestamp(), Ordering::Relaxed);
                        }
                        Err(e) => {
                            error!(
                                "Failing to send packet with length of {} to {}, {:?}",
                                len, port, e
                            )
                        }
                    }
                }
                info!("Closing connection {}, port {}", id, port);
                if let Err(e) = writer.close().await {
                    error!(
                        "Failing to close connection to local service {}, {:?}",
                        port, e
                    );
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
                            recv_pkt_c.fetch_add(1, Ordering::Relaxed);
                            last_act_c2.store(unix_timestamp(), Ordering::Relaxed);
                        }
                        Err(e) => {
                            error!("Cannot read from local service {}, Error {:?}", port, e);
                            break;
                        }
                    }
                }
                conn_map.remove(&(id as usize));
            }
        });
        Self {
            id,
            port,
            recv_pkt,
            sent_pkt,
            last_act,
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
