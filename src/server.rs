use std::{
    cell::RefCell,
    collections::HashMap,
    error::Error,
    mem,
    sync::{atomic::*, Arc},
    time::Duration,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{channel::mpsc::SendError, prelude::*};
use lightning::map::{Map, ObjectMap};
use log::{debug, error, info, trace, warn};
use tokio::{io::AsyncReadExt, stream::*, sync::mpsc::Receiver};
use tokio::{io::AsyncWriteExt, sync::mpsc::Sender};
use tokio::{net::TcpStream, sync::mpsc::channel};
use tokio_util::codec::{BytesCodec, Framed, FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::utils::{unix_timestamp, FRAME_CAPACITY};

type ConnMap = Arc<ObjectMap<Arc<Connection>>>;

pub struct Connection {
    id: u64,
    port: u32,
    recv_pkt: Arc<AtomicUsize>,
    sent_pkt: Arc<AtomicUsize>,
    last_act: Arc<AtomicU64>,
    outgoing_tx: Arc<Sender<(u64, BytesMut)>>,
    host_tx: Sender<Bytes>,
    host_rx: RefCell<Option<Receiver<Bytes>>>,
    act: AtomicBool,
    conn_map: ConnMap,
}

unsafe impl Sync for Connection {}

pub struct Server {
    conns: ConnMap,
}

impl Server {
    pub fn new() -> Self {
        let conns = Arc::new(ObjectMap::with_capacity(128));
        Self { conns }
    }

    pub async fn start<'a>(
        &self,
        ports: Vec<(u32, u32)>,
        bridge: &'a str,
    ) -> Result<(), Box<dyn Error + '_>> {
        info!(
            "Running as server with ports {:?}, bridge {} for each port",
            ports, bridge
        );
        let conns = self.conns.clone();
        let bridge = bridge.to_owned();
        let ports = ports.clone();
        tokio::spawn(async move {
            loop {
                let _ = Self::start_thread(&conns, &ports, &bridge).await;
                conns.entries().into_iter().for_each(|(id, conn)| {
                    tokio::spawn(async move {
                        // Send empty to close the connection
                        warn!("Closing connection {} due to bridge offline", id);
                        conn.host_tx.send(Bytes::new()).await
                    });
                    conns.remove(&id);
                });
                info!("Disconnected, wait for 5 secs to retry...");
                tokio::time::sleep(Duration::from_secs(5)).await;
                info!("Reconnecting...");
            }
        });
        Ok(())
    }

    async fn start_thread(
        conns: &Arc<ObjectMap<Arc<Connection>>>,
        ports: &Vec<(u32, u32)>,
        bridge: &String,
    ) -> Result<(), Box<dyn Error>> {
        let socket = TcpStream::connect(bridge).await?;
        let transport = Framed::with_capacity(socket, LengthDelimitedCodec::new(), FRAME_CAPACITY);
        let (mut writer, mut reader) = transport.split();
        let ports = ports.clone();
        let conns = conns.clone();
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
                    error!("Cannot send port {}, error {}", dest, e);
                }
            }
            writer.flush().await.unwrap();
            trace!("Sent port {}", dest);
        }
        debug!("Reading thread port initialization message");
        let init_res = reader.next().await.unwrap().unwrap();
        debug!("Thread port initialization message received");
        assert_eq!(init_res.chunk(), &1u8.to_le_bytes());
        let (write_tx, mut write_rx) = channel::<(u64, BytesMut)>(1);
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
                writer.flush().await.unwrap();
            }
            let _r = writer.close().await;
            warn!(
                "Bridge disconnected due to channel closed"
            );
        });
        let rev_port = ports
            .iter()
            .map(|(src, dest)| (*dest, *src))
            .collect::<HashMap<_, _>>();
        let out = Arc::new(write_tx);
        while let Some(res) = reader.next().await {
            if let Ok(mut data) = res {
                let dest_port = data.get_u32_le();
                trace!(
                    "Received packet for port {}, length {}",
                    dest_port,
                    data.len()
                );
                let src_port = match rev_port.get(&dest_port) {
                    Some(p) => *p,
                    None => {
                        error!("Cannot find port {}", dest_port);
                        continue;
                    }
                };
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
                        src_port,
                        out.clone(),
                        conns.clone(),
                    ))
                });
                connection.activate().await;
                if data.remaining() > 0 {
                    connection.send_to_host(data.freeze()).await;
                } else {
                    debug!(
                        "Received close instruction for conn {}, port {}",
                        conn_id, dest_port
                    );
                    conns.remove(&(conn_id as usize));
                    connection.send_to_host(data.freeze()).await;
                    debug!("Closed connection for conn {}, port {}", conn_id, dest_port);
                }
            }
        }
        warn!("Bridge connection closed");
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
        let (host_tx, host_rx) = channel::<Bytes>(1);
        let recv_pkt: Arc<AtomicUsize> = Default::default();
        let sent_pkt: Arc<AtomicUsize> = Default::default();
        let last_act: Arc<AtomicU64> = Default::default();
        Self {
            id,
            port,
            recv_pkt,
            sent_pkt,
            last_act,
            outgoing_tx,
            host_tx,
            conn_map,
            host_rx: RefCell::new(Some(host_rx)),
            act: AtomicBool::new(false),
        }
    }

    async fn activate(&self) {
        if self
            .act
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            trace!("Connection {} have already activated", self.id);
            return;
        }
        let recv_pkt_c = self.recv_pkt.clone();
        let sent_pkt_c = self.sent_pkt.clone();
        let last_act_c1 = self.last_act.clone();
        let last_act_c2 = self.last_act.clone();
        let conn_map = self.conn_map.clone();
        let conn_map2 = self.conn_map.clone();
        let out = self.outgoing_tx.clone();
        let port = self.port;
        let id = self.id;
        let mut host_rx = mem::replace(&mut*self.host_rx.borrow_mut(), None).unwrap();
        debug!("Activating server client {}", self.id);
        let socket = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .expect(&format!("Cannot connect to {}", port));
        let transport = Framed::with_capacity(socket, BytesCodec::new(), FRAME_CAPACITY);
        let (mut writer, mut reader) = transport.split();
        tokio::spawn(async move {
            while let Some(data) = host_rx.recv().await {
                let len = data.len();
                if len == 0 {
                    trace!("Received host connection close packet for {}", id);
                    host_rx.close();
                    break;
                }
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
            if let Err(e) = writer.close().await {
                error!(
                    "Failing to close connection to local service {}, {:?}",
                    port, e
                );
            };
            conn_map2.remove(&(id as usize));
            info!("Connection closed for {}, port {}", id, port);
        });
        tokio::spawn(async move {
            loop {
                while let Some(res) = reader.next().await {
                    match res {
                        Ok(bytes) => {
                            trace!(
                            "Received and sending packet to endpoint channel for conn {} port {}, size {}",
                            id,
                            port,
                            bytes.len()
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
                info!(
                    "Server closed its connection for conn {}, port {}",
                    id, port
                );
                let _ = out.send((id, BytesMut::new())).await;
                conn_map.remove(&(id as usize));
                break;
            }
        });
    }

    pub async fn send_to_host(&self, bytes: Bytes) {
        if let Err(e) = self.host_tx.send(bytes).await {
            error!("Error to send to host channel {}, {:?}", self.id, e)
        }
    }
}
