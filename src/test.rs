use std::error::Error;

use crate::{bridge, server::*, utils::FRAME_CAPACITY};
use bytes::{Buf, Bytes};
use futures::{SinkExt, StreamExt};
use log::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{sleep, Duration},
};
use tokio_util::codec::{BytesCodec, Framed};

#[tokio::test(flavor = "multi_thread")]
pub async fn bridge_starts() {
    let _l = env_logger::try_init();
    tokio::spawn(async { bridge::start("127.0.0.1:3664").await.unwrap() });
    let server = Server::new(1);
    server
        .start(vec![(3665, 3667)], "127.0.0.1:3664")
        .await
        .unwrap();
    tokio::spawn(async {
        echo_server(3665).await.unwrap();
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    let socket = TcpStream::connect("127.0.0.1:3667")
        .await
        .expect(&format!("Cannot connect to {}", 3667));
    let transport = Framed::with_capacity(socket, BytesCodec::new(), FRAME_CAPACITY);
    let (mut writer, mut reader) = transport.split();
    for i in 1..1024 {
        let data = vec![i as u8; i];
        writer.send(Bytes::copy_from_slice(&data)).await.unwrap();
        let response = reader.next().await.unwrap().unwrap();
        assert_eq!(
            response.chunk(),
            &data,
            "Data got len {}, expect {}",
            response.len(),
            data.len()
        );
        info!(
            "Received echo message from bridge with size {}, expecting {}",
            response.len(),
            data.len()
        );
    }
}

async fn echo_server(port: u32) -> Result<(), Box<dyn Error>> {
    // Next up we create a TCP listener which will listen for incoming
    // connections. This TCP listener is bound to the address we determined
    // above and must be associated with an event loop.
    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Listening on: {}", addr);

    loop {
        // Asynchronously wait for an inbound socket.
        let (mut socket, _) = listener.accept().await?;

        // And this is where much of the magic of this server happens. We
        // crucially want all clients to make progress concurrently, rather than
        // blocking one on completion of another. To achieve this we use the
        // `tokio::spawn` function to execute the work in the background.
        //
        // Essentially here we're executing a new task to run concurrently,
        // which will allow all of our clients to be processed concurrently.

        tokio::spawn(async move {
            let mut buf = vec![0; 4096];

            // In a loop, read data from the socket and write the data back.
            loop {
                let n = socket
                    .read(&mut buf)
                    .await
                    .expect("failed to read data from socket");

                if n == 0 {
                    return;
                }

                socket
                    .write_all(&buf[0..n])
                    .await
                    .expect("failed to write data to socket");
            }
        });
    }
}
