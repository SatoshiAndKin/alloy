//! WebSocket duplex, keepalive, and cancellation regressions on real TCP sockets.

use alloy_json_rpc::{Request, ResponsePayload};
use alloy_pubsub::PubSubConnect;
use alloy_transport_ws::WsConnect;
use futures::StreamExt;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::oneshot,
    task::JoinSet,
    time::timeout,
};

#[tokio::test]
async fn reads_response_while_request_write_is_blocked() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (first_tx, first_rx) = oneshot::channel();
    let (drain_tx, drain_rx) = oneshot::channel();
    let second =
        Request::new("second", 2.into(), ["a".repeat(8 * 1024 * 1024)]).serialize().unwrap();
    let result = "b".repeat(8 * 1024 * 1024);
    let response =
        serde_json::to_vec(&serde_json::json!({"jsonrpc":"2.0","id":1,"result":result})).unwrap();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let first = ws.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(first.contains("first"));
        first_tx.send(()).unwrap();
        let mut stream = ws.into_inner();
        let mut header = [0; 14];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(&header[..2], &[0x81, 0xff]);
        let second_len = u64::from_be_bytes(header[2..10].try_into().unwrap()) as usize;
        eprintln!("Server received second request frame header: {second_len} bytes; writing first response before draining it");
        let mut frame = vec![0x81, 0x7f];
        frame.extend_from_slice(&(response.len() as u64).to_be_bytes());
        frame.extend_from_slice(&response);
        stream.write_all(&frame).await.unwrap();
        drain_rx.await.unwrap();
        let mut body = vec![0; second_len];
        stream.read_exact(&mut body).await.unwrap();
        for (i, byte) in body.iter_mut().enumerate() { *byte ^= header[10 + i % 4]; }
        let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(request["method"], "second");
        let response = br#"{"jsonrpc":"2.0","id":2,"result":"0x2"}"#;
        stream.write_all(&[0x81, response.len() as u8]).await.unwrap();
        stream.write_all(response).await.unwrap();
    });
    let frontend =
        WsConnect::new(format!("ws://{address}")).with_max_retries(0).into_service().await.unwrap();
    let first = Request::new("first", 1.into(), ()).serialize().unwrap();
    let exchange = async {
        let first = frontend.send(first);
        tokio::pin!(first);
        tokio::select! {
            value = &mut first => panic!("early response: {value:?}"),
            received = first_rx => received.unwrap(),
        }
        let second = frontend.send(second);
        tokio::pin!(second);
        let first = tokio::select! {
            value = &mut first => value.unwrap(),
            value = &mut second => panic!("second response first: {value:?}"),
        };
        let ResponsePayload::Success(raw) = first.payload else { panic!("expected success") };
        assert_eq!(serde_json::from_str::<String>(raw.get()).unwrap(), result);
        drain_tx.send(()).unwrap();
        assert_eq!(second.await.unwrap().id, 2.into());
        tasks.join_next().await.unwrap().unwrap();
    };
    let started = std::time::Instant::now();
    timeout(Duration::from_secs(5), exchange)
        .await
        .expect("response read blocked by pending request write");
    eprintln!("duplex exchange completed in {:?}", started.elapsed());
}

async fn read_frame(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 2];
    stream.read_exact(&mut header).await.unwrap();
    assert_ne!(header[1] & 0x80, 0, "client frames must be masked");
    let length = match header[1] & 0x7f {
        126 => stream.read_u16().await.unwrap() as usize,
        127 => stream.read_u64().await.unwrap() as usize,
        length => length as usize,
    };
    let mut mask = [0; 4];
    stream.read_exact(&mut mask).await.unwrap();
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    for (i, byte) in body.iter_mut().enumerate() {
        *byte ^= mask[i % 4];
    }
    (header[0] & 0x0f, body)
}

async fn write_small_frame(stream: &mut tokio::net::TcpStream, opcode: u8, body: &[u8]) {
    assert!(body.len() < 126);
    stream.write_all(&[0x80 | opcode, body.len() as u8]).await.unwrap();
    stream.write_all(body).await.unwrap();
}

#[tokio::test]
async fn writes_request_while_response_is_incomplete() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (partial_tx, partial_rx) = oneshot::channel();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = tokio_tungstenite::accept_async(stream).await.unwrap().into_inner();
        let (_, first) = read_frame(&mut stream).await;
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&first).unwrap()["method"], "first");
        let response = br#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#;
        stream.write_all(&[0x81, response.len() as u8]).await.unwrap();
        stream.write_all(&response[..1]).await.unwrap();
        partial_tx.send(()).unwrap();
        let (_, second) = read_frame(&mut stream).await;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&second).unwrap()["method"],
            "second"
        );
        stream.write_all(&response[1..]).await.unwrap();
        write_small_frame(&mut stream, 1, br#"{"jsonrpc":"2.0","id":2,"result":"0x2"}"#).await;
    });
    let frontend =
        WsConnect::new(format!("ws://{address}")).with_max_retries(0).into_service().await.unwrap();
    timeout(Duration::from_secs(2), async {
        let first = frontend.send(Request::new("first", 1.into(), ()).serialize().unwrap());
        tokio::pin!(first);
        tokio::select! {
            response = &mut first => panic!("incomplete response completed: {response:?}"),
            received = partial_rx => received.unwrap(),
        }
        let second = frontend.send(Request::new("second", 2.into(), ()).serialize().unwrap());
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.unwrap().id, 1.into());
        assert_eq!(second.unwrap().id, 2.into());
        tasks.join_next().await.unwrap().unwrap();
    })
    .await
    .expect("incomplete response must not block new requests");
}

#[tokio::test]
async fn server_ping_gets_pong_without_a_request() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = tokio_tungstenite::accept_async(stream).await.unwrap().into_inner();
        write_small_frame(&mut stream, 9, b"probe").await;
        assert_eq!(read_frame(&mut stream).await, (10, b"probe".to_vec()));
    });
    let _frontend =
        WsConnect::new(format!("ws://{address}")).with_max_retries(0).into_service().await.unwrap();
    timeout(Duration::from_secs(2), tasks.join_next()).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn keepalive_pongs_keep_connection_open() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (pongs_tx, pongs_rx) = oneshot::channel();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = tokio_tungstenite::accept_async(stream).await.unwrap().into_inner();
        for _ in 0..3 {
            let (opcode, body) = read_frame(&mut stream).await;
            assert_eq!(opcode, 9);
            write_small_frame(&mut stream, 10, &body).await;
        }
        pongs_tx.send(()).unwrap();
        let (opcode, _) = read_frame(&mut stream).await;
        assert_eq!(opcode, 1);
        write_small_frame(&mut stream, 1, br#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#).await;
    });
    let frontend = WsConnect::new(format!("ws://{address}"))
        .with_max_retries(0)
        .with_keepalive_interval(Duration::from_millis(100))
        .into_service()
        .await
        .unwrap();
    timeout(Duration::from_secs(3), async {
        pongs_rx.await.unwrap();
        assert_eq!(
            frontend
                .send(Request::new("first", 1.into(), ()).serialize().unwrap())
                .await
                .unwrap()
                .id,
            1.into()
        );
        tasks.join_next().await.unwrap().unwrap();
    })
    .await
    .expect("pongs must keep the connection usable");
}

#[tokio::test]
async fn missed_pong_closes_connection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = tokio_tungstenite::accept_async(stream).await.unwrap().into_inner();
        assert_eq!(read_frame(&mut stream).await.0, 9);
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty(), "connection should close after the missed pong");
    });
    let _frontend = WsConnect::new(format!("ws://{address}"))
        .with_max_retries(0)
        .with_keepalive_interval(Duration::from_millis(100))
        .into_service()
        .await
        .unwrap();
    timeout(Duration::from_secs(3), tasks.join_next()).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn close_frame_cancels_blocked_request_write() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release_tx, release_rx) = oneshot::channel();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        // Reject the service reconnect so this test measures transport closure,
        // not a second connection waiting for an unserved handshake.
        drop(listener);
        let mut stream = tokio_tungstenite::accept_async(stream).await.unwrap().into_inner();
        let mut header = [0; 14];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(&header[..2], &[0x81, 0xff]);
        write_small_frame(&mut stream, 8, &[]).await;
        // Keep the read half open without draining the pending request.
        release_rx.await.unwrap();
    });
    let frontend =
        WsConnect::new(format!("ws://{address}")).with_max_retries(0).into_service().await.unwrap();
    let request =
        Request::new("large", 1.into(), ["a".repeat(8 * 1024 * 1024)]).serialize().unwrap();
    assert!(timeout(Duration::from_secs(2), frontend.send(request))
        .await
        .expect("close frame must cancel a blocked send")
        .is_err());
    release_tx.send(()).unwrap();
    tasks.join_next().await.unwrap().unwrap();
}

#[tokio::test]
async fn frontend_shutdown_cancels_blocked_request_write() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (header_tx, header_rx) = oneshot::channel();
    let (drain_tx, drain_rx) = oneshot::channel();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = tokio_tungstenite::accept_async(stream).await.unwrap().into_inner();
        let mut header = [0; 14];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(&header[..2], &[0x81, 0xff]);
        let total = u64::from_be_bytes(header[2..10].try_into().unwrap()) as usize;
        header_tx.send(()).unwrap();
        drain_rx.await.unwrap();
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await.unwrap();
        assert!(rest.len() < total, "shutdown must cancel, not finish the pending request");
    });
    let frontend =
        WsConnect::new(format!("ws://{address}")).with_max_retries(0).into_service().await.unwrap();
    {
        let send = frontend.send(
            Request::new("large", 1.into(), ["a".repeat(8 * 1024 * 1024)]).serialize().unwrap(),
        );
        tokio::pin!(send);
        timeout(Duration::from_secs(2), async {
            tokio::select! {
                value = &mut send => panic!("unexpected early response: {value:?}"),
                received = header_rx => received.unwrap(),
            }
        })
        .await
        .expect("server must receive the pending request header");
    }
    drop(frontend);
    // Let the transport observe closure before the server drains queued bytes.
    tokio::time::sleep(Duration::from_millis(50)).await;
    drain_tx.send(()).unwrap();
    timeout(Duration::from_secs(2), tasks.join_next()).await.unwrap().unwrap().unwrap();
}
