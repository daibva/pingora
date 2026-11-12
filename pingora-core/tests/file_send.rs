// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#![cfg(all(feature = "file-send", target_os = "linux"))]

use pingora_core::protocols::{
    http::{file::FileRegion, v1::server::HttpSession},
    l4::stream::Stream,
};
use pingora_http::ResponseHeader;
use std::{fs::File, io::Write, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

struct Object(PathBuf);
impl Object {
    fn new(body: &[u8]) -> Self {
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("pingora-file-{}-{seq}", std::process::id()));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.write_all(body).unwrap();
        Self(path)
    }
    fn open(&self) -> Arc<File> {
        Arc::new(File::open(&self.0).unwrap())
    }
}
impl Drop for Object {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

async fn response(client: &mut TcpStream) -> Vec<u8> {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        header.push(client.read_u8().await.unwrap());
        assert!(header.len() < 8192);
    }
    let header = String::from_utf8(header).unwrap();
    assert!(header.starts_with("HTTP/1.1 200"), "{header}");
    let len: usize = header
        .lines()
        .find_map(|s| {
            s.to_ascii_lowercase()
                .strip_prefix("content-length: ")
                .map(|v| v.parse().unwrap())
        })
        .unwrap();
    let mut body = vec![0; len];
    client.read_exact(&mut body).await.unwrap();
    body
}

async fn send(session: &mut HttpSession, region: FileRegion) {
    let mut header = ResponseHeader::build(200, None).unwrap();
    header
        .insert_header("content-length", region.len.to_string())
        .unwrap();
    session
        .write_response_header(Box::new(header))
        .await
        .unwrap();
    let mut sent = 0;
    while sent < region.len {
        let n = session
            .write_response_file(FileRegion {
                file: region.file.clone(),
                offset: region.offset + sent as u64,
                len: region.len - sent,
                lease: None,
            })
            .await
            .unwrap()
            .expect("plaintext Linux TCP should support sendfile");
        assert!(n > 0 && n <= pingora_core::protocols::http::file::FILE_SEND_QUANTUM);
        sent += n;
    }
    session.finish_body().await.unwrap();
    assert_eq!(session.body_bytes_sent(), region.len);
    assert_eq!(session.file_bytes_sent(), region.len);
}

#[tokio::test]
async fn file_header_order_offsets_unlink_and_keepalive() {
    let body: Vec<u8> = (0..700_013).map(|i| (i % 251) as u8).collect();
    let object = Object::new(&body);
    let file = object.open();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    // Unlink must not invalidate an already acquired immutable reader.
    std::fs::remove_file(&object.0).unwrap();
    let expected = body[123..650_123].to_vec();
    let server = tokio::spawn(async move {
        let mut session = HttpSession::new(Box::new(Stream::from(socket)));
        session.read_request().await.unwrap().unwrap();
        send(
            &mut session,
            FileRegion::new(file.clone(), 123, 650_000).unwrap(),
        )
        .await;
        let reused = session.reuse().await.unwrap().unwrap();
        let (stream, _) = reused.into_parts();
        let mut session = HttpSession::new(stream);
        session.read_request().await.unwrap().unwrap();
        send(&mut session, FileRegion::new(file, 0, 17).unwrap()).await;
    });
    client
        .write_all(b"GET /a HTTP/1.1\r\nHost: test\r\n\r\n")
        .await
        .unwrap();
    assert_eq!(response(&mut client).await, expected);
    client
        .write_all(b"GET /b HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    assert_eq!(response(&mut client).await, body[..17]);
    server.await.unwrap();
}

#[test]
fn file_bounds_are_checked() {
    let object = Object::new(b"hello");
    assert!(FileRegion::new(object.open(), 1, 4).is_ok());
    assert!(FileRegion::new(object.open(), 1, 5).is_err());
    assert!(FileRegion::new(object.open(), u64::MAX, 1).is_err());
}

#[test]
fn modules_must_explicitly_allow_file_delivery() {
    use pingora_core::modules::http::{
        compression::ResponseCompressionBuilder, HttpModule, HttpModuleCtx,
    };
    struct Unknown;
    #[async_trait::async_trait]
    impl HttpModule for Unknown {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }
    assert!(!Unknown.supports_response_file());
    assert!(HttpModuleCtx::empty().supports_response_file());
    let disabled = ResponseCompressionBuilder::enable(0).init();
    assert!(disabled.supports_response_file());
    let enabled = ResponseCompressionBuilder::enable(6).init();
    assert!(!enabled.supports_response_file());
}

#[tokio::test]
async fn non_tcp_transport_falls_back() {
    let mut session = HttpSession::new(Box::new(std::io::Cursor::new(Vec::<u8>::new())));
    assert!(!session.supports_response_file());
    let object = Object::new(b"hello");
    assert!(session
        .write_response_file(FileRegion::new(object.open(), 0, 5).unwrap())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn cancelled_file_response_cannot_be_reused() {
    let object = Object::new(&vec![7; 4 * 1024 * 1024]);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    nix::sys::socket::setsockopt(&socket, nix::sys::socket::sockopt::SndBuf, &4096).unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: test\r\n\r\n")
        .await
        .unwrap();
    let mut session = HttpSession::new(Box::new(Stream::from(socket)));
    session.read_request().await.unwrap().unwrap();
    let mut header = ResponseHeader::build(200, None).unwrap();
    header.insert_header("content-length", "4194304").unwrap();
    session
        .write_response_header(Box::new(header))
        .await
        .unwrap();
    let file = object.open();
    let result = tokio::time::timeout(Duration::from_millis(50), async {
        let mut sent = 0;
        while sent < 4 * 1024 * 1024 {
            sent += session
                .write_response_file(FileRegion {
                    file: file.clone(),
                    offset: sent as u64,
                    len: 4 * 1024 * 1024 - sent,
                    lease: None,
                })
                .await
                .unwrap()
                .unwrap();
        }
    })
    .await;
    assert!(
        result.is_err(),
        "client never reads, bounded socket must eventually block"
    );
    assert!(session.reuse().await.unwrap().is_none());
}

#[tokio::test]
async fn partial_file_then_buffered_suffix_preserves_length_and_order() {
    let length = 2 * 1024 * 1024 + 37;
    let body: Vec<_> = (0..length).map(|i| (i % 251) as u8).collect();
    let object = Object::new(&body);
    let file = object.open();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    let server = tokio::spawn(async move {
        let mut session = HttpSession::new(Box::new(Stream::from(socket)));
        session.read_request().await.unwrap().unwrap();
        let mut header = ResponseHeader::build(200, None).unwrap();
        header
            .insert_header("content-length", length.to_string())
            .unwrap();
        session
            .write_response_header(Box::new(header))
            .await
            .unwrap();
        let n = session
            .write_response_file(FileRegion::new(file, 0, length).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert!(n > 0 && n <= pingora_core::protocols::http::file::FILE_SEND_QUANTUM);
        let suffix: Vec<_> = (n..length).map(|i| (i % 251) as u8).collect();
        session.write_body(&suffix).await.unwrap();
        session.finish_body().await.unwrap();
        assert_eq!(session.body_bytes_sent(), length);
        assert_eq!(session.file_bytes_sent(), n);
        assert!(session.reuse().await.unwrap().is_some());
    });
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: test\r\n\r\n")
        .await
        .unwrap();
    assert_eq!(response(&mut client).await, body);
    server.await.unwrap();
}

#[tokio::test]
async fn truncated_file_fails_without_reusing_connection() {
    let object = Object::new(b"short");
    let file = object.open();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: test\r\n\r\n")
        .await
        .unwrap();
    let mut session = HttpSession::new(Box::new(Stream::from(socket)));
    session.read_request().await.unwrap().unwrap();
    let mut header = ResponseHeader::build(200, None).unwrap();
    header.insert_header("content-length", "10").unwrap();
    session
        .write_response_header(Box::new(header))
        .await
        .unwrap();
    let n = session
        .write_response_file(FileRegion {
            file: file.clone(),
            offset: 0,
            len: 10,
            lease: None,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 5);
    assert!(session
        .write_response_file(FileRegion {
            file,
            offset: 5,
            len: 5,
            lease: None,
        })
        .await
        .is_err());
    assert!(session.reuse().await.unwrap().is_none());
}

#[tokio::test]
async fn slow_socket_releases_slot_and_keeps_reader_lease_until_cancel() {
    let object = Object::new(&vec![17; 4 * 1024 * 1024]);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    nix::sys::socket::setsockopt(&socket, nix::sys::socket::sockopt::SndBuf, &4096).unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: test\r\n\r\n")
        .await
        .unwrap();
    let lease = Arc::new(());
    let weak = Arc::downgrade(&lease);
    let mut region = FileRegion::new(object.open(), 0, 4 * 1024 * 1024).unwrap();
    region.lease = Some(lease);
    let (started, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut session = HttpSession::new(Box::new(Stream::from(socket)));
        session.read_request().await.unwrap().unwrap();
        let mut header = ResponseHeader::build(200, None).unwrap();
        header
            .insert_header("content-length", region.len.to_string())
            .unwrap();
        session
            .write_response_header(Box::new(header))
            .await
            .unwrap();
        let _ = started.send(());
        loop {
            let n = session
                .write_response_file(region.clone())
                .await
                .unwrap()
                .unwrap();
            region.offset += n as u64;
            region.len -= n;
            if region.len == 0 {
                break;
            }
        }
    });
    ready.await.unwrap();
    // 客户端故意不读取，发送最终应停在 writable 等待，而不是持槽等待网络。
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!task.is_finished());
    assert!(weak.upgrade().is_some());
    // 另一条正常连接必须还能拿到槽并完成，避免测试依赖全局指标快照。
    let mut fast = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    let fast_server = tokio::spawn(async move {
        let mut session = HttpSession::new(Box::new(Stream::from(socket)));
        session.read_request().await.unwrap().unwrap();
        send(&mut session, FileRegion::new(object.open(), 0, 7).unwrap()).await;
    });
    fast.write_all(b"GET / HTTP/1.1\r\nHost: test\r\n\r\n")
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), response(&mut fast))
            .await
            .unwrap(),
        vec![17; 7]
    );
    fast_server.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    // 已运行的 blocking job 不能被 abort；最后一个租约必须随真实任务释放。
    tokio::time::timeout(Duration::from_secs(2), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut rest = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut rest))
        .await
        .unwrap()
        .unwrap();
}
