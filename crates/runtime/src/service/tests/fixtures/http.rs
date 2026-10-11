//! Local HTTP echo server for host-fetch and hardening tests.
use super::*;

pub(crate) async fn echo_requests(
    listener: TcpListener,
    count: usize,
    (header_key, header_name): (&str, &str),
    require_content_length: bool,
) {
    for _ in 0..count {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 2048];
        let header_end = loop {
            let length = socket.read(&mut buffer).await.unwrap();
            assert!(length > 0, "request ended before its headers");
            request.extend_from_slice(&buffer[..length]);
            assert!(request.len() <= 16 * 1024);
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers = String::from_utf8(request[..header_end].to_vec()).unwrap();
        let header = |name: &str| {
            headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.trim().to_string())
        };
        let content_length = header("content-length");
        if require_content_length {
            assert!(content_length.is_some(), "request must have content-length");
        }
        let body_length = content_length.map_or(0, |value| value.parse::<usize>().unwrap());
        assert!(body_length <= 4096);
        while request.len() < header_end + body_length {
            let length = socket.read(&mut buffer).await.unwrap();
            assert!(length > 0, "request ended before its body");
            request.extend_from_slice(&buffer[..length]);
        }
        let body = serde_json::to_vec(&serde_json::json!({
            "method": headers.split_whitespace().next().unwrap(),
            (header_key): header(header_name),
            "body": String::from_utf8(request[header_end..header_end + body_length].to_vec()).unwrap(),
        })).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
    }
}
