use super::*;

pub(crate) fn test_invocation() -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: "http://worker/".to_string(),
        headers: Vec::new(),
        body: Vec::new(),
        request_id: "test-request".to_string(),
    }
}

pub(crate) fn test_invocation_with_path(path: &str, request_id: &str) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: format!("http://worker{path}"),
        headers: Vec::new(),
        body: Vec::new(),
        request_id: request_id.to_string(),
    }
}

pub(crate) fn test_websocket_invocation(path: &str, request_id: &str) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: format!("http://worker{path}"),
        headers: vec![
            ("connection".to_string(), "Upgrade".to_string()),
            ("upgrade".to_string(), "websocket".to_string()),
            ("sec-websocket-version".to_string(), "13".to_string()),
            (
                "sec-websocket-key".to_string(),
                "dGhlIHNhbXBsZSBub25jZQ==".to_string(),
            ),
        ],
        body: Vec::new(),
        request_id: request_id.to_string(),
    }
}

pub(crate) fn test_transport_invocation() -> WorkerInvocation {
    WorkerInvocation {
        method: "CONNECT".to_string(),
        url: "http://worker/session".to_string(),
        headers: vec![(
            "x-dd-transport-protocol".to_string(),
            "webtransport".to_string(),
        )],
        body: Vec::new(),
        request_id: "test-transport-request".to_string(),
    }
}
