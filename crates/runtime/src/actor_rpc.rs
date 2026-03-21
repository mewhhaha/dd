use crate::actor_rpc_capnp;
use common::{PlatformError, Result, WorkerInvocation, WorkerOutput};
use std::io::Cursor;

#[derive(Debug, Clone)]
pub struct ActorInvokeRequest {
    pub worker_name: String,
    pub binding: String,
    pub key: String,
    pub call: ActorInvokeCall,
}

#[derive(Debug, Clone)]
pub enum ActorInvokeCall {
    Fetch(WorkerInvocation),
    Method {
        name: String,
        args: Vec<u8>,
        request_id: String,
    },
}

#[derive(Debug, Clone)]
pub enum ActorInvokeResponse {
    Fetch(WorkerOutput),
    Method { value: Vec<u8> },
    Error(String),
}

pub fn encode_actor_invoke_request(request: &ActorInvokeRequest) -> Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    let mut root = message.init_root::<actor_rpc_capnp::invoke_request::Builder<'_>>();
    {
        let mut target = root.reborrow().init_target();
        target.set_worker_name(&request.worker_name);
        target.set_binding(&request.binding);
        target.set_key(&request.key);
    }
    match &request.call {
        ActorInvokeCall::Fetch(fetch) => {
            let mut call = root.reborrow().init_fetch();
            call.set_method(&fetch.method);
            call.set_url(&fetch.url);
            call.set_body(fetch.body.as_slice());
            call.set_request_id(&fetch.request_id);
            let mut headers = call.reborrow().init_headers(fetch.headers.len() as u32);
            for (idx, (name, value)) in fetch.headers.iter().enumerate() {
                let mut entry = headers.reborrow().get(idx as u32);
                entry.set_name(name);
                entry.set_value(value);
            }
        }
        ActorInvokeCall::Method {
            name,
            args,
            request_id,
        } => {
            let mut call = root.reborrow().init_method();
            call.set_name(name);
            call.set_args(args.as_slice());
            call.set_request_id(request_id);
        }
    }
    let mut out = Vec::new();
    capnp::serialize_packed::write_message(&mut out, &message).map_err(actor_rpc_error)?;
    Ok(out)
}

pub fn decode_actor_invoke_request(frame: &[u8]) -> Result<ActorInvokeRequest> {
    let mut cursor = Cursor::new(frame);
    let message =
        capnp::serialize_packed::read_message(&mut cursor, capnp::message::ReaderOptions::new())
            .map_err(actor_rpc_error)?;
    let root = message
        .get_root::<actor_rpc_capnp::invoke_request::Reader<'_>>()
        .map_err(actor_rpc_error)?;
    let target = root.get_target().map_err(actor_rpc_error)?;
    let worker_name = read_text(target.get_worker_name().map_err(actor_rpc_error)?)?;
    let binding = read_text(target.get_binding().map_err(actor_rpc_error)?)?;
    let key = read_text(target.get_key().map_err(actor_rpc_error)?)?;
    let call = match root.which().map_err(actor_rpc_error)? {
        actor_rpc_capnp::invoke_request::Fetch(fetch) => {
            let fetch = fetch.map_err(actor_rpc_error)?;
            let mut headers = Vec::new();
            let header_reader = fetch.get_headers().map_err(actor_rpc_error)?;
            for entry in header_reader.iter() {
                let name = read_text(entry.get_name().map_err(actor_rpc_error)?)?;
                let value = read_text(entry.get_value().map_err(actor_rpc_error)?)?;
                headers.push((name, value));
            }
            ActorInvokeCall::Fetch(WorkerInvocation {
                method: read_text(fetch.get_method().map_err(actor_rpc_error)?)?,
                url: read_text(fetch.get_url().map_err(actor_rpc_error)?)?,
                headers,
                body: fetch.get_body().map_err(actor_rpc_error)?.to_vec(),
                request_id: read_text(fetch.get_request_id().map_err(actor_rpc_error)?)?,
            })
        }
        actor_rpc_capnp::invoke_request::Method(call) => {
            let call = call.map_err(actor_rpc_error)?;
            ActorInvokeCall::Method {
                name: read_text(call.get_name().map_err(actor_rpc_error)?)?,
                args: call.get_args().map_err(actor_rpc_error)?.to_vec(),
                request_id: read_text(call.get_request_id().map_err(actor_rpc_error)?)?,
            }
        }
    };
    Ok(ActorInvokeRequest {
        worker_name,
        binding,
        key,
        call,
    })
}

pub fn encode_actor_invoke_response(response: &ActorInvokeResponse) -> Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    let mut root = message.init_root::<actor_rpc_capnp::invoke_response::Builder<'_>>();
    match response {
        ActorInvokeResponse::Fetch(output) => {
            let mut fetch = root.reborrow().init_fetch();
            fetch.set_status(output.status);
            fetch.set_body(output.body.as_slice());
            let mut headers = fetch.reborrow().init_headers(output.headers.len() as u32);
            for (idx, (name, value)) in output.headers.iter().enumerate() {
                let mut entry = headers.reborrow().get(idx as u32);
                entry.set_name(name);
                entry.set_value(value);
            }
        }
        ActorInvokeResponse::Method { value } => {
            root.reborrow().init_method().set_value(value.as_slice());
        }
        ActorInvokeResponse::Error(error) => {
            root.set_error(error);
        }
    }
    let mut out = Vec::new();
    capnp::serialize_packed::write_message(&mut out, &message).map_err(actor_rpc_error)?;
    Ok(out)
}

pub fn decode_actor_invoke_response(frame: &[u8]) -> Result<ActorInvokeResponse> {
    let mut cursor = Cursor::new(frame);
    let message =
        capnp::serialize_packed::read_message(&mut cursor, capnp::message::ReaderOptions::new())
            .map_err(actor_rpc_error)?;
    let root = message
        .get_root::<actor_rpc_capnp::invoke_response::Reader<'_>>()
        .map_err(actor_rpc_error)?;
    match root.which().map_err(actor_rpc_error)? {
        actor_rpc_capnp::invoke_response::Fetch(fetch) => {
            let fetch = fetch.map_err(actor_rpc_error)?;
            let mut headers = Vec::new();
            let header_reader = fetch.get_headers().map_err(actor_rpc_error)?;
            for entry in header_reader.iter() {
                headers.push((
                    read_text(entry.get_name().map_err(actor_rpc_error)?)?,
                    read_text(entry.get_value().map_err(actor_rpc_error)?)?,
                ));
            }
            Ok(ActorInvokeResponse::Fetch(WorkerOutput {
                status: fetch.get_status(),
                headers,
                body: fetch.get_body().map_err(actor_rpc_error)?.to_vec(),
            }))
        }
        actor_rpc_capnp::invoke_response::Method(method) => {
            let method = method.map_err(actor_rpc_error)?;
            Ok(ActorInvokeResponse::Method {
                value: method.get_value().map_err(actor_rpc_error)?.to_vec(),
            })
        }
        actor_rpc_capnp::invoke_response::Error(error) => Ok(ActorInvokeResponse::Error(
            read_text(error.map_err(actor_rpc_error)?)?,
        )),
    }
}

fn actor_rpc_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(format!("actor rpc capnp error: {error}"))
}

fn read_text(reader: capnp::text::Reader<'_>) -> Result<String> {
    reader
        .to_str()
        .map(|value| value.to_owned())
        .map_err(actor_rpc_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_request_roundtrip() {
        let request = ActorInvokeRequest {
            worker_name: "api".to_string(),
            binding: "USERS".to_string(),
            key: "user-1".to_string(),
            call: ActorInvokeCall::Fetch(WorkerInvocation {
                method: "POST".to_string(),
                url: "https://actor/work".to_string(),
                headers: vec![("x-foo".to_string(), "bar".to_string())],
                body: vec![1, 2, 3, 4],
                request_id: "req-1".to_string(),
            }),
        };
        let encoded = encode_actor_invoke_request(&request).expect("request should encode");
        let decoded = decode_actor_invoke_request(&encoded).expect("request should decode");
        assert_eq!(decoded.worker_name, request.worker_name);
        assert_eq!(decoded.binding, request.binding);
        assert_eq!(decoded.key, request.key);
        match decoded.call {
            ActorInvokeCall::Fetch(inner) => {
                if let ActorInvokeCall::Fetch(expected) = request.call {
                    assert_eq!(inner.method, expected.method);
                    assert_eq!(inner.url, expected.url);
                    assert_eq!(inner.headers, expected.headers);
                    assert_eq!(inner.body, expected.body);
                    assert_eq!(inner.request_id, expected.request_id);
                } else {
                    panic!("expected fetch call");
                }
            }
            _ => panic!("expected fetch call"),
        }
    }

    #[test]
    fn method_response_roundtrip() {
        let response = ActorInvokeResponse::Method {
            value: vec![9, 8, 7],
        };
        let encoded = encode_actor_invoke_response(&response).expect("response should encode");
        let decoded = decode_actor_invoke_response(&encoded).expect("response should decode");
        match decoded {
            ActorInvokeResponse::Method { value } => assert_eq!(value, vec![9, 8, 7]),
            _ => panic!("expected method response"),
        }
    }
}
