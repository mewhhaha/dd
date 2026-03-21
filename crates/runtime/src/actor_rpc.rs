use common::{PlatformError, Result, WorkerInvocation, WorkerOutput};
use std::io::Cursor;

const MAGIC: &[u8; 7] = b"ddacpr1";

pub struct ActorInvokeRequest {
    pub worker_name: String,
    pub binding: String,
    pub key: String,
    pub request: WorkerInvocation,
}

pub fn encode_actor_invoke_request(request: &ActorInvokeRequest) -> Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(512);
    payload.extend_from_slice(MAGIC);
    write_string(&mut payload, &request.worker_name);
    write_string(&mut payload, &request.binding);
    write_string(&mut payload, &request.key);
    write_string(&mut payload, &request.request.method);
    write_string(&mut payload, &request.request.url);
    write_headers(&mut payload, &request.request.headers);
    write_bytes(&mut payload, &request.request.body);
    write_string(&mut payload, &request.request.request_id);
    encode_capnp_data(payload)
}

pub fn decode_actor_invoke_request(frame: &[u8]) -> Result<ActorInvokeRequest> {
    let payload = decode_capnp_data(frame)?;
    let mut reader = SliceReader::new(&payload);
    reader.expect_magic(MAGIC)?;
    let worker_name = reader.read_string()?;
    let binding = reader.read_string()?;
    let key = reader.read_string()?;
    let method = reader.read_string()?;
    let url = reader.read_string()?;
    let headers = reader.read_headers()?;
    let body = reader.read_bytes()?;
    let request_id = reader.read_string()?;
    reader.expect_eof()?;
    Ok(ActorInvokeRequest {
        worker_name,
        binding,
        key,
        request: WorkerInvocation {
            method,
            url,
            headers,
            body,
            request_id,
        },
    })
}

pub fn encode_actor_invoke_response(output: &WorkerOutput) -> Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(256 + output.body.len());
    payload.extend_from_slice(MAGIC);
    payload.extend_from_slice(&output.status.to_le_bytes());
    write_headers(&mut payload, &output.headers);
    write_bytes(&mut payload, &output.body);
    encode_capnp_data(payload)
}

pub fn decode_actor_invoke_response(frame: &[u8]) -> Result<WorkerOutput> {
    let payload = decode_capnp_data(frame)?;
    let mut reader = SliceReader::new(&payload);
    reader.expect_magic(MAGIC)?;
    let status = reader.read_u16()?;
    let headers = reader.read_headers()?;
    let body = reader.read_bytes()?;
    reader.expect_eof()?;
    Ok(WorkerOutput {
        status,
        headers,
        body,
    })
}

fn encode_capnp_data(payload: Vec<u8>) -> Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    message
        .set_root::<capnp::data::Owned>(payload.as_slice())
        .map_err(actor_rpc_error)?;
    let mut out = Vec::with_capacity(payload.len() + 64);
    capnp::serialize_packed::write_message(&mut out, &message).map_err(actor_rpc_error)?;
    Ok(out)
}

fn decode_capnp_data(frame: &[u8]) -> Result<Vec<u8>> {
    let mut cursor = Cursor::new(frame);
    let reader =
        capnp::serialize_packed::read_message(&mut cursor, capnp::message::ReaderOptions::new())
            .map_err(actor_rpc_error)?;
    let data = reader
        .get_root::<capnp::data::Reader<'_>>()
        .map_err(actor_rpc_error)?;
    Ok(data.to_vec())
}

fn write_string(out: &mut Vec<u8>, value: &str) {
    write_bytes(out, value.as_bytes());
}

fn write_headers(out: &mut Vec<u8>, headers: &[(String, String)]) {
    out.extend_from_slice(&(headers.len() as u32).to_le_bytes());
    for (name, value) in headers {
        write_string(out, name);
        write_string(out, value);
    }
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

struct SliceReader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> SliceReader<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn expect_magic(&mut self, expected: &[u8]) -> Result<()> {
        let found = self.read_exact(expected.len())?;
        if found == expected {
            Ok(())
        } else {
            Err(PlatformError::runtime(
                "actor rpc capnp frame has invalid magic",
            ))
        }
    }

    fn read_headers(&mut self) -> Result<Vec<(String, String)>> {
        let count = self.read_u32()? as usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            out.push((self.read_string()?, self.read_string()?));
        }
        Ok(out)
    }

    fn read_string(&mut self) -> Result<String> {
        let bytes = self.read_bytes()?;
        String::from_utf8(bytes).map_err(|error| {
            PlatformError::runtime(format!("actor rpc utf8 decode failed: {error}"))
        })
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        Ok(bytes.to_vec())
    }

    fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| PlatformError::runtime("actor rpc frame offset overflow"))?;
        if end > self.input.len() {
            return Err(PlatformError::runtime(
                "actor rpc capnp frame truncated while decoding",
            ));
        }
        let out = &self.input[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn expect_eof(&self) -> Result<()> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(PlatformError::runtime(
                "actor rpc capnp frame has trailing bytes",
            ))
        }
    }
}

fn actor_rpc_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(format!("actor rpc capnp error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_invoke_request_roundtrip() {
        let request = ActorInvokeRequest {
            worker_name: "api".to_string(),
            binding: "USERS".to_string(),
            key: "user-1".to_string(),
            request: WorkerInvocation {
                method: "POST".to_string(),
                url: "https://actor/work".to_string(),
                headers: vec![("x-foo".to_string(), "bar".to_string())],
                body: vec![1, 2, 3, 4],
                request_id: "req-1".to_string(),
            },
        };
        let encoded = encode_actor_invoke_request(&request).expect("request should encode");
        let decoded = decode_actor_invoke_request(&encoded).expect("request should decode");
        assert_eq!(decoded.worker_name, request.worker_name);
        assert_eq!(decoded.binding, request.binding);
        assert_eq!(decoded.key, request.key);
        assert_eq!(decoded.request.method, request.request.method);
        assert_eq!(decoded.request.url, request.request.url);
        assert_eq!(decoded.request.headers, request.request.headers);
        assert_eq!(decoded.request.body, request.request.body);
        assert_eq!(decoded.request.request_id, request.request.request_id);
    }

    #[test]
    fn actor_invoke_response_roundtrip() {
        let output = WorkerOutput {
            status: 207,
            headers: vec![("content-type".to_string(), "text/plain".to_string())],
            body: b"ok".to_vec(),
        };
        let encoded = encode_actor_invoke_response(&output).expect("response should encode");
        let decoded = decode_actor_invoke_response(&encoded).expect("response should decode");
        assert_eq!(decoded.status, output.status);
        assert_eq!(decoded.headers, output.headers);
        assert_eq!(decoded.body, output.body);
    }
}
