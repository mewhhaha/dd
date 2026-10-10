// Blob part storage behind `Blob` and `File`, ported from deno_web's blob.rs
// (Copyright 2018-2026 the Deno authors, MIT license). Parts live in the
// runtime's op state, keyed by UUID strings.

use dd_v8::{JsBuffer, OpError, OpState, ToJsBuffer};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

/// A part's bytes and the range of them it covers.
#[derive(Clone)]
struct Part {
    bytes: Arc<[u8]>,
    start: usize,
    len: usize,
}

impl Part {
    fn bytes(&self) -> &[u8] {
        &self.bytes[self.start..self.start + self.len]
    }
}

struct ObjectUrlBlob {
    media_type: String,
    parts: Vec<Part>,
}

#[derive(Default)]
pub(crate) struct BlobStore {
    parts: HashMap<String, Part>,
    object_urls: HashMap<String, ObjectUrlBlob>,
}

fn store(state: &mut OpState) -> &mut BlobStore {
    if !state.has::<BlobStore>() {
        state.put(BlobStore::default());
    }
    state.borrow_mut::<BlobStore>()
}

fn part_not_found() -> OpError {
    OpError::type_error("Blob part not found")
}

fn insert(store: &mut BlobStore, part: Part) -> String {
    let id = Uuid::new_v4().to_string();
    store.parts.insert(id.clone(), part);
    id
}

pub(super) fn op_blob_create_part(state: &mut OpState, data: JsBuffer) -> String {
    let bytes: Arc<[u8]> = data.into_vec().into();
    let len = bytes.len();
    insert(
        store(state),
        Part {
            bytes,
            start: 0,
            len,
        },
    )
}

#[derive(Deserialize)]
pub(super) struct SliceOptions {
    start: usize,
    len: usize,
}

pub(super) fn op_blob_slice_part(
    state: &mut OpState,
    id: String,
    options: SliceOptions,
) -> Result<String, OpError> {
    let store = store(state);
    let part = store.parts.get(&id).ok_or_else(part_not_found)?;
    if options.start + options.len > part.len {
        return Err(OpError::type_error(
            "start + len can not be larger than blob part size",
        ));
    }
    let sliced = Part {
        bytes: Arc::clone(&part.bytes),
        start: part.start + options.start,
        len: options.len,
    };
    Ok(insert(store, sliced))
}

pub(super) async fn op_blob_read_part(
    state: Rc<RefCell<OpState>>,
    id: String,
) -> Result<ToJsBuffer, OpError> {
    let mut state = state.borrow_mut();
    let part = store(&mut state)
        .parts
        .get(&id)
        .ok_or_else(part_not_found)?;
    Ok(part.bytes().to_vec().into())
}

pub(super) fn op_blob_remove_part(state: &mut OpState, id: String) {
    store(state).parts.remove(&id);
}

#[derive(Serialize)]
pub(super) struct ReturnBlobPart {
    uuid: String,
    size: usize,
}

pub(super) fn op_blob_clone_part(
    state: &mut OpState,
    id: String,
) -> Result<ReturnBlobPart, OpError> {
    let store = store(state);
    let part = store.parts.get(&id).ok_or_else(part_not_found)?.clone();
    let size = part.len;
    Ok(ReturnBlobPart {
        uuid: insert(store, part),
        size,
    })
}

pub(super) fn op_blob_create_object_url(
    state: &mut OpState,
    media_type: String,
    part_ids: Vec<String>,
) -> Result<String, OpError> {
    let store = store(state);
    let parts = part_ids
        .iter()
        .map(|id| store.parts.get(id).cloned().ok_or_else(part_not_found))
        .collect::<Result<Vec<_>, _>>()?;
    let url = format!("blob:null/{}", Uuid::new_v4());
    store
        .object_urls
        .insert(url.clone(), ObjectUrlBlob { media_type, parts });
    Ok(url)
}

pub(super) fn op_blob_revoke_object_url(state: &mut OpState, url: String) -> Result<(), OpError> {
    let url = Url::parse(&url).map_err(|error| OpError::new(error.to_string()))?;
    store(state).object_urls.remove(url.as_str());
    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ReturnBlob {
    media_type: String,
    parts: Vec<ReturnBlobPart>,
}

pub(super) fn op_blob_from_object_url(
    state: &mut OpState,
    url: String,
) -> Result<Option<ReturnBlob>, OpError> {
    let mut url = Url::parse(&url).map_err(|error| OpError::new(error.to_string()))?;
    if url.scheme() != "blob" {
        return Ok(None);
    }
    url.set_fragment(None);
    let store = store(state);
    let Some(blob) = store.object_urls.get(url.as_str()) else {
        return Ok(None);
    };
    let media_type = blob.media_type.clone();
    let parts = blob.parts.clone();
    let parts = parts
        .into_iter()
        .map(|part| {
            let size = part.len;
            ReturnBlobPart {
                uuid: insert(store, part),
                size,
            }
        })
        .collect();
    Ok(Some(ReturnBlob { media_type, parts }))
}
