use super::{RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
use base64::Engine;
use bytes::Bytes;
use common::{
    DeployAsset, DeployBinding, DeployConfig, DeployInternalConfig, DeployServerModule,
    DeployServerModuleKind, DeployTraceDestination, ErrorKind, WorkerInvocation, WorkerOutput,
};
use serde::Deserialize;
use serde_json::Value;
use serial_test::serial;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

mod assets;
mod bindings;
mod cache;
mod console;
mod deployment;
mod deployment_drain;
mod egress;
mod examples;
mod fixtures;
mod globals;
mod hardening;
mod host_fetch;
mod inspector;
mod internal_trace;
mod kv;
mod limits;
mod memory;
mod memory_contract;
mod memory_list;
mod memory_read;
mod request_control;
mod scaling;
mod scheduling;
mod service_fetch;
mod sessions;
mod shutdown;
mod startup;
mod streaming;

use self::fixtures::*;
