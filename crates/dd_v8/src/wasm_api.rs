//! Safe-Rust WebAssembly JS API host backing powered by `wtime-runtime`.
//!
//! Exposes `WebAssembly.{validate, compile, instantiate, Module, Instance,
//! compileStreaming, instantiateStreaming}` to JavaScript inside `dd_v8`
//! without V8, compiling and executing guest Wasm modules via `wtime-runtime`.

use crate::ops::OpError;
use crate::serde_v8::{WireValue, decode_wire_args, encode_err_envelope, encode_ok_envelope};
use std::collections::HashMap;
use wtime_runtime::{
    Config, Engine, External, ExternalKind, ExternalType, Linker, Module, RunOutcome, Store,
    ValType, Value,
};

struct InstanceRec {
    exports: HashMap<String, External>,
}

pub(crate) struct WasmHostState {
    engine: Engine,
    store: Store,
    modules: Vec<Module>,
    instances: Vec<InstanceRec>,
}

impl WasmHostState {
    pub(crate) fn new() -> Self {
        let engine = Engine::new(Config::default()).expect("wtime-runtime default Engine");
        let store = Store::new(&engine);
        Self {
            engine,
            store,
            modules: Vec::new(),
            instances: Vec::new(),
        }
    }

    pub(crate) fn dispatch(&mut self, kind: u32, payload: &[u8], max_bytes: usize) -> Vec<u8> {
        let args = match decode_wire_args(payload, max_bytes) {
            Ok(v) => v,
            Err(e) => return encode_err_envelope(&OpError::from(e)),
        };
        let result = match kind {
            0 => self.op_validate(&args),
            1 => self.op_compile(&args),
            2 => self.op_instantiate(&args),
            3 => self.op_call(&args),
            4 => self.op_read_global(&args),
            5 => self.op_read_memory(&args),
            _ => Err(OpError::type_error(format!("unknown wasm_op kind {kind}"))),
        };
        match result {
            Ok(v) => encode_ok_envelope(&v),
            Err(e) => encode_err_envelope(&e),
        }
    }

    fn op_validate(&self, args: &[WireValue]) -> Result<WireValue, OpError> {
        let Some(WireValue::Bytes(bytes)) = args.first() else {
            return Err(OpError::type_error(
                "WebAssembly.validate requires a BufferSource",
            ));
        };
        Ok(WireValue::Bool(self.engine.compile(bytes).is_ok()))
    }

    fn op_compile(&mut self, args: &[WireValue]) -> Result<WireValue, OpError> {
        let Some(WireValue::Bytes(bytes)) = args.first() else {
            return Err(OpError::type_error(
                "WebAssembly.Module requires a BufferSource",
            ));
        };
        let module = self
            .engine
            .compile(bytes)
            .map_err(|e| OpError::custom("WebAssemblyCompileError", e.to_string()))?;
        let exports: Vec<WireValue> = module
            .exports()
            .into_iter()
            .map(|(name, kind, _)| {
                let kind_str = match kind {
                    ExternalKind::Func => "function",
                    ExternalKind::Table => "table",
                    ExternalKind::Memory => "memory",
                    ExternalKind::Global => "global",
                    ExternalKind::Tag => "tag",
                };
                WireValue::Object(vec![
                    ("name".to_string(), WireValue::String(name.to_string())),
                    ("kind".to_string(), WireValue::String(kind_str.to_string())),
                ])
            })
            .collect();
        let id = self.modules.len() as i32;
        self.modules.push(module);
        Ok(WireValue::Object(vec![
            ("id".to_string(), WireValue::I32(id)),
            ("exports".to_string(), WireValue::Array(exports)),
        ]))
    }

    fn op_instantiate(&mut self, args: &[WireValue]) -> Result<WireValue, OpError> {
        let mod_id = match args.first() {
            Some(WireValue::I32(n)) if *n >= 0 => *n as usize,
            Some(WireValue::F64(n)) if *n >= 0.0 => *n as usize,
            _ => return Err(OpError::type_error("invalid WebAssembly.Module handle")),
        };
        let Some(module) = self.modules.get(mod_id) else {
            return Err(OpError::type_error("unknown WebAssembly.Module handle"));
        };
        let linker = Linker::new(&self.engine);
        let instance = linker
            .instantiate(&mut self.store, module)
            .map_err(|e| OpError::custom("WebAssemblyLinkError", e.to_string()))?;
        let raw_exports = instance
            .exports(&self.store)
            .map_err(|e| OpError::custom("WebAssemblyRuntimeError", e.to_string()))?;
        let mut export_map = HashMap::with_capacity(raw_exports.len());
        let mut export_list = Vec::with_capacity(raw_exports.len());
        for (name, ext) in raw_exports {
            let kind_str = match ext {
                External::Func(_) => "function",
                External::Table(_) => "table",
                External::Memory(_) => "memory",
                External::Global(_) => "global",
                External::Tag(_) => "tag",
            };
            export_list.push(WireValue::Object(vec![
                ("name".to_string(), WireValue::String(name.clone())),
                ("kind".to_string(), WireValue::String(kind_str.to_string())),
            ]));
            export_map.insert(name, ext);
        }
        let id = self.instances.len() as i32;
        self.instances.push(InstanceRec {
            exports: export_map,
        });
        Ok(WireValue::Object(vec![
            ("id".to_string(), WireValue::I32(id)),
            ("exports".to_string(), WireValue::Array(export_list)),
        ]))
    }

    fn op_call(&mut self, args: &[WireValue]) -> Result<WireValue, OpError> {
        let inst_id = match args.first() {
            Some(WireValue::I32(n)) if *n >= 0 => *n as usize,
            Some(WireValue::F64(n)) if *n >= 0.0 => *n as usize,
            _ => return Err(OpError::type_error("invalid WebAssembly.Instance handle")),
        };
        let Some(WireValue::String(name)) = args.get(1) else {
            return Err(OpError::type_error("missing export name"));
        };
        let call_args = match args.get(2) {
            Some(WireValue::Array(arr)) => arr.as_slice(),
            _ => &[],
        };
        let func = {
            let Some(rec) = self.instances.get(inst_id) else {
                return Err(OpError::type_error("unknown WebAssembly.Instance handle"));
            };
            match rec.exports.get(name) {
                Some(External::Func(f)) => *f,
                _ => {
                    return Err(OpError::type_error(format!(
                        "export {name} is not a function"
                    )));
                }
            }
        };
        let func_ty = match self
            .store
            .external_type(External::Func(func))
            .map_err(|e| OpError::custom("WebAssemblyRuntimeError", e.to_string()))?
        {
            ExternalType::Func(ft) => ft,
            _ => return Err(OpError::type_error("export is not a function")),
        };
        let mut wasm_args = Vec::with_capacity(func_ty.params.len());
        for (i, param_ty) in func_ty.params.iter().enumerate() {
            let js_val = call_args.get(i).unwrap_or(&WireValue::Undefined);
            wasm_args.push(wire_to_wasm(js_val, *param_ty)?);
        }
        let outcome = self
            .store
            .invoke(func, &wasm_args)
            .map_err(|e| OpError::custom("WebAssemblyRuntimeError", e.to_string()))?;
        match outcome {
            RunOutcome::Returned(mut vals) => match vals.len() {
                0 => Ok(WireValue::Undefined),
                1 => Ok(wasm_to_wire(vals.remove(0))),
                _ => Ok(WireValue::Array(
                    vals.into_iter().map(wasm_to_wire).collect(),
                )),
            },
            other => Err(OpError::custom(
                "WebAssemblyRuntimeError",
                format!("unexpected Wasm outcome: {other:?}"),
            )),
        }
    }

    fn op_read_global(&self, args: &[WireValue]) -> Result<WireValue, OpError> {
        let inst_id = match args.first() {
            Some(WireValue::I32(n)) if *n >= 0 => *n as usize,
            Some(WireValue::F64(n)) if *n >= 0.0 => *n as usize,
            _ => return Err(OpError::type_error("invalid WebAssembly.Instance handle")),
        };
        let Some(WireValue::String(name)) = args.get(1) else {
            return Err(OpError::type_error("missing global name"));
        };
        let Some(rec) = self.instances.get(inst_id) else {
            return Err(OpError::type_error("unknown WebAssembly.Instance handle"));
        };
        let Some(External::Global(g)) = rec.exports.get(name) else {
            return Err(OpError::type_error(format!(
                "export {name} is not a global"
            )));
        };
        let v = self
            .store
            .get_global(*g)
            .map_err(|e| OpError::custom("WebAssemblyRuntimeError", e.to_string()))?;
        Ok(wasm_to_wire(v))
    }

    fn op_read_memory(&self, args: &[WireValue]) -> Result<WireValue, OpError> {
        let inst_id = match args.first() {
            Some(WireValue::I32(n)) if *n >= 0 => *n as usize,
            Some(WireValue::F64(n)) if *n >= 0.0 => *n as usize,
            _ => return Err(OpError::type_error("invalid WebAssembly.Instance handle")),
        };
        let Some(WireValue::String(name)) = args.get(1) else {
            return Err(OpError::type_error("missing memory name"));
        };
        let offset = match args.get(2) {
            Some(WireValue::I32(n)) if *n >= 0 => *n as u64,
            Some(WireValue::F64(n)) if *n >= 0.0 => *n as u64,
            _ => 0,
        };
        let len = match args.get(3) {
            Some(WireValue::I32(n)) if *n >= 0 => *n as usize,
            Some(WireValue::F64(n)) if *n >= 0.0 => *n as usize,
            _ => 0,
        };
        let Some(rec) = self.instances.get(inst_id) else {
            return Err(OpError::type_error("unknown WebAssembly.Instance handle"));
        };
        let Some(External::Memory(m)) = rec.exports.get(name) else {
            return Err(OpError::type_error(format!(
                "export {name} is not a memory"
            )));
        };
        let bytes = self
            .store
            .read_memory(*m, offset, len)
            .map_err(|e| OpError::custom("WebAssemblyRuntimeError", e.to_string()))?;
        Ok(WireValue::Bytes(bytes))
    }
}

fn wire_to_f64(v: &WireValue) -> f64 {
    match v {
        WireValue::Undefined => f64::NAN,
        WireValue::Null => 0.0,
        WireValue::Bool(b) => {
            if *b {
                1.0
            } else {
                0.0
            }
        }
        WireValue::I32(n) => f64::from(*n),
        WireValue::F64(n) => *n,
        WireValue::String(s) => s.trim().parse::<f64>().unwrap_or(f64::NAN),
        _ => f64::NAN,
    }
}

fn wire_to_i32(v: &WireValue) -> i32 {
    let f = wire_to_f64(v);
    if !f.is_finite() || f == 0.0 {
        return 0;
    }
    let trunc = f.trunc();
    let rem = trunc.rem_euclid(4_294_967_296.0);
    (rem as u32) as i32
}

fn wire_to_wasm(v: &WireValue, ty: ValType) -> Result<Value, OpError> {
    match ty {
        ValType::I32 => Ok(Value::I32(wire_to_i32(v))),
        ValType::I64 => match v {
            WireValue::BigInt(s) => {
                let n = s
                    .parse::<i128>()
                    .map_err(|_| OpError::type_error("cannot convert BigInt to i64"))?;
                Ok(Value::I64(n as i64))
            }
            _ => Err(OpError::type_error(
                "cannot convert non-BigInt to WebAssembly i64",
            )),
        },
        ValType::F32 => Ok(Value::F32((wire_to_f64(v) as f32).to_bits())),
        ValType::F64 => Ok(Value::F64(wire_to_f64(v).to_bits())),
        other => Err(OpError::type_error(format!(
            "unsupported JS-to-Wasm parameter type {other:?}"
        ))),
    }
}

fn wasm_to_wire(v: Value) -> WireValue {
    match v {
        Value::I32(n) => WireValue::I32(n),
        Value::I64(n) => WireValue::BigInt(n.to_string()),
        Value::F32(bits) => WireValue::F64(f64::from(f32::from_bits(bits))),
        Value::F64(bits) => WireValue::F64(f64::from_bits(bits)),
        _ => WireValue::Undefined,
    }
}
