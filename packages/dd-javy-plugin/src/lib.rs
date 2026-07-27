use javy_plugin_api::{
    Config, import_namespace,
    javy::{Runtime, quickjs::prelude::Func},
};

import_namespace!("dd-javy-plugin-v1");

#[link(wasm_import_module = "dd_host")]
unsafe extern "C" {
    fn call(request_pointer: *const u8, request_length: usize) -> usize;
    fn read(response_pointer: *mut u8, response_length: usize);
}

fn config() -> Config {
    let mut config = Config::default();
    config
        .event_loop(true)
        .javy_stream_io(true)
        .text_encoding(true);
    config
}

fn modify_runtime(runtime: Runtime) -> Runtime {
    runtime.context().with(|context| {
        context
            .globals()
            .set("__ddHostCall", Func::from(call_host))
            .expect("could not install __ddHostCall");
    });
    runtime
}

fn call_host(request: String) -> String {
    let response_length = unsafe { call(request.as_ptr(), request.len()) };
    let mut response = vec![0; response_length];
    unsafe { read(response.as_mut_ptr(), response.len()) };
    String::from_utf8(response).expect("dd_host returned non-UTF-8 bytes")
}

#[unsafe(export_name = "initialize-runtime")]
fn initialize_runtime() {
    javy_plugin_api::initialize_runtime(config, modify_runtime)
        .expect("could not initialize dd Javy runtime");
}
