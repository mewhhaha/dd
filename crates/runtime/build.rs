fn main() {
    println!("cargo:rerun-if-changed=schema/actor_rpc.capnp");
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/actor_rpc.capnp")
        .run()
        .expect("failed to compile Cap'n Proto schema for actor_rpc");
}
