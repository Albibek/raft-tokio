extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .file("schema/handshake.capnp")
        .run()
        .expect("Failed compiling messages schema");
}
