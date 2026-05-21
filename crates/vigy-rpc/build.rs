//! Generates Rust types + service stubs from `spec/vigy.proto` at build
//! time. Uses the vendored protoc from `protobuf-src` so the build is
//! hermetic — no system protoc dependency.

fn main() {
    // Vendored protoc — works on any platform without external install.
    std::env::set_var("PROTOC", protobuf_src::protoc());

    let proto = "../../spec/vigy.proto";
    println!("cargo:rerun-if-changed={proto}");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &["../../spec"])
        .expect("tonic_build compile vigy.proto");
}
