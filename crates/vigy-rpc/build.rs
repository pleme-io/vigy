//! Generates Rust types + service stubs from `spec/vigy.proto` at build
//! time. Relies on a `protoc` binary being available on PATH.
//!
//! NOT a vendored-protoc crate (`protobuf-src`, `protoc-bin-vendored`):
//! both locate their bundled binary via `env!("CARGO_MANIFEST_DIR")` or
//! a build.rs-produced `OUT_DIR`, baked in as a compile-time constant
//! pointing at THEIR OWN ephemeral Nix build sandbox — a path that no
//! longer exists by the time THIS crate's build.rs calls it in a
//! separate derivation. The substrate's nix build provides a real
//! `protoc` (nixpkgs' `pkgs.protobuf`) on PATH for this crate via a
//! `NativeBuildInputs` quirk (gen-cargo's quirks registry, "vigy-rpc"),
//! so no explicit PROTOC env var is needed here.

fn main() {
    // `CARGO_MANIFEST_DIR` means different things under different build
    // harnesses for a workspace member: plain cargo sets it to this
    // crate's own directory (crates/vigy-rpc), but nixpkgs'
    // buildRustCrate sets it to the whole unpacked WORKSPACE ROOT
    // instead — a hand-relative "../../spec/vigy.proto" resolved
    // against the wrong base under one harness or the other. Try both
    // interpretations rather than assume either.
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        manifest_dir.join("../../spec"), // CARGO_MANIFEST_DIR = this crate's dir
        manifest_dir.join("spec"),       // CARGO_MANIFEST_DIR = the workspace root
    ];
    let spec_dir = candidates
        .into_iter()
        .find(|p| p.join("vigy.proto").is_file())
        .expect("could not locate spec/vigy.proto relative to CARGO_MANIFEST_DIR under either interpretation");
    let proto = spec_dir.join("vigy.proto");
    println!("cargo:rerun-if-changed={}", proto.display());
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[&proto], &[&spec_dir])
        .expect("tonic_build compile vigy.proto");
}
