{
  description = "vigy — always-on tatara-lisp reconciler runtime embedded in pleme-io apps";

  # substrate.rust.workspace dispatches over Cargo.gen.lock (the slim gen delta,
  # reconstructed to the full BuildSpec in pure Nix) — no crate2nix, no Cargo.nix.
  inputs.substrate.url = "github:pleme-io/substrate";

  outputs = { substrate, ... }: substrate.rust.workspace {
    src = ./.;
    member = "vigy";
    # vigy-rpc's build script runs protoc (tonic-build); the name reaches the
    # build and the dev shell the release gate tests in.
    nativeBuildInputs = [ "protobuf" ];
  };
}
