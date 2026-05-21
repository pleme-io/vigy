{
  description = "vigy — always-on tatara-lisp reconciler runtime embedded in pleme-io apps";

  nixConfig = {
    allow-import-from-derivation = true;
  };

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    crate2nix.url = "github:nix-community/crate2nix";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.fenix.follows = "fenix";
    };
    devenv = {
      url = "github:cachix/devenv";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    crate2nix,
    flake-utils,
    substrate,
    devenv,
    ...
  }:
    (import "${substrate}/lib/rust-workspace-release-flake.nix" {
      inherit nixpkgs crate2nix flake-utils devenv;
    }) {
      toolName = "vigy";
      packageName = "vigy-cli";
      src = self;
      repo = "pleme-io/vigy";

      module = {
        description = "vigy — always-on tatara-lisp reconciler runtime embeddable in mado, tear, or any pleme-io app";
        hmNamespace = "blackmatter.components";

        # User-level launchd / systemd-user service running
        # `vigy serve --addr <httpAddr>`. Operators enable with
        # `blackmatter.components.vigy.enableHttpService = true;`.
        # REST + Swagger UI bind here.
        withHttp = true;
        httpSubcommand = "serve";
        defaultHttpAddr = "127.0.0.1:38821";

        # System-level service (NixOS systemd / Darwin launchd). Most
        # fleet use is per-user via withHttp, but we expose the system
        # path too because vigy scales fine as a host-wide primitive
        # (e.g. one runtime serving multiple operator accounts).
        withSystemDaemon = true;
        daemonSubcommand = "serve";

        # Shikumi-style YAML config at ~/.config/vigy/vigy.yaml. Operators
        # set typed values via `blackmatter.components.vigy.runtime.*` /
        # `.api.*` and the substrate materialises the YAML.
        withShikumiConfig = true;
        shikumiConfigPath = ".config/vigy/vigy.yaml";
        shikumiDefaults = {
          runtime = {
            db_path = "~/.local/share/vigy/vigy.db";
          };
          api = {
            rest_addr = "127.0.0.1:38821";
            grpc_addr = "127.0.0.1:38822";
            graphql_addr = "127.0.0.1:38823";
          };
        };
        shikumiTypedGroups = {
          runtime = {
            db_path = {
              type = "str";
              default = "~/.local/share/vigy/vigy.db";
              description = "SQLite DB path. Stores registered vigies + run history.";
            };
          };
          api = {
            rest_addr = {
              type = "str";
              default = "127.0.0.1:38821";
              description = "Axum REST + Swagger UI bind address.";
            };
            grpc_addr = {
              type = "str";
              default = "127.0.0.1:38822";
              description = "Tonic gRPC bind address (reserved; handlers TODO).";
            };
            graphql_addr = {
              type = "str";
              default = "127.0.0.1:38823";
              description = "async-graphql bind address.";
            };
          };
        };
      };
    };
}
