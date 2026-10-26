# vigy

Always-on reconciler runtime (caixa Biblioteca kind). Every host (mado, tear,
arnes, escriba) embeds this one runtime.

## Two languages, one intrinsic set

A vigy carries a typed `Language` (vigy-types): `tatara-lisp` (the default) or
`blue`. Nothing translates one into the other. A blue vigy is never looser than
the tatara path. The 30 intrinsics are ONE `#[host(prefix = "vigy_")] impl
VigyHost` block; blue names are the tatara names with `-` spelled `_`.
Detail, the arity test and `HostExtension` rules: [docs/two-languages.md](docs/two-languages.md).

> **Theory:** `pleme-io/theory/VIGY.md` (canonical: blue authoring, one platform, per-tick surfaces)
> **Operator doc:** `pleme-io/docs/vigy.md` (TODO)
> **Skill:** `pleme-io/blackmatter-pleme/skills/vigy/SKILL.md`
> **Family:** mado (primary embedder), tear (co-host), arnes, escriba,
> tatara-lisp (evaluator), vitrine (sibling primitive), carve (sibling).

This library hosts small tatara-lisp reconcilers — *vigies* — inside
a long-running app. The runtime ticks each vigy on its configured
interval, evaluates its program against a fresh host, persists the
result, and broadcasts on an event bus. Three API surfaces (gRPC /
REST / GraphQL) plus an MCP catalog let any consumer drive the
registry.

## Workspace layout

```
crates/
  vigy-types/      domain types
  vigy-store/      SeaORM/SQLite persistence
  vigy-eval/       tatara-lisp host bindings
  vigy-runtime/    tokio scheduler + registry + event bus
  vigy-rpc/        gRPC (tonic, vendored protoc)
  vigy-graphql/    async-graphql + axum
  vigy-rest/       axum + utoipa REST + Swagger UI
  vigy-mcp/        MCP tool catalog + dispatch
  vigy-cli/        `vigy` binary
  vigy/            facade re-export
spec/
  vigy.openapi.yaml   REST source of truth (utoipa-derived handlers)
  vigy.proto          gRPC service def (tonic-build)
  vigy.graphql        GraphQL SDL
```

## Build / Run

```bash
nix build .#vigy                       # builds the binary
nix run  .#vigy -- --help              # invocation

cargo build --workspace
cargo test  --workspace
cargo run --bin vigy -- --help
```

## Conventions

- Rust edition 2021, MIT license (workspace defaults).
- clap derive for CLI.
- shikumi-style typed config (future — currently CLI flags + env).
- BLAKE3-derived stable VigyIds (name + program → 16 hex chars).
- substrate's `rust-workspace-release-flake.nix` for multi-platform release.
- caixa-native (caixa.lisp declares Biblioteca kind).

## Subcommand surface (vigy-cli)

```bash
vigy register <file> --name <n> [--language tatara-lisp|blue] [--every <ms>] [--label k=v] [--disabled]
vigy list [--selector k=v]
vigy inspect <id>
vigy tick <id>                         # force-tick
vigy enable <id> | disable <id> | delete <id>
vigy tail [--id <id>]                  # stream reconcile events
vigy serve [--bind addr]               # API servers (not yet wired in v0.1)
```

## What this library deliberately doesn't do

- **Doesn't interpret reconcile actions.** Vigies emit
  `ReconcileAction`s; the host app interprets them. The runtime
  records + broadcasts only — semantics belong to the embedder.
- **Doesn't fight the host for the event loop.** Each vigy lives in
  its own tokio task. The host's main runtime + the vigy runtime
  cooperate via `Arc` handles + broadcast channels.
- **Doesn't sync state across hosts.** That's the host's
  responsibility (via vigies it authors). Vigy is the *mechanism*;
  the *policy* is the embedder's.

## Persistence

SeaORM + SQLite at `~/.local/share/vigy/vigy.db` (or `$VIGY_DB`).
Operator-debuggable via `sqlite3 vigy.db`. Migrations run on `Store::open`.

## API source of truth

Schemas in `spec/` are authoritative. Handlers in the transport crates
derive from them — no parallel definition. Adding a field is a
spec-edit + regenerate, not a 4-place change.

## Status

Counts and known gaps: [docs/two-languages.md](docs/two-languages.md#status).
