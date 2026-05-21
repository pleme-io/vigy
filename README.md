# vigy

> Always-on tatara-lisp reconciler runtime embeddable in pleme-io apps.

Small reconcilers — *vigies* — authored in tatara-lisp, ticking
continuously inside a host app (mado, tear-daemon, vitrine, carve, or
your own binary) to keep some piece of state always-converging toward
desired. Kubernetes-controller discipline at lisp-program scope.

## Why this exists

Every long-running pleme-io app ends up wanting the same pattern:
*"on a timer, look at some state, decide what to do, do it, repeat —
forever, configurably, observably."* Mado wants it to keep its
embedded tear in sync with the system daemon. Tear-daemon wants it to
flush scrollback rings to disk. Vitrine wants it to refresh PR
evidence. Carve wants it for restack on review feedback.

Implementing that loop from scratch in every app is the duplication
this crate kills. Vigy reifies the pattern: one runtime, one schema,
one set of API surfaces, an evaluator that runs operator-authored
tatara-lisp programs as the reconciler bodies.

## Layout

```
vigy/
├── spec/
│   ├── vigy.openapi.yaml    # REST source of truth (utoipa generates handlers)
│   ├── vigy.proto           # gRPC (tonic-build)
│   └── vigy.graphql         # GraphQL SDL
├── crates/
│   ├── vigy-types/          # pure domain (Vigy, VigyState, ReconcileAction)
│   ├── vigy-store/          # SeaORM/SQLite — vigies, runs, events
│   ├── vigy-eval/           # tatara-lisp host bindings + intrinsics
│   ├── vigy-runtime/        # tokio tick scheduler + registry + event bus
│   ├── vigy-rpc/            # gRPC server (tonic, vendored protoc)
│   ├── vigy-graphql/        # async-graphql + axum
│   ├── vigy-rest/           # axum + utoipa REST + Swagger UI
│   ├── vigy-mcp/            # MCP tool catalog + dispatch
│   ├── vigy-cli/            # `vigy` binary
│   └── vigy/                # facade re-export crate
└── examples/
    └── hello-vigy.tatara
```

## Anatomy of a vigy

A vigy is a tatara-lisp program. Every tick (configurable interval, ≥
100 ms), the runtime evaluates the program against a fresh per-tick
host. The program emits reconcile actions through intrinsics:

```lisp
;; hello-vigy.tatara — the smallest non-trivial vigy
(vigy-log "info" "hello from a vigy")
(vigy-noop)
```

Intrinsics:

| Form                          | Effect                                          |
|-------------------------------|-------------------------------------------------|
| `(vigy-emit kind payload?)`   | Queue a ReconcileAction. Kind ∈ pull\|push\|noop\|custom. |
| `(vigy-pull payload)`         | Sugar for `(vigy-emit "pull" payload)`.         |
| `(vigy-push payload)`         | Sugar for `(vigy-emit "push" payload)`.         |
| `(vigy-noop)`                 | Sugar for `(vigy-emit "noop")`.                 |
| `(vigy-log level msg)`        | Emit a structured log line.                     |
| `(vigy-tick)`                 | Unix epoch millis of the current tick's start. |

Plus the full tatara-lisp stdlib — arithmetic, comparison, list ops,
strings, channels, fibers, higher-order helpers.

## Quick start

```bash
# build
cargo build --release

# register a vigy
./target/release/vigy register examples/hello-vigy.tatara \
    --name hello --every 1000 --label host=local

# inspect
./target/release/vigy list
./target/release/vigy inspect <id>
./target/release/vigy tail              # stream reconcile events as JSON

# force-tick
./target/release/vigy tick <id>

# lifecycle
./target/release/vigy disable <id>
./target/release/vigy enable <id>
./target/release/vigy delete <id>

# serve the API surfaces (gRPC + REST + GraphQL on the same runtime)
./target/release/vigy serve --bind 127.0.0.1:38821
```

## Persistence

SeaORM-backed SQLite, default at `~/.local/share/vigy/vigy.db`
(overridable with `--db` or `$VIGY_DB`). Operator-debuggable:

```bash
sqlite3 ~/.local/share/vigy/vigy.db
sqlite> .schema
sqlite> SELECT id, name, enabled FROM vigies;
sqlite> SELECT vigy_id, result, started_at FROM vigy_runs ORDER BY started_at DESC LIMIT 10;
```

## API surfaces

Same semantic operations on three transports — pick by consumer:

| Transport | Use for | Bind     | Endpoint |
|-----------|---------|----------|----------|
| **gRPC**     | low-latency hot-path (mado embed, tear-daemon coord) | port  | `pleme.vigy.v1.VigyService` |
| **REST**     | human-facing CLI, OpenAPI tooling | port  | `/v1/vigies/*` + Swagger UI at `/swagger` |
| **GraphQL**  | introspection, dashboards         | port  | `/graphql` |
| **MCP**      | Claude + other AI clients         | stdio | tool catalog in `vigy_mcp::tool_catalog()` |

Schemas live in `spec/` as the source of truth — server handlers in
each transport crate are derived from them, not hand-written.

## Embedding in mado / tear-daemon

```rust
use vigy::{RuntimeHandle, Vigy, TickInterval};

// In your app's main:
let runtime = RuntimeHandle::open("/path/to/your-app.db".as_ref()).await?;

// Register reconcilers your app needs always-on:
let v = Vigy::new(
    "sync-to-system-tear",
    include_str!("../vigies/sync-to-system-tear.tatara"),
    TickInterval::from_millis(500)?,
)?;
runtime.register_or_update(v).await?;

// Now mado / tear-daemon / etc. owns a vigy that ticks forever as
// long as the app runs. Apps in your panes can register more via
// the MCP catalog (vigy_mcp::dispatch).
```

## Status

| Crate         | Status    | Tests |
|---------------|-----------|-------|
| vigy-types    | shipped   | 10    |
| vigy-store    | shipped   | 3     |
| vigy-eval     | shipped   | 6     |
| vigy-runtime  | shipped   | 4     |
| vigy-cli      | shipped   | 0 (e2e via smoke) |
| vigy-rest     | shipped   | —     |
| vigy-graphql  | shipped (read-side; mutations minimal) | — |
| vigy-rpc      | scaffolded (proto compiles; handlers Unimplemented) | — |
| vigy-mcp      | catalog + dispatch | — |
| vigy (facade) | shipped   | —     |

Total: **23 tests pass**.

## Family

- **mado** — GPU terminal emulator. Primary embedder.
- **tear** — Rust-native multiplexer. Co-host of vigies that bridge
  embedded sessions to the system daemon.
- **tatara-lisp** — the lisp dialect vigy programs are authored in.
- **vitrine** — pre-merge evidence delivery. May host vigies that
  keep PR evidence fresh in the background.
- **carve** — monolithic→stacked PR primitive. May host vigies for
  restack-on-feedback automation.

## License

MIT.
