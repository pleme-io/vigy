;; vigy — always-on tatara-lisp reconciler runtime embeddable in pleme-io apps.
;;
;; Substrate-promoted form of the "vigy" pattern: small always-running
;; reconcilers, authored in tatara-lisp, hosted inside an app (mado, tear,
;; vitrine, carve, etc.) that wants to maintain state continuously.
;;
;; Crates:
;;   vigy-types     — pure typed domain (Vigy, VigyState, ReconcileAction).
;;   vigy-store     — SeaORM/SQLite persistence (sessions, runs, events).
;;   vigy-eval      — tatara-lisp host bindings + stdlib.
;;   vigy-runtime   — tokio scheduler, registry, hot-reload.
;;   vigy-rpc       — gRPC server (tonic).
;;   vigy-graphql   — async-graphql server.
;;   vigy-rest      — axum REST handlers derived from spec/vigy.openapi.yaml.
;;   vigy-mcp       — MCP server tools (lets Claude drive the registry).
;;   vigy-cli       — `vigy run / list / inspect / tail / serve` CLI.
;;   vigy           — facade re-exporting the lib surface.

(defcaixa
  :nome "vigy"
  :versao "0.1.0"
  :kind Biblioteca
  :edicao "2026"
  :descricao "Always-on tatara-lisp reconciler runtime: small reconcilers authored in tatara-lisp, embeddable in mado/tear/vitrine/carve. SeaORM/SQLite persistence; gRPC + GraphQL + OpenAPI-derived REST control surfaces."
  :repositorio "github:pleme-io/vigy"
  :licenca "MIT"
  :autores ("pleme-io")
  :etiquetas ("vigy" "reconciler" "tatara-lisp" "mado" "tear" "always-on" "caixa-biblioteca")
  :deps ("tatara-lisp" "tatara-lisp-eval" "shikumi")
  :deps-dev ())
