//! MCP server for vigy.
//!
//! Exposes the runtime CRUD surface as MCP tools so Claude (and other
//! MCP clients) can drive the registry from a chat. Tools mirror the
//! REST/GraphQL/gRPC operations; semantics identical.
//!
//! ## Implementation status
//!
//! v0.1 ships the tool *catalog* — the JSON Schemas + tool descriptions
//! a host's MCP server can re-export. Transport wiring (stdio JSON-RPC
//! vs SSE vs http+stream) is left to the host (mado's `kaname` MCP
//! server, or a standalone `vigy-mcp` binary in a follow-up).
//!
//! This split keeps the crate transport-agnostic — usable both as the
//! MCP backbone of mado *and* as a sidecar daemon when vigy runs
//! out-of-process.

use serde_json::json;
use vigy_runtime::RuntimeHandle;

/// One MCP tool entry — catalog shape that any MCP server can convert
/// into its own ToolDefinition.
pub struct ToolEntry {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: serde_json::Value,
}

/// The full catalog of vigy MCP tools.
pub fn tool_catalog() -> Vec<ToolEntry> {
    vec![
        ToolEntry {
            name: "vigy_register",
            description: "Register a new tatara-lisp reconciler (vigy). Idempotent: same name+program yields the same id.",
            input_schema: json!({
                "type": "object",
                "required": ["name", "program"],
                "properties": {
                    "name": { "type": "string" },
                    "program": { "type": "string", "description": "tatara-lisp source" },
                    "tick_interval_ms": { "type": "integer", "minimum": 100, "default": 1000 },
                    "enabled": { "type": "boolean", "default": true },
                    "labels": { "type": "object", "additionalProperties": { "type": "string" } }
                }
            }),
        },
        ToolEntry {
            name: "vigy_list",
            description: "List registered vigies. Optional label selector (k=v,k=v).",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "label_selector": { "type": "string" },
                    "limit": { "type": "integer" }
                }
            }),
        },
        ToolEntry {
            name: "vigy_inspect",
            description: "Inspect a single vigy plus its 5 most-recent runs.",
            input_schema: json!({
                "type": "object",
                "required": ["id"],
                "properties": { "id": { "type": "string" } }
            }),
        },
        ToolEntry {
            name: "vigy_tick",
            description: "Force-tick a vigy now; return the resulting VigyRun (actions emitted, result, error if any).",
            input_schema: json!({
                "type": "object",
                "required": ["id"],
                "properties": { "id": { "type": "string" } }
            }),
        },
        ToolEntry {
            name: "vigy_enable",
            description: "Enable a vigy so the runtime resumes ticking it.",
            input_schema: json!({
                "type": "object",
                "required": ["id"],
                "properties": { "id": { "type": "string" } }
            }),
        },
        ToolEntry {
            name: "vigy_disable",
            description: "Disable a vigy. The reconciler stops ticking but remains registered.",
            input_schema: json!({
                "type": "object",
                "required": ["id"],
                "properties": { "id": { "type": "string" } }
            }),
        },
        ToolEntry {
            name: "vigy_delete",
            description: "Delete a vigy permanently. Recorded VigyRuns persist for audit.",
            input_schema: json!({
                "type": "object",
                "required": ["id"],
                "properties": { "id": { "type": "string" } }
            }),
        },
    ]
}

/// Generic dispatch shim. The host MCP server calls this with the tool
/// name + its parsed args; we return the response payload as JSON.
pub async fn dispatch(
    rt: &RuntimeHandle,
    name: &str,
    args: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    use vigy_types::{TickInterval, Vigy, VigyId};

    match name {
        "vigy_register" => {
            let n = args
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("missing name"))?;
            let p = args
                .get("program")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("missing program"))?;
            let every = args
                .get("tick_interval_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(1000);
            let interval = TickInterval::from_millis(every)?;
            let mut vigy = Vigy::new(n.to_string(), p.to_string(), interval)?;
            if let Some(enabled) = args.get("enabled").and_then(|v| v.as_bool()) {
                vigy.enabled = enabled;
            }
            if let Some(labels) = args.get("labels").and_then(|v| v.as_object()) {
                for (k, val) in labels {
                    if let Some(s) = val.as_str() {
                        vigy.labels.insert(k.clone(), s.to_string())?;
                    }
                }
            }
            let registered = rt.register_or_update(vigy).await?;
            Ok(serde_json::to_value(registered)?)
        }
        "vigy_list" => {
            let sel = args.get("label_selector").and_then(|v| v.as_str());
            let mut all = rt.list(sel).await?;
            if let Some(limit) = args.get("limit").and_then(|v| v.as_u64()) {
                all.truncate(limit as usize);
            }
            Ok(serde_json::to_value(all)?)
        }
        "vigy_inspect" => {
            let id = VigyId::parse(
                args.get("id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing id"))?,
            )?;
            let v = rt.get(&id).await?;
            let runs = rt.recent_runs(&id, 5).await?;
            Ok(json!({ "vigy": v, "recent_runs": runs }))
        }
        "vigy_tick" => {
            let id = VigyId::parse(
                args.get("id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing id"))?,
            )?;
            Ok(serde_json::to_value(rt.tick_now(&id).await?)?)
        }
        "vigy_enable" => {
            let id = VigyId::parse(
                args.get("id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing id"))?,
            )?;
            Ok(serde_json::to_value(rt.enable(&id).await?)?)
        }
        "vigy_disable" => {
            let id = VigyId::parse(
                args.get("id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing id"))?,
            )?;
            Ok(serde_json::to_value(rt.disable(&id).await?)?)
        }
        "vigy_delete" => {
            let id = VigyId::parse(
                args.get("id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("missing id"))?,
            )?;
            Ok(json!({ "deleted": rt.delete(&id).await? }))
        }
        other => Err(anyhow::anyhow!("unknown vigy MCP tool {other:?}")),
    }
}
