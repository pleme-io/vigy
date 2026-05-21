//! vigy CLI.
//!
//! Subcommands:
//!
//!   vigy register <file.tatara> --name <n> [--every <ms>] [--label k=v]
//!       Read a .tatara file, register it, return the assigned VigyId.
//!
//!   vigy list [--selector k=v]
//!       Tabular dump of registered vigies.
//!
//!   vigy inspect <id>
//!       Full record + recent runs.
//!
//!   vigy tick <id>
//!       Force-tick once; print the resulting VigyRun.
//!
//!   vigy enable <id> | vigy disable <id> | vigy delete <id>
//!       Lifecycle.
//!
//!   vigy tail [--id <id>]
//!       Stream reconcile events to stdout (newest-first; JSON one-per-line).
//!
//!   vigy serve [--bind 127.0.0.1:38821] [--db <path>]
//!       Start the always-on runtime + bind the gRPC/GraphQL/REST surfaces.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use colored::Colorize;
use std::path::PathBuf;
use std::time::Duration;
use vigy_runtime::RuntimeHandle;
use vigy_types::{TickInterval, Vigy, VigyId};

#[derive(Debug, Parser)]
#[command(name = "vigy", version, about = "always-on tatara-lisp reconciler runtime")]
struct Cli {
    /// SQLite DB path. Defaults to ~/.local/share/vigy/vigy.db (or $VIGY_DB).
    #[arg(long, global = true)]
    db: Option<PathBuf>,

    /// Logging verbosity (-v info, -vv debug, -vvv trace).
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    #[command(subcommand)]
    command: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    Register {
        file: PathBuf,
        #[arg(long)]
        name: String,
        /// Tick interval in milliseconds (>= 100). Default 1000.
        #[arg(long, default_value_t = 1000)]
        every: u64,
        /// k=v label, repeatable.
        #[arg(long = "label")]
        labels: Vec<String>,
        /// Register but leave disabled (no ticking).
        #[arg(long)]
        disabled: bool,
    },
    List {
        #[arg(long)]
        selector: Option<String>,
    },
    Inspect {
        id: String,
    },
    Tick {
        id: String,
    },
    Enable {
        id: String,
    },
    Disable {
        id: String,
    },
    Delete {
        id: String,
    },
    Tail {
        #[arg(long)]
        id: Option<String>,
    },
    Serve {
        /// (gRPC + REST + GraphQL bind) — placeholder until API crates land.
        #[arg(long, default_value = "127.0.0.1:38821")]
        bind: String,
    },
}

fn db_path(override_: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(p) = override_ {
        return Ok(p);
    }
    if let Ok(p) = std::env::var("VIGY_DB") {
        return Ok(PathBuf::from(p));
    }
    let home = std::env::var("HOME").context("HOME unset")?;
    let mut p = PathBuf::from(home);
    p.push(".local");
    p.push("share");
    p.push("vigy");
    p.push("vigy.db");
    Ok(p)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);
    let db = db_path(cli.db.clone())?;
    tracing::debug!(?db, "vigy db");

    let rt = RuntimeHandle::open(&db).await?;

    match cli.command {
        Cmd::Register {
            file,
            name,
            every,
            labels,
            disabled,
        } => {
            let program = std::fs::read_to_string(&file)
                .with_context(|| format!("read {}", file.display()))?;
            let interval = TickInterval::from_millis(every)?;
            let mut vigy = Vigy::new(name, program, interval)?;
            for kv in labels {
                let (k, v) = kv
                    .split_once('=')
                    .with_context(|| format!("label must be k=v, got {kv:?}"))?;
                vigy.labels.insert(k, v)?;
            }
            if disabled {
                vigy.enabled = false;
            }
            let registered = rt.register_or_update(vigy).await?;
            println!(
                "{} {}  {}",
                "✓".green().bold(),
                registered.id.to_string().bold(),
                registered.name.dimmed()
            );
            if registered.enabled {
                println!(
                    "  ticking every {} ms",
                    registered.tick_interval.as_millis()
                );
            } else {
                println!("  {}", "registered disabled".yellow());
            }
        }
        Cmd::List { selector } => {
            let all = rt.list(selector.as_deref()).await?;
            if all.is_empty() {
                println!("{}", "no vigies registered".dimmed());
            }
            for v in all {
                let marker = if v.enabled { "■".green() } else { "□".dimmed() };
                println!(
                    "{marker} {id}  {name}  ({every} ms)",
                    id = v.id.to_string().bold(),
                    name = v.name,
                    every = v.tick_interval.as_millis(),
                );
                if !v.labels.iter().next().is_none() {
                    let labels: Vec<_> = v
                        .labels
                        .iter()
                        .map(|(k, val)| format!("{k}={val}"))
                        .collect();
                    println!("    {}", labels.join(", ").dimmed());
                }
            }
        }
        Cmd::Inspect { id } => {
            let id = VigyId::parse(id)?;
            let v = rt.get(&id).await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
            let runs = rt.recent_runs(&id, 5).await?;
            if !runs.is_empty() {
                println!("\nrecent runs:");
                for r in runs {
                    println!("  {} {:?} ({} actions)", r.id, r.result, r.actions.len());
                }
            }
        }
        Cmd::Tick { id } => {
            let id = VigyId::parse(id)?;
            let run = rt.tick_now(&id).await?;
            println!("{}", serde_json::to_string_pretty(&run)?);
        }
        Cmd::Enable { id } => {
            let id = VigyId::parse(id)?;
            let v = rt.enable(&id).await?;
            println!("{} {} enabled", "✓".green(), v.id);
        }
        Cmd::Disable { id } => {
            let id = VigyId::parse(id)?;
            let v = rt.disable(&id).await?;
            println!("{} {} disabled", "✓".green(), v.id);
        }
        Cmd::Delete { id } => {
            let id = VigyId::parse(id)?;
            let deleted = rt.delete(&id).await?;
            if deleted {
                println!("{} {} deleted", "✓".green(), id);
            } else {
                println!("{} {} not found", "·".dimmed(), id);
            }
        }
        Cmd::Tail { id } => {
            let filter_id = id.map(VigyId::parse).transpose()?;
            let mut rx = rt.subscribe();
            println!("{}", "tailing reconcile events (Ctrl-C to stop)…".dimmed());
            loop {
                match rx.recv().await {
                    Ok(run) => {
                        if filter_id
                            .as_ref()
                            .is_some_and(|f| f != &run.vigy_id)
                        {
                            continue;
                        }
                        println!("{}", serde_json::to_string(&run)?);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!(
                            "{} dropped {} events (slow consumer)",
                            "warn:".yellow(),
                            n
                        );
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
        Cmd::Serve { bind } => {
            println!(
                "{} runtime live (db={}). API servers (gRPC+GraphQL+REST) bind at {bind} — not yet implemented.",
                "▶".green().bold(),
                db.display()
            );
            println!("{}", "ticking registered vigies in the background…".dimmed());
            // Idle main; tick tasks are running.
            tokio::signal::ctrl_c().await?;
            println!("\n{}", "shutting down".dimmed());
        }
    }

    Ok(())
}

fn init_tracing(verbosity: u8) {
    use tracing_subscriber::EnvFilter;
    let level = match verbosity {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("vigy={level}")));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}

#[allow(dead_code)]
const fn _dur(_: Duration) {}
