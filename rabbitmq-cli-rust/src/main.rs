use anyhow::Result;
use clap::{Arg, ArgMatches, Command};
use std::env;
use tracing::{debug, info};

mod transport;
mod context;
mod cli;
mod terminal;
mod error;

use crate::cli::RabbitMQCli;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Get program name and arguments
    let args: Vec<String> = env::args().collect();
    let prog_name = args[0]
        .split('/')
        .last()
        .unwrap_or("rabbitmq")
        .strip_suffix(".exe")
        .unwrap_or_else(|| args[0].as_str());

    debug!("Starting RabbitMQ CLI: {}", prog_name);

    // Check if this should use legacy CLI
    if is_legacy_progname(prog_name) {
        eprintln!("Legacy CLI programs not supported in Rust implementation");
        eprintln!("Use the Erlang-based tools for: {}", prog_name);
        std::process::exit(1);
    }

    // Build initial argument parser for global options
    let app = build_initial_app();
    let matches = app.try_get_matches_from(&args)?;

    // Create and run CLI
    let mut cli = RabbitMQCli::new(prog_name.to_string(), args)
        .map_err(|e| anyhow::anyhow!("CLI creation failed: {}", e))?;
    cli.run(matches).await
        .map_err(|e| anyhow::anyhow!("CLI execution failed: {}", e))?;
    
    Ok(())
}

fn is_legacy_progname(progname: &str) -> bool {
    matches!(progname, 
        "rabbitmqctl" | 
        "rabbitmq-diagnostics" | 
        "rabbitmq-plugins" | 
        "rabbitmq-queues" | 
        "rabbitmq-streams" | 
        "rabbitmq-upgrade"
    )
}

fn build_initial_app() -> Command {
    Command::new("rabbitmq")
        .version(env!("CARGO_PKG_VERSION"))
        .about("RabbitMQ CLI - Rust implementation")
        // help is automatically added by clap
        .arg(
            Arg::new("node")
                .short('n')
                .long("node")
                .value_name("NODE")
                .help("Name of the node to control")
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Be verbose; can be specified multiple times to increase verbosity")
                .action(clap::ArgAction::Count)
        )
        // version is automatically added by clap
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List RabbitMQ resources")
                .subcommand_required(true)
                .subcommand(Command::new("exchanges").about("List exchanges"))
                .subcommand(Command::new("queues").about("List queues"))
                .subcommand(Command::new("bindings").about("List bindings"))
        )
}