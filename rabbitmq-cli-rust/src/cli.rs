use anyhow::Result;
use clap::ArgMatches;
use serde_json::Value;
use std::collections::HashMap;
use tracing::{debug, info, error};

use crate::context::CliContext;
use crate::transport::ErlangTransport;
use crate::error::CliError;

pub struct RabbitMQCli {
    context: CliContext,
    transport: Option<ErlangTransport>,
}

impl RabbitMQCli {
    pub fn new(progname: String, args: Vec<String>) -> Result<Self, CliError> {
        let context = CliContext::new(progname, args);
        
        Ok(Self {
            context,
            transport: None,
        })
    }
    
    pub async fn run(&mut self, matches: ArgMatches) -> Result<(), CliError> {
        // Parse global arguments
        self.parse_global_args(&matches)?;
        
        // Connect to RabbitMQ node
        self.connect().await?;
        
        // Discover available commands
        self.discover_commands().await?;
        
        // Parse and execute the specific command
        self.execute_command(&matches).await?;
        
        Ok(())
    }
    
    fn parse_global_args(&mut self, matches: &ArgMatches) -> Result<(), CliError> {
        // Handle --node argument
        if let Some(node) = matches.get_one::<String>("node") {
            self.context.update_arg_map("node".to_string(), Value::String(node.clone()));
        }
        
        // Handle --verbose argument
        let verbose_count = matches.get_count("verbose") as i64;
        if verbose_count > 0 {
            self.context.update_arg_map("verbose".to_string(), Value::Number(verbose_count.into()));
        }
        
        // Handle subcommand and its arguments
        if let Some((subcommand, sub_matches)) = matches.subcommand() {
            debug!("Processing subcommand: {}", subcommand);
            
            // For our CLI, we expect commands like "list exchanges"
            // These come in as external subcommands
            self.context.set_cmd_path(vec![subcommand.to_string()]);
            
            // Parse command-specific arguments (this will update cmd_path if external)
            self.parse_command_args(sub_matches)?;
        }
        
        Ok(())
    }
    
    fn parse_command_args(&mut self, matches: &ArgMatches) -> Result<(), CliError> {
        // Handle nested subcommands (like "list exchanges")
        if let Some((nested_cmd, nested_matches)) = matches.subcommand() {
            debug!("Processing nested command: {}", nested_cmd);
            
            // Update command path with the nested command
            let mut current_path = self.context.cmd_path.clone();
            current_path.push(nested_cmd.to_string());
            self.context.set_cmd_path(current_path);
            
            // Recursively parse any further nested commands
            self.parse_command_args(nested_matches)?;
        }
        
        // Parse regular arguments
        for id in matches.ids() {
            let id_str = id.as_str();
            
            // Skip internal clap arguments
            if id_str.is_empty() || id_str == "help" || id_str == "version" {
                continue;
            }
            
            debug!("Processing argument: {}", id_str);
            
            // Try to get as string first, then as other types
            if let Some(value) = matches.get_one::<String>(id_str) {
                self.context.update_arg_map(
                    id_str.to_string(),
                    Value::String(value.clone())
                );
            } else if let Some(values) = matches.get_many::<String>(id_str) {
                let collected: Vec<String> = values.cloned().collect();
                self.context.update_arg_map(
                    id_str.to_string(),
                    Value::Array(collected.into_iter().map(Value::String).collect())
                );
            }
        }
        
        Ok(())
    }
    
    async fn connect(&mut self) -> Result<(), CliError> {
        let node_name = self.context.arg_map.get("node")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        
        info!("Establishing connection to RabbitMQ node");
        
        let transport = ErlangTransport::connect(node_name).await?;
        self.transport = Some(transport);
        
        Ok(())
    }
    
    async fn discover_commands(&mut self) -> Result<(), CliError> {
        debug!("Discovering available commands");
        
        let transport = self.transport.as_mut()
            .ok_or_else(|| CliError::Transport("Not connected".to_string()))?;
        
        let commands = transport.discover_commands().await?;
        self.context.set_argparse_def(commands);
        
        Ok(())
    }
    
    async fn execute_command(&mut self, _matches: &ArgMatches) -> Result<(), CliError> {
        info!("Executing command: {:?}", self.context.cmd_path);
        
        let transport = self.transport.as_mut()
            .ok_or_else(|| CliError::Transport("Not connected".to_string()))?;
        
        // Execute the command on the remote node
        let result = transport.run_command(&self.context).await?;
        
        // Process and display the result
        self.handle_command_result(result).await?;
        
        Ok(())
    }
    
    async fn handle_command_result(&self, result: Value) -> Result<(), CliError> {
        debug!("Processing command result");
        
        match result {
            Value::Object(map) => {
                if let Some(output) = map.get("output") {
                    if let Some(output_str) = output.as_str() {
                        print!("{}", output_str);
                    } else {
                        println!("{}", serde_json::to_string_pretty(output)?);
                    }
                } else {
                    println!("{}", serde_json::to_string_pretty(&map)?);
                }
            }
            Value::String(s) => {
                print!("{}", s);
            }
            _ => {
                println!("{}", serde_json::to_string_pretty(&result)?);
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_cli_creation() {
        let cli = RabbitMQCli::new(
            "rabbitmq".to_string(),
            vec!["rabbitmq".to_string(), "list".to_string(), "exchanges".to_string()]
        );
        
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert_eq!(cli.context.progname, "rabbitmq");
        assert_eq!(cli.context.args.len(), 3);
    }
}