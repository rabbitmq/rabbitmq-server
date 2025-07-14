use thiserror::Error;

#[derive(Error, Debug)]
pub enum CliError {
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Transport error: {0}")]
    Transport(String),
    
    #[error("Command error: {0}")]
    Command(String),
    
    #[error("Argument parsing error: {0}")]
    ArgumentParsing(String),
    
    #[error("Backend error: {0}")]
    Backend(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("Node not found: {0}")]
    NodeNotFound(String),
    
    #[error("Authentication failed")]
    AuthenticationFailed,
    
    #[error("Timeout")]
    Timeout,
}

pub type Result<T> = std::result::Result<T, CliError>;