use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::env;

use crate::terminal::TerminalInfo;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliContext {
    pub progname: String,
    pub args: Vec<String>,
    pub argparse_def: Option<Value>,
    pub arg_map: HashMap<String, Value>,
    pub cmd_path: Vec<String>,
    pub command: Option<Value>,
    pub os: (String, String),
    pub client: ClientInfo,
    pub env: Vec<(String, String)>,
    pub terminal: TerminalInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfo {
    pub hostname: String,
    pub proto: String,
}

impl CliContext {
    pub fn new(progname: String, args: Vec<String>) -> Self {
        let hostname = hostname::get()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        let os_info = (
            env::consts::OS.to_string(),
            env::consts::ARCH.to_string(),
        );

        let env_vars: Vec<(String, String)> = env::vars().collect();

        Self {
            progname,
            args,
            argparse_def: None,
            arg_map: HashMap::new(),
            cmd_path: Vec::new(),
            command: None,
            os: os_info,
            client: ClientInfo {
                hostname,
                proto: "erldist".to_string(),
            },
            env: env_vars,
            terminal: TerminalInfo::collect(),
        }
    }

    pub fn update_arg_map(&mut self, key: String, value: Value) {
        self.arg_map.insert(key, value);
    }

    pub fn set_cmd_path(&mut self, path: Vec<String>) {
        self.cmd_path = path;
    }

    pub fn set_command(&mut self, command: Value) {
        self.command = Some(command);
    }

    pub fn set_argparse_def(&mut self, def: Value) {
        self.argparse_def = Some(def);
    }

    /// Convert to the map format expected by the Erlang backend
    pub fn to_erlang_map(&self) -> HashMap<String, Value> {
        let mut map = HashMap::new();
        
        map.insert("progname".to_string(), Value::String(self.progname.clone()));
        map.insert("args".to_string(), 
            Value::Array(self.args.iter().map(|s| Value::String(s.clone())).collect()));
        
        if let Some(ref def) = self.argparse_def {
            map.insert("argparse_def".to_string(), def.clone());
        }
        
        map.insert("arg_map".to_string(), serde_json::to_value(&self.arg_map).unwrap());
        map.insert("cmd_path".to_string(), 
            Value::Array(self.cmd_path.iter().map(|s| Value::String(s.clone())).collect()));
        
        if let Some(ref cmd) = self.command {
            map.insert("command".to_string(), cmd.clone());
        }
        
        map.insert("os".to_string(), 
            Value::Array(vec![
                Value::String(self.os.0.clone()),
                Value::String(self.os.1.clone())
            ]));
        
        map.insert("client".to_string(), serde_json::to_value(&self.client).unwrap());
        
        let env_array: Vec<Value> = self.env.iter()
            .map(|(k, v)| Value::Array(vec![Value::String(k.clone()), Value::String(v.clone())]))
            .collect();
        map.insert("env".to_string(), Value::Array(env_array));
        
        map.insert("terminal".to_string(), serde_json::to_value(&self.terminal).unwrap());
        
        map
    }
}