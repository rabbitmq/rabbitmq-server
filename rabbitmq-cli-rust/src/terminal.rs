use serde::{Deserialize, Serialize};
use crossterm::tty::IsTty;
use std::io;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalInfo {
    pub stdout: bool,
    pub stderr: bool,
    pub stdin: bool,
    pub name: String,
    pub info: Option<TermInfoData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermInfoData {
    // Placeholder for terminfo data - we'll expand this as needed
    pub colors: Option<i32>,
    pub cols: Option<i32>,
    pub lines: Option<i32>,
}

impl TerminalInfo {
    pub fn collect() -> Self {
        let stdout = io::stdout().is_tty();
        let stderr = io::stderr().is_tty();
        let stdin = io::stdin().is_tty();
        
        let term_name = std::env::var("TERM").unwrap_or_else(|_| "unknown".to_string());
        
        // Try to get terminal capabilities
        let term_info = if stdout {
            TermInfoData::detect()
        } else {
            None
        };

        Self {
            stdout,
            stderr,
            stdin,
            name: term_name,
            info: term_info,
        }
    }

    pub fn supports_colors(&self) -> bool {
        if let Some(ref info) = self.info {
            if let Some(colors) = info.colors {
                return colors > 0;
            }
        }
        
        // Fallback: check common environment variables
        std::env::var("COLORTERM").is_ok() || 
        std::env::var("TERM").map(|t| t.contains("color")).unwrap_or(false)
    }
}

impl TermInfoData {
    fn detect() -> Option<Self> {
        // Try to get terminal size
        let (cols, lines) = crossterm::terminal::size().ok()
            .map(|(w, h)| (Some(w as i32), Some(h as i32)))
            .unwrap_or((None, None));

        // Detect color support
        let colors = detect_color_support();

        Some(Self {
            colors,
            cols,
            lines,
        })
    }
}

fn detect_color_support() -> Option<i32> {
    // Check COLORTERM for truecolor support
    if let Ok(colorterm) = std::env::var("COLORTERM") {
        if colorterm == "truecolor" || colorterm == "24bit" {
            return Some(16777216); // 24-bit color
        }
    }

    // Check TERM variable for color capabilities
    if let Ok(term) = std::env::var("TERM") {
        if term.contains("256color") {
            return Some(256);
        } else if term.contains("color") {
            return Some(8);
        }
    }

    // If we can't determine, assume basic color support for TTY
    if io::stdout().is_tty() {
        Some(8)
    } else {
        Some(0)
    }
}