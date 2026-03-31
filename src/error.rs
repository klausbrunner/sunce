//! Error types for CLI parsing, planning, and output handling.

use std::fmt;
use std::io;

#[derive(Debug)]
pub enum CliError {
    /// Print message to stdout and exit with code 0 (help/version/usage).
    Exit(String),
    /// Print message to stderr and exit with code 1.
    Message(String),
    /// Print message to stderr and exit with a specific code.
    MessageWithCode(String, i32),
}

pub fn predicate_error(message: impl Into<String>) -> CliError {
    CliError::MessageWithCode(message.into(), 2)
}

impl From<String> for CliError {
    fn from(value: String) -> Self {
        Self::Message(value)
    }
}

impl From<&str> for CliError {
    fn from(value: &str) -> Self {
        Self::Message(value.to_string())
    }
}

impl From<std::io::Error> for CliError {
    fn from(value: std::io::Error) -> Self {
        Self::Message(value.to_string())
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Exit(msg) | CliError::Message(msg) | CliError::MessageWithCode(msg, _) => {
                f.write_str(msg)
            }
        }
    }
}

#[derive(Debug)]
pub struct StringError(pub String);

impl From<String> for StringError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for StringError {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<io::Error> for StringError {
    fn from(value: io::Error) -> Self {
        Self(value.to_string())
    }
}

impl fmt::Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub type PlannerError = StringError;
pub type OutputError = StringError;
