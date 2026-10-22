//! Error types for CLI parsing, validation, and output handling.

use std::fmt;
use std::io;

#[cfg(feature = "cli")]
#[derive(Debug)]
pub enum CliError {
    /// Print message to stdout and exit with code 0 (help/version/usage).
    Exit(String),
    /// Print message to stderr and exit with code 1.
    Message(String),
    /// Print message to stderr and exit with a specific code.
    MessageWithCode(String, i32),
}

#[cfg(feature = "cli")]
pub fn predicate_error(message: impl Into<String>) -> CliError {
    CliError::MessageWithCode(message.into(), 2)
}

#[cfg(feature = "cli")]
impl From<String> for CliError {
    fn from(value: String) -> Self {
        Self::Message(value)
    }
}

#[cfg(feature = "cli")]
impl From<&str> for CliError {
    fn from(value: &str) -> Self {
        Self::Message(value.to_string())
    }
}

#[cfg(feature = "cli")]
impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Exit(msg) | CliError::Message(msg) | CliError::MessageWithCode(msg, _) => {
                f.write_str(msg)
            }
        }
    }
}

/// An error at a library pipeline boundary. CLI exit codes are handled separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Invalid configuration, a malformed record, or a reader failure.
    Input(String),
    /// A solar calculation could not be completed.
    Calculation(String),
    /// Serialization or writing failed.
    Output(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Input(message) | Self::Calculation(message) | Self::Output(message) => {
                f.write_str(message)
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Output(error.to_string())
    }
}
