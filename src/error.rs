//! Error types for CLI parsing, validation, and output handling.

use std::fmt;
use std::io;

#[cfg(feature = "cli")]
#[derive(Debug)]
pub enum CliError {
    /// Print help or version information and exit successfully.
    Exit(String),
    /// Print a diagnostic to stderr and exit with code 2.
    Message(String),
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
