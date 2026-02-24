use std::fmt;

#[derive(Debug)]
pub enum CliError {
    /// Print message to stdout and exit with code 0 (help/version/usage).
    Exit(String),
    /// Print message to stderr and exit with code 1.
    Message(String),
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
            CliError::Exit(msg) | CliError::Message(msg) => f.write_str(msg),
        }
    }
}

#[derive(Debug)]
pub struct PlannerError(pub String);

impl From<String> for PlannerError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for PlannerError {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<std::io::Error> for PlannerError {
    fn from(value: std::io::Error) -> Self {
        Self(value.to_string())
    }
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug)]
pub struct OutputError(pub String);

impl From<String> for OutputError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for OutputError {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<std::io::Error> for OutputError {
    fn from(value: std::io::Error) -> Self {
        Self(value.to_string())
    }
}

impl fmt::Display for OutputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
