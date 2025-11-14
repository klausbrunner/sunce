use std::fmt;

#[derive(Debug)]
pub enum CliError {
    Message(String),
}

impl From<String> for CliError {
    fn from(value: String) -> Self {
        CliError::Message(value)
    }
}

impl From<&str> for CliError {
    fn from(value: &str) -> Self {
        CliError::Message(value.to_string())
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Message(msg) => write!(f, "{}", msg),
        }
    }
}

#[derive(Debug)]
pub enum PlannerError {
    Message(String),
}

impl From<String> for PlannerError {
    fn from(value: String) -> Self {
        PlannerError::Message(value)
    }
}

impl From<&str> for PlannerError {
    fn from(value: &str) -> Self {
        PlannerError::Message(value.to_string())
    }
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlannerError::Message(msg) => write!(f, "{}", msg),
        }
    }
}

#[derive(Debug)]
pub enum OutputError {
    Message(String),
}

impl From<String> for OutputError {
    fn from(value: String) -> Self {
        OutputError::Message(value)
    }
}

impl From<&str> for OutputError {
    fn from(value: &str) -> Self {
        OutputError::Message(value.to_string())
    }
}

impl fmt::Display for OutputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputError::Message(msg) => write!(f, "{}", msg),
        }
    }
}
