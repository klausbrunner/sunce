use std::fmt;

#[derive(Debug)]
pub enum CliError {
    Validation(String),
    Io(String),
}

impl From<String> for CliError {
    fn from(value: String) -> Self {
        CliError::Validation(value)
    }
}

impl From<&str> for CliError {
    fn from(value: &str) -> Self {
        CliError::Validation(value.to_string())
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Validation(msg) | CliError::Io(msg) => write!(f, "{}", msg),
        }
    }
}

impl From<std::io::Error> for CliError {
    fn from(value: std::io::Error) -> Self {
        CliError::Io(value.to_string())
    }
}

#[derive(Debug)]
pub enum PlannerError {
    Validation(String),
    Io(String),
}

impl From<String> for PlannerError {
    fn from(value: String) -> Self {
        PlannerError::Validation(value)
    }
}

impl From<&str> for PlannerError {
    fn from(value: &str) -> Self {
        PlannerError::Validation(value.to_string())
    }
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlannerError::Validation(msg) | PlannerError::Io(msg) => write!(f, "{}", msg),
        }
    }
}

impl From<std::io::Error> for PlannerError {
    fn from(value: std::io::Error) -> Self {
        PlannerError::Io(value.to_string())
    }
}

#[derive(Debug)]
pub enum OutputError {
    Io(String),
    Format(String),
}

impl From<String> for OutputError {
    fn from(value: String) -> Self {
        OutputError::Format(value)
    }
}

impl From<&str> for OutputError {
    fn from(value: &str) -> Self {
        OutputError::Format(value.to_string())
    }
}

impl fmt::Display for OutputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputError::Io(msg) | OutputError::Format(msg) => write!(f, "{}", msg),
        }
    }
}

impl From<std::io::Error> for OutputError {
    fn from(value: std::io::Error) -> Self {
        OutputError::Io(value.to_string())
    }
}
