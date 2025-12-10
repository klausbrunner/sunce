use std::fmt;

macro_rules! simple_error {
    ($name:ident) => {
        #[derive(Debug)]
        pub struct $name(pub String);

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_string())
            }
        }

        impl From<std::io::Error> for $name {
            fn from(value: std::io::Error) -> Self {
                Self(value.to_string())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

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
            CliError::Exit(msg) | CliError::Message(msg) => write!(f, "{}", msg),
        }
    }
}

simple_error!(PlannerError);
simple_error!(OutputError);
