//! Parsed CLI values before semantic validation.

use crate::data::{Command, InputPath, LocationSource, Parameters, Predicate};

#[derive(Debug)]
pub enum ParsedTimeSource {
    Value(String),
    File(InputPath),
    Now,
}

#[derive(Debug)]
pub enum ParsedInput {
    Separate(LocationSource, ParsedTimeSource),
    Paired(InputPath),
}

#[derive(Debug, Default)]
pub struct ParsedOptionUsage {
    // These options can be given explicitly without changing their default values.
    pub format: bool,
    pub headers: bool,
    pub elevation: bool,
    pub temperature: bool,
    pub pressure: bool,
}

#[derive(Debug)]
pub struct ParsedCommand {
    pub command: Command,
    pub input: ParsedInput,
    pub params: Parameters,
    pub predicate: Option<Predicate>,
    pub usage: ParsedOptionUsage,
}
