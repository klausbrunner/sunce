//! Parsed CLI shapes before semantic validation.

use crate::data::{Command, InputPath, LocationSource, Parameters, Predicate};

#[derive(Debug, Clone)]
pub enum ParsedTimeSource {
    Value(String),
    File(InputPath),
    Now,
}

#[derive(Debug, Clone)]
pub enum ParsedInput {
    Separate(LocationSource, ParsedTimeSource),
    Paired(InputPath),
}

#[derive(Debug, Clone, Default)]
pub struct ParsedOptionUsage {
    pub format: bool,
    pub headers: bool,
    pub show_inputs: bool,
    pub perf: bool,
    pub step: bool,
    pub no_refraction: bool,
    pub elevation_angle: bool,
    pub elevation: bool,
    pub temperature: bool,
    pub pressure: bool,
    pub algorithm: bool,
    pub horizon: bool,
    pub twilight: bool,
}

#[derive(Debug, Clone)]
pub struct ParsedCommand {
    pub command: Command,
    pub input: ParsedInput,
    pub params: Parameters,
    pub predicate: Option<Predicate>,
    pub usage: ParsedOptionUsage,
}
