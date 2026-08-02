//! Execution planning: convert parsed CLI data into a job specification used by main.

use crate::data::{self, Command, CoordTimeStream, DataSource, Parameters};
use crate::predicate::PredicateJob;
use crate::validate::{StreamRequest, ValidCommand};

pub struct ComputePlan {
    pub data_iter: CoordTimeStream,
    pub command: Command,
    pub params: Parameters,
    pub allow_time_cache: bool,
    pub flush_each_record: bool,
}

pub enum RunPlan {
    Stream(ComputePlan),
    Predicate(PredicateJob),
}

pub fn build_plan(valid: ValidCommand) -> Result<RunPlan, String> {
    match valid {
        ValidCommand::Predicate(job) => Ok(RunPlan::Predicate(job)),
        ValidCommand::Stream(request) => build_stream_plan(request),
    }
}

fn build_stream_plan(request: StreamRequest) -> Result<RunPlan, String> {
    let StreamRequest {
        command,
        source,
        params,
    } = request;
    let allow_time_cache = !source.is_watch_mode(&params.step);
    let flush_each_record = source.uses_stdin() || source.is_watch_mode(&params.step);
    let data_iter = match source {
        DataSource::Separate(loc_source, time_source) => data::expand_cartesian_product(
            loc_source,
            time_source,
            params.step,
            params.timezone.clone(),
            command,
        ),
        DataSource::Paired(path) => data::expand_paired_file(path, params.timezone.clone()),
    }?;

    Ok(RunPlan::Stream(ComputePlan {
        data_iter,
        command,
        allow_time_cache,
        flush_each_record,
        params,
    }))
}
