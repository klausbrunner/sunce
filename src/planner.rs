//! Execution planning: convert parsed CLI data into a job specification used by main.

use crate::data::{self, Command, CoordTimeStream, DataSource, Parameters};
use crate::error::PlannerError;

pub struct ComputePlan {
    pub data_iter: CoordTimeStream,
    pub command: Command,
    pub params: Parameters,
    pub allow_time_cache: bool,
    pub flush_each_record: bool,
}

pub fn build_job(
    source: DataSource,
    command: Command,
    params: Parameters,
) -> Result<ComputePlan, PlannerError> {
    let data_iter = match &source {
        DataSource::Separate(loc_source, time_source) => data::expand_cartesian_product(
            loc_source.clone(),
            time_source.clone(),
            params.step,
            params.timezone.clone(),
            command,
        )
        .map_err(PlannerError::from),
        DataSource::Paired(path) => data::expand_paired_file(path.clone(), params.timezone.clone())
            .map_err(PlannerError::from),
    }?;

    Ok(ComputePlan {
        data_iter,
        command,
        allow_time_cache: !source.is_watch_mode(&params.step),
        flush_each_record: source.uses_stdin() || source.is_watch_mode(&params.step),
        params,
    })
}
