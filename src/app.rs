//! Solar position calculator application pipeline.

use crate::error::CliError;
use crate::{Error, Location, cli, data, input, output, predicate, validate};

fn export_options(
    params: &data::Parameters,
    format: output::Format,
    flush_each_record: bool,
) -> output::Options {
    output::Options {
        format,
        headers: params.output.headers,
        show_inputs: params.output.should_show_inputs(),
        elevation_angle: params.output.elevation_angle,
        flush_each_record,
    }
}

fn run_stream(request: validate::StreamRequest) -> Result<(), Error> {
    let validate::StreamRequest {
        command,
        source,
        params,
    } = request;
    if params.verbose {
        let operation = match command {
            data::Command::Position => "positions",
            data::Command::Events => "events",
        };
        eprintln!(
            "Calculating {operation} using {}; output: {}.",
            params.calculation.algorithm, params.output.format
        );
    }
    let watch_mode = source.is_watch_mode(params.step.is_some());
    let flush_each_record = source.uses_stdin() || watch_mode;
    let data_iter = match source {
        data::DataSource::Separate(locations, times) => data::expand_cartesian_product(
            locations,
            times,
            params.step,
            params.timezone.clone(),
            command,
        ),
        data::DataSource::Paired(path) => data::expand_paired_file(path, params.timezone.clone()),
    }
    .map_err(Error::Input)?;
    let start = params.perf.then(std::time::Instant::now);

    let record_count = match command {
        data::Command::Position => {
            let input = data_iter.map(|item| {
                item.map(|(latitude, longitude, time)| input::PositionInput {
                    location: Location {
                        latitude,
                        longitude,
                    },
                    datetime: time,
                })
                .map_err(Error::Input)
            });
            let rows = crate::positions(input, params.position_options())?;
            let layout = output::PositionLayout::new(
                params.output.should_show_inputs(),
                params.output.elevation_angle,
                params.environment.refraction,
            );
            match params.output.format {
                data::OutputFormat::Standard(format) => output::write_position_rows(
                    rows,
                    export_options(&params, format, flush_each_record),
                    layout,
                    std::io::stdout(),
                ),
                #[cfg(feature = "parquet")]
                data::OutputFormat::Parquet => {
                    crate::parquet::write_position_rows(rows, layout, std::io::stdout())
                }
            }
        }
        data::Command::Events => {
            let input = data_iter.map(|item| {
                item.map(|(latitude, longitude, time)| input::EventInput {
                    location: Location {
                        latitude,
                        longitude,
                    },
                    date: time.date(),
                    timezone: time.time_zone().clone(),
                })
                .map_err(Error::Input)
            });
            let rows = crate::events(input, params.calculation)?;
            match params.output.format {
                data::OutputFormat::Standard(format) => output::write_events(
                    rows,
                    export_options(&params, format, flush_each_record),
                    std::io::stdout(),
                ),
                #[cfg(feature = "parquet")]
                data::OutputFormat::Parquet => crate::parquet::write_events(
                    rows,
                    params.output.should_show_inputs(),
                    std::io::stdout(),
                ),
            }
        }
    }?;

    if let Some(start_time) = start {
        let elapsed = start_time.elapsed();
        eprintln!(
            "Processed {} records in {:.3}s ({:.0} records/sec)",
            record_count,
            elapsed.as_secs_f64(),
            record_count as f64 / elapsed.as_secs_f64()
        );
    } else if params.verbose {
        eprintln!("Processed {record_count} records.");
    }
    Ok(())
}

fn execute(valid: validate::ValidCommand) -> Result<i32, CliError> {
    match valid {
        validate::ValidCommand::Predicate(job) if job.params.wait => {
            predicate::wait_until_true(&job)?;
            Ok(0)
        }
        validate::ValidCommand::Predicate(job) => Ok(i32::from(!predicate::run_once(&job)?)),
        validate::ValidCommand::Stream(request) => {
            run_stream(request).map_err(|err| err.to_string())?;
            Ok(0)
        }
    }
}

pub fn run(args: Vec<String>) -> i32 {
    match cli::parse_cli(&args)
        .and_then(validate::validate)
        .and_then(execute)
    {
        Ok(code) => code,
        Err(CliError::Exit(message)) => {
            println!("{message}");
            0
        }
        Err(CliError::Message(message)) => {
            eprintln!("Error: {message}");
            2
        }
    }
}
